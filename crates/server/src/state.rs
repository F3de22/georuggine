//! Macchina a stati fermo/movimento dei veicoli.
//!
//! [`StateTracker`] ricorda per ogni utente la posizione precedente e l'istante
//! dell'ultimo spostamento, e da quei due dati ricava le due transizioni della traccia:
//! oltre [`MOVEMENT_THRESHOLD_METERS`] dalla posizione precedente l'utente e' in
//! movimento, e dopo [`STILL_AFTER_SECS`] secondi senza spostarsi torna fermo.
//!
//! Le due transizioni seguono percorsi diversi. Il passaggio a [`UserState::Moving`]
//! avviene su un evento, la posizione, e vive quindi in [`StateTracker::on_position`]. Il
//! ritorno a [`UserState::Still`] non ha nessun evento che lo provochi — succede perche'
//! il tempo passa mentre non arriva nulla — e sta percio' in [`StateTracker::tick`], che
//! il livello di rete chiama a intervalli regolari da un task dedicato.
//!
//! La mappa sta sotto il [`Mutex`] della libreria standard e non sotto quello di tokio
//! perche' i metodi sono sincroni: nessuna sezione critica contiene un punto di
//! sospensione, e ognuna e' un accesso a una `HashMap` in memoria.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use chrono::{DateTime, Utc};
use georuggine_common::clock::Clock;
use georuggine_common::types::{
    Coordinate, MOVEMENT_THRESHOLD_METERS, STILL_AFTER_SECS, UserId, UserState, haversine_meters,
};

/// [`STILL_AFTER_SECS`] nel tipo con cui si confrontano le durate di chrono.
///
/// Saturare a [`i64::MAX`] e' l'esito prudente se qualcuno alzasse la soglia oltre quel
/// valore: nessun utente verrebbe piu' riportato a fermo, invece di esserlo per un
/// valore troncato in silenzio da `as`.
fn soglia_fermo_secs() -> i64 {
    i64::try_from(STILL_AFTER_SECS).unwrap_or(i64::MAX)
}

/// Cio' che si ricorda di un utente per decidere se e' fermo o in movimento.
#[derive(Debug, Clone, PartialEq)]
struct UserTracking {
    /// Stato comunicato ai client.
    state: UserState,
    /// Ultima posizione ricevuta, con cui si confronta la prossima.
    ///
    /// E' `None` subito dopo la connessione: senza un punto precedente non si puo'
    /// misurare nessuno spostamento, e la prima posizione della sessione non porta mai
    /// in movimento.
    last_coordinate: Option<Coordinate>,
    /// Istante dell'ultimo spostamento oltre la soglia, da cui si contano i tre minuti.
    last_movement_at: DateTime<Utc>,
}

impl UserTracking {
    /// Voce di un utente appena connesso: fermo, senza posizione precedente.
    fn nuovo(adesso: DateTime<Utc>) -> Self {
        Self {
            state: UserState::Still,
            last_coordinate: None,
            last_movement_at: adesso,
        }
    }
}

/// Macchina a stati fermo/movimento di tutti gli utenti, condivisa fra le connessioni
/// come `Arc<StateTracker>`.
pub struct StateTracker {
    /// Cio' che si ricorda di ogni utente.
    ///
    /// Le voci non vengono mai rimosse: alla disconnessione lo stato diventa
    /// [`UserState::Disconnected`] e la voce resta, cosi' una riconnessione la reimposta
    /// senza doverla ricreare.
    utenti: Mutex<HashMap<UserId, UserTracking>>,
    /// Sorgente dell'ora: mai `Utc::now()`, cosi' i test sono deterministici.
    orologio: Arc<dyn Clock>,
}

impl StateTracker {
    /// Costruisce un tracciatore senza utenti, che legge l'ora dall'orologio indicato.
    pub fn new(orologio: Arc<dyn Clock>) -> Self {
        Self {
            utenti: Mutex::new(HashMap::new()),
            orologio,
        }
    }

    /// Stato corrente di un utente, `None` se non e' mai stato visto.
    pub fn current_state(&self, user_id: UserId) -> Option<UserState> {
        self.blocca()
            .get(&user_id)
            .map(|tracciamento| tracciamento.state)
    }

    /// Confronta la posizione con la precedente e restituisce il nuovo stato se cambia.
    ///
    /// Restituisce `Some(stato)` solo quando c'e' qualcosa di nuovo da comunicare al
    /// client: `None` sia sotto soglia sia quando l'utente era gia' in movimento. La
    /// transizione inversa non avviene qui, perche' il requisito parla di tre minuti
    /// senza movimento e non della singola rilevazione: se ne occupa [`Self::tick`].
    ///
    /// Un utente senza voce nella mappa non e' un errore: la voce viene creata come per
    /// una connessione appena aperta.
    pub fn on_position(&self, user_id: UserId, coordinate: Coordinate) -> Option<UserState> {
        let adesso = self.orologio.now();

        let mut utenti = self.blocca();
        let tracciamento = utenti
            .entry(user_id)
            .or_insert_with(|| UserTracking::nuovo(adesso));

        let precedente = tracciamento.state;
        let si_e_spostato = tracciamento
            .last_coordinate
            .is_some_and(|ultima| haversine_meters(ultima, coordinate) > MOVEMENT_THRESHOLD_METERS);

        tracciamento.last_coordinate = Some(coordinate);
        if si_e_spostato {
            tracciamento.last_movement_at = adesso;
            tracciamento.state = UserState::Moving;
        }

        (tracciamento.state != precedente).then_some(tracciamento.state)
    }

    /// Apre (o riapre) il tracciamento di un utente, che riparte fermo.
    ///
    /// La voce viene sostituita e non aggiornata: dopo una riconnessione la coordinata
    /// precedente non e' piu' un termine di paragone valido, perche' nel frattempo il
    /// veicolo puo' essersi spostato ovunque senza che il server lo sappia.
    pub fn on_connect(&self, user_id: UserId) {
        let adesso = self.orologio.now();

        self.blocca().insert(user_id, UserTracking::nuovo(adesso));
    }

    /// Segna l'utente come [`UserState::Disconnected`], se e' noto.
    pub fn on_disconnect(&self, user_id: UserId) {
        if let Some(tracciamento) = self.blocca().get_mut(&user_id) {
            tracciamento.state = UserState::Disconnected;
        }
    }

    /// Riporta a [`UserState::Still`] chi non si sposta da [`STILL_AFTER_SECS`] secondi.
    ///
    /// Restituisce le coppie `(utente, stato)` passate a fermo in questa chiamata, che il
    /// chiamante inoltra ai client interessati. Gli utenti gia' fermi e quelli
    /// disconnessi non vengono toccati, quindi due chiamate consecutive senza posizioni
    /// in mezzo segnalano lo stesso utente una volta sola.
    pub fn tick(&self) -> Vec<(UserId, UserState)> {
        let adesso = self.orologio.now();
        let soglia = soglia_fermo_secs();
        let mut passati_a_fermo = Vec::new();

        let mut utenti = self.blocca();
        for (user_id, tracciamento) in utenti.iter_mut() {
            if tracciamento.state != UserState::Moving {
                continue;
            }
            // Se l'orologio tornasse indietro la differenza sarebbe negativa e l'utente
            // resterebbe in movimento, che e' l'esito prudente.
            let fermo_da = (adesso - tracciamento.last_movement_at).num_seconds();
            if fermo_da >= soglia {
                tracciamento.state = UserState::Still;
                passati_a_fermo.push((*user_id, UserState::Still));
            }
        }

        passati_a_fermo
    }

    /// Prende il lock riprendendolo anche se e' avvelenato: ogni sezione critica e' un
    /// aggiornamento che non lascia lo stato a meta', e la posizione successiva rimette
    /// in pari la voce di un utente.
    fn blocca(&self) -> MutexGuard<'_, HashMap<UserId, UserTracking>> {
        self.utenti.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use chrono::TimeDelta;
    use georuggine_common::clock::FakeClock;

    fn istante_base() -> DateTime<Utc> {
        DateTime::from_timestamp(1_704_067_200, 0).expect("istante valido")
    }

    fn coord(lat: f64, lon: f64) -> Coordinate {
        Coordinate::new(lat, lon).expect("coordinata di test valida")
    }

    fn partenza() -> Coordinate {
        coord(45.0703, 7.6869)
    }

    /// Circa 1 m a nord di [`partenza`]: sotto la soglia di movimento.
    fn a_un_metro() -> Coordinate {
        coord(45.0703 + 0.000_009, 7.6869)
    }

    /// Circa 50 m a nord di [`partenza`]: dieci volte la soglia, cosi' il test non
    /// dipende dalla precisione della formula.
    fn a_cinquanta_metri() -> Coordinate {
        coord(45.0703 + 0.000_45, 7.6869)
    }

    /// Circa 100 m a nord di [`partenza`], distante anche da [`a_cinquanta_metri`].
    fn a_cento_metri() -> Coordinate {
        coord(45.0703 + 0.000_9, 7.6869)
    }

    /// Tracciatore con un orologio finto, restituito perche' il test lo possa spostare.
    fn tracciatore() -> (Arc<FakeClock>, StateTracker) {
        let orologio = Arc::new(FakeClock::new(istante_base()));
        let sorgente: Arc<dyn Clock> = orologio.clone();
        (orologio, StateTracker::new(sorgente))
    }

    /// Porta l'utente in [`UserState::Moving`] partendo da una connessione nuova.
    fn porta_in_movimento(tracker: &StateTracker, user_id: UserId) {
        tracker.on_connect(user_id);
        assert_eq!(tracker.on_position(user_id, partenza()), None);
        assert_eq!(
            tracker.on_position(user_id, a_cinquanta_metri()),
            Some(UserState::Moving),
            "la preparazione del test deve lasciare l'utente in movimento"
        );
    }

    #[test]
    fn la_soglia_decide_il_passaggio_in_movimento() {
        let (_orologio, tracker) = tracciatore();
        tracker.on_connect(UserId(1));

        // Senza una coordinata precedente nessun punto dice che il veicolo si sposta.
        assert_eq!(tracker.on_position(UserId(1), partenza()), None);
        assert_eq!(tracker.current_state(UserId(1)), Some(UserState::Still));

        assert_eq!(tracker.on_position(UserId(1), a_un_metro()), None);
        assert_eq!(tracker.current_state(UserId(1)), Some(UserState::Still));

        assert_eq!(
            tracker.on_position(UserId(1), a_cinquanta_metri()),
            Some(UserState::Moving)
        );

        // Gia' in movimento: la posizione va presa in carico, ma non c'e' nessun cambio
        // da comunicare al client.
        assert_eq!(tracker.on_position(UserId(1), a_cento_metri()), None);
        assert_eq!(tracker.current_state(UserId(1)), Some(UserState::Moving));
    }

    #[test]
    fn una_posizione_senza_connessione_crea_la_voce_senza_panico() {
        let (_orologio, tracker) = tracciatore();

        assert_eq!(tracker.current_state(UserId(42)), None);
        assert_eq!(tracker.on_position(UserId(42), partenza()), None);
        assert_eq!(tracker.current_state(UserId(42)), Some(UserState::Still));
    }

    #[test]
    fn tick_riporta_fermo_solo_dopo_la_scadenza() {
        let (orologio, tracker) = tracciatore();
        porta_in_movimento(&tracker, UserId(1));

        orologio.advance(TimeDelta::seconds(soglia_fermo_secs() - 1));
        assert!(tracker.tick().is_empty());
        assert_eq!(tracker.current_state(UserId(1)), Some(UserState::Moving));

        orologio.advance(TimeDelta::seconds(2));
        assert_eq!(tracker.tick(), vec![(UserId(1), UserState::Still)]);
        assert_eq!(tracker.current_state(UserId(1)), Some(UserState::Still));

        // Gia' fermo: una seconda chiamata non lo segnala di nuovo.
        assert!(tracker.tick().is_empty());
    }

    #[test]
    fn una_posizione_prima_della_scadenza_azzera_il_conteggio() {
        let (orologio, tracker) = tracciatore();
        porta_in_movimento(&tracker, UserId(1));

        // A dieci secondi dalla scadenza arriva un altro spostamento oltre la soglia.
        orologio.advance(TimeDelta::seconds(soglia_fermo_secs() - 10));
        assert_eq!(tracker.on_position(UserId(1), a_cento_metri()), None);

        // Venti secondi dopo sono passati piu' di tre minuti dal primo spostamento, ma
        // solo venti secondi dal secondo: il conteggio riparte da li'.
        orologio.advance(TimeDelta::seconds(20));

        assert!(
            tracker.tick().is_empty(),
            "il conteggio deve ripartire dall'ultimo spostamento, non dal primo"
        );
    }

    #[test]
    fn il_passare_del_tempo_non_tocca_chi_e_fermo_o_disconnesso() {
        let (orologio, tracker) = tracciatore();
        tracker.on_connect(UserId(1));
        tracker.on_position(UserId(1), partenza());
        porta_in_movimento(&tracker, UserId(2));
        tracker.on_disconnect(UserId(2));

        orologio.advance(TimeDelta::seconds(soglia_fermo_secs() * 10));

        assert!(tracker.tick().is_empty());
        assert_eq!(tracker.current_state(UserId(1)), Some(UserState::Still));
        assert_eq!(
            tracker.current_state(UserId(2)),
            Some(UserState::Disconnected)
        );
    }

    #[test]
    fn la_disconnessione_di_uno_sconosciuto_non_crea_la_voce() {
        let (_orologio, tracker) = tracciatore();

        tracker.on_disconnect(UserId(1));

        assert_eq!(tracker.current_state(UserId(1)), None);
    }

    #[test]
    fn la_riconnessione_riparte_da_fermo_e_dimentica_l_ultima_posizione() {
        let (_orologio, tracker) = tracciatore();
        porta_in_movimento(&tracker, UserId(1));
        tracker.on_disconnect(UserId(1));

        tracker.on_connect(UserId(1));
        assert_eq!(tracker.current_state(UserId(1)), Some(UserState::Still));

        // Un punto lontanissimo da quello di prima della disconnessione: se il confronto
        // usasse ancora quella coordinata, questa sarebbe una posizione in movimento.
        assert_eq!(
            tracker.on_position(UserId(1), coord(45.4642, 9.19)),
            None,
            "la prima posizione dopo il rientro non ha un precedente con cui confrontarsi"
        );
    }
}
