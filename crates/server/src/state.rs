//! Macchina a stati fermo/movimento dei veicoli: requisiti R4 e R5.
//!
//! Il livello di rete sa quando una posizione arriva, ma non se quella posizione sia uno
//! spostamento: per deciderlo servono la coordinata precedente dello stesso utente e
//! l'istante dell'ultimo movimento. [`StateTracker`] tiene quei due dati per ogni utente
//! e li usa per rispondere alle due domande della traccia: se la nuova posizione dista
//! piu' di [`MOVEMENT_THRESHOLD_METERS`] dalla precedente l'utente e' in movimento (R4),
//! e se non si sposta per [`STILL_AFTER_SECS`] secondi torna fermo (R5).
//!
//! # Perche' due percorsi diversi
//!
//! Il passaggio a [`UserState::Moving`] avviene su un evento, la posizione, e vive
//! quindi dentro [`PositionObserver::on_position`]. Il ritorno a [`UserState::Still`]
//! non ha un evento che lo provochi: succede perche' il tempo passa mentre non arriva
//! nulla, e non c'e' nessuna chiamata su cui agganciarlo. Sta percio' in [`tick`], che
//! non fa parte del trait e che il server chiamera' a intervalli regolari da un task
//! dedicato. Il metodo e' puro calcolo sullo stato interno: restituisce chi e' passato a
//! fermo e lascia al chiamante il compito di avvisare i client.
//!
//! [`tick`]: StateTracker::tick
//!
//! # Scelte di implementazione
//!
//! L'orologio arriva come `Arc<dyn Clock>` invece che per parametro generico, come in
//! [`InMemoryRepository`] e in [`ConnectionContext`]: il tracciatore viene condiviso fra
//! tutte le connessioni come `Arc<dyn PositionObserver>`, quindi il tipo dell'orologio
//! sarebbe comunque cancellato al primo uso e renderlo generico propagherebbe un
//! parametro che nessuno legge.
//!
//! La mappa sta sotto il [`Mutex`] della libreria standard e non sotto quello di tokio
//! perche' i metodi di [`PositionObserver`] sono sincroni: nessuna sezione critica
//! contiene un punto di sospensione, e il guard non attraversa mai un `.await`. Ogni
//! sezione critica e' un accesso a una `HashMap` in memoria, quindi il task che serve la
//! connessione non resta fermo in modo apprezzabile.
//!
//! [`InMemoryRepository`]: crate::storage::InMemoryRepository
//! [`ConnectionContext`]: crate::net::ConnectionContext

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use chrono::{DateTime, Utc};
use georuggine_common::clock::Clock;
use georuggine_common::types::{
    Coordinate, MOVEMENT_THRESHOLD_METERS, STILL_AFTER_SECS, UserId, UserState, haversine_meters,
};

use crate::observer::PositionObserver;

/// [`STILL_AFTER_SECS`] nel tipo con cui si confrontano le durate di chrono.
///
/// La conversione non puo' fallire con il valore attuale della costante, ma si usa
/// comunque la forma fallibile: `as` troncherebbe in silenzio se qualcuno alzasse la
/// soglia oltre [`i64::MAX`], e l'`unwrap` e' vietato dalle convenzioni del progetto.
/// Saturare a [`i64::MAX`] e' l'esito prudente: nessun utente verrebbe piu' riportato a
/// fermo, invece di esserlo per un valore troncato che non c'entra nulla.
fn soglia_fermo_secs() -> i64 {
    i64::try_from(STILL_AFTER_SECS).unwrap_or(i64::MAX)
}

/// Cio' che si ricorda di un utente per decidere se e' fermo o in movimento.
#[derive(Debug, Clone, PartialEq)]
struct UserTracking {
    /// Stato comunicato ai client di monitoraggio.
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

/// Macchina a stati fermo/movimento di tutti gli utenti, condivisa fra le connessioni.
///
/// Implementa [`PositionObserver`], quindi il livello di rete la usa senza conoscerla:
/// si costruisce una volta all'avvio e si passa a [`ConnectionContext`] come
/// `Arc<dyn PositionObserver>`.
///
/// [`ConnectionContext`]: crate::net::ConnectionContext
pub struct StateTracker {
    /// Cio' che si ricorda di ogni utente, indicizzato per id.
    ///
    /// Le voci non vengono mai rimosse: alla disconnessione lo stato diventa
    /// [`UserState::Disconnected`] e la voce resta, cosi' [`StateTracker::tick`] non
    /// trova buchi e una riconnessione la reimposta senza doverla ricreare.
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

    /// Riporta a [`UserState::Still`] chi non si sposta da [`STILL_AFTER_SECS`] secondi.
    ///
    /// Restituisce le coppie `(utente, stato)` passate a fermo in questa chiamata, che
    /// il chiamante inoltra ai client interessati; l'ordine non e' significativo. Gli
    /// utenti gia' fermi e quelli disconnessi non vengono toccati, quindi due chiamate
    /// consecutive senza posizioni in mezzo segnalano lo stesso utente una volta sola.
    ///
    /// Non fa parte di [`PositionObserver`] perche' non risponde a un evento della
    /// connessione: e' il server a chiamarla a intervalli regolari.
    pub fn tick(&self) -> Vec<(UserId, UserState)> {
        let adesso = self.orologio.now();
        let soglia = soglia_fermo_secs();
        let mut passati_a_fermo = Vec::new();

        let mut utenti = self.blocca();
        for (user_id, tracciamento) in utenti.iter_mut() {
            if tracciamento.state != UserState::Moving {
                continue;
            }
            // `num_seconds` tronca verso lo zero e non va in overflow: se l'orologio
            // tornasse indietro la differenza sarebbe negativa e l'utente resterebbe in
            // movimento, che e' l'esito prudente.
            let fermo_da = (adesso - tracciamento.last_movement_at).num_seconds();
            if fermo_da >= soglia {
                tracciamento.state = UserState::Still;
                passati_a_fermo.push((*user_id, UserState::Still));
            }
        }

        passati_a_fermo
    }

    /// Prende il lock sulla mappa, riprendendolo anche se e' avvelenato.
    ///
    /// L'avvelenamento segnala che un altro thread e' andato in panico tenendo il lock.
    /// Come in [`InMemoryRepository::blocca`], ogni sezione critica e' un aggiornamento
    /// che non lascia lo stato a meta': al peggio la voce di un utente e' quella di
    /// prima, e la posizione successiva la rimette in pari. Si continua percio' con
    /// [`PoisonError::into_inner`] invece dell'`unwrap` che il progetto vieta.
    ///
    /// [`InMemoryRepository::blocca`]: crate::storage::InMemoryRepository
    fn blocca(&self) -> MutexGuard<'_, HashMap<UserId, UserTracking>> {
        self.utenti.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl PositionObserver for StateTracker {
    /// Confronta la posizione con la precedente e segnala il passaggio a
    /// [`UserState::Moving`].
    ///
    /// La transizione inversa non avviene qui: una posizione che non si sposta non rende
    /// fermo l'utente, perche' il requisito parla di tre minuti senza movimento e non
    /// della singola rilevazione. Se ne occupa [`StateTracker::tick`].
    ///
    /// Un utente senza voce nella mappa non e' un errore: non dovrebbe accadere, perche'
    /// il gestore chiama sempre [`PositionObserver::on_connect`] prima, ma qui la voce
    /// viene semplicemente creata come per una connessione appena aperta.
    fn on_position(&self, user_id: UserId, coordinate: Coordinate) -> Option<UserState> {
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

        // `None` anche quando l'utente era gia' in movimento: il client conosce gia'
        // quello stato e un secondo annuncio non gli direbbe nulla di nuovo.
        (tracciamento.state != precedente).then_some(tracciamento.state)
    }

    /// Apre (o riapre) il tracciamento di un utente, che riparte fermo.
    ///
    /// La voce viene sostituita, non aggiornata: dopo una riconnessione la coordinata di
    /// prima della disconnessione non e' piu' un termine di paragone valido, perche' nel
    /// frattempo il veicolo puo' essersi spostato ovunque senza che il server lo sappia.
    /// Confrontarla con la prima posizione della nuova sessione segnalerebbe un
    /// movimento che nessuno ha osservato.
    fn on_connect(&self, user_id: UserId) {
        let adesso = self.orologio.now();

        self.blocca().insert(user_id, UserTracking::nuovo(adesso));
    }

    /// Segna l'utente come [`UserState::Disconnected`], se e' noto.
    ///
    /// La voce resta nella mappa: [`PositionObserver::on_connect`] la reimposta comunque
    /// a una sessione nuova, e tenerla evita che [`StateTracker::tick`] trovi un buco.
    /// Su un utente sconosciuto non fa nulla e non e' un errore.
    fn on_disconnect(&self, user_id: UserId) {
        if let Some(tracciamento) = self.blocca().get_mut(&user_id) {
            tracciamento.state = UserState::Disconnected;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use chrono::TimeDelta;
    use georuggine_common::clock::FakeClock;

    /// Istante di riferimento dei test: 2024-01-01T00:00:00Z.
    fn istante_base() -> DateTime<Utc> {
        DateTime::from_timestamp(1_704_067_200, 0).expect("istante valido")
    }

    /// Costruisce una coordinata che il test da' per valida.
    fn coord(lat: f64, lon: f64) -> Coordinate {
        Coordinate::new(lat, lon).expect("coordinata di test valida")
    }

    /// Punto di partenza dei test: piazza Castello, Torino.
    fn partenza() -> Coordinate {
        coord(45.0703, 7.6869)
    }

    /// Punto circa 50 m a nord di [`partenza`], quindi oltre la soglia di movimento.
    ///
    /// Un grado di latitudine vale circa 111_320 m, quindi 0,000_45 gradi valgono circa
    /// 50 m: dieci volte [`MOVEMENT_THRESHOLD_METERS`], abbastanza perche' il test non
    /// dipenda dalla precisione della formula.
    fn a_cinquanta_metri() -> Coordinate {
        coord(45.0703 + 0.000_45, 7.6869)
    }

    /// Punto circa 1 m a nord di [`partenza`], quindi sotto la soglia di movimento.
    fn a_un_metro() -> Coordinate {
        coord(45.0703 + 0.000_009, 7.6869)
    }

    /// Punto circa 100 m a nord di [`partenza`]: serve dove occorre un terzo punto
    /// distante sia dalla partenza sia da [`a_cinquanta_metri`].
    fn a_cento_metri() -> Coordinate {
        coord(45.0703 + 0.000_9, 7.6869)
    }

    /// Costruisce un tracciatore con un orologio finto fermo su [`istante_base`].
    ///
    /// Restituisce anche l'orologio, cosi' il test puo' spostarlo: senza, verificare la
    /// scadenza di [`STILL_AFTER_SECS`] richiederebbe di aspettare tre minuti veri.
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

    // --- prima posizione dopo la connessione ---

    #[test]
    fn la_prima_posizione_dopo_la_connessione_non_produce_movimento() {
        let (_orologio, tracker) = tracciatore();
        tracker.on_connect(UserId(1));

        // Non c'e' una coordinata precedente con cui confrontarla: qualunque punto sia,
        // non dice nulla sul fatto che il veicolo si stia spostando.
        assert_eq!(tracker.on_position(UserId(1), partenza()), None);
        assert_eq!(tracker.current_state(UserId(1)), Some(UserState::Still));
    }

    #[test]
    fn on_position_senza_on_connect_crea_la_voce_senza_panico() {
        let (_orologio, tracker) = tracciatore();

        assert_eq!(tracker.on_position(UserId(1), partenza()), None);
        assert_eq!(tracker.current_state(UserId(1)), Some(UserState::Still));
    }

    #[test]
    fn current_state_di_un_utente_mai_visto_e_none() {
        let (_orologio, tracker) = tracciatore();

        assert_eq!(tracker.current_state(UserId(42)), None);
    }

    // --- soglia di movimento ---

    #[test]
    fn uno_spostamento_di_cinquanta_metri_porta_in_movimento() {
        let (_orologio, tracker) = tracciatore();
        tracker.on_connect(UserId(1));
        tracker.on_position(UserId(1), partenza());

        assert_eq!(
            tracker.on_position(UserId(1), a_cinquanta_metri()),
            Some(UserState::Moving)
        );
        assert_eq!(tracker.current_state(UserId(1)), Some(UserState::Moving));
    }

    #[test]
    fn uno_spostamento_di_un_metro_non_cambia_stato() {
        let (_orologio, tracker) = tracciatore();
        tracker.on_connect(UserId(1));
        tracker.on_position(UserId(1), partenza());

        assert_eq!(tracker.on_position(UserId(1), a_un_metro()), None);
        assert_eq!(
            tracker.current_state(UserId(1)),
            Some(UserState::Still),
            "sotto la soglia lo stato resta quello di prima"
        );
    }

    #[test]
    fn un_secondo_spostamento_non_ripete_il_cambio_di_stato() {
        let (_orologio, tracker) = tracciatore();
        porta_in_movimento(&tracker, UserId(1));

        // L'utente e' gia' Moving: la posizione va comunque presa in carico, ma non c'e'
        // nessun cambio da comunicare al client.
        assert_eq!(tracker.on_position(UserId(1), a_cento_metri()), None);
        assert_eq!(tracker.current_state(UserId(1)), Some(UserState::Moving));
    }

    // --- scadenza dei tre minuti ---

    #[test]
    fn tick_prima_della_scadenza_lascia_l_utente_in_movimento() {
        let (orologio, tracker) = tracciatore();
        porta_in_movimento(&tracker, UserId(1));

        orologio.advance(TimeDelta::seconds(soglia_fermo_secs() - 1));

        assert!(tracker.tick().is_empty());
        assert_eq!(tracker.current_state(UserId(1)), Some(UserState::Moving));
    }

    #[test]
    fn tick_dopo_la_scadenza_riporta_l_utente_fermo() {
        let (orologio, tracker) = tracciatore();
        porta_in_movimento(&tracker, UserId(1));

        orologio.advance(TimeDelta::seconds(soglia_fermo_secs() + 1));

        assert_eq!(tracker.tick(), vec![(UserId(1), UserState::Still)]);
        assert_eq!(tracker.current_state(UserId(1)), Some(UserState::Still));
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
        assert_eq!(tracker.current_state(UserId(1)), Some(UserState::Moving));
    }

    #[test]
    fn tick_su_una_mappa_senza_utenti_in_movimento_torna_vuoto() {
        let (orologio, tracker) = tracciatore();
        tracker.on_connect(UserId(1));
        tracker.on_position(UserId(1), partenza());
        tracker.on_connect(UserId(2));
        tracker.on_disconnect(UserId(2));

        orologio.advance(TimeDelta::seconds(soglia_fermo_secs() * 10));

        assert!(tracker.tick().is_empty());
        assert_eq!(tracker.current_state(UserId(1)), Some(UserState::Still));
        assert_eq!(
            tracker.current_state(UserId(2)),
            Some(UserState::Disconnected),
            "un utente disconnesso non viene toccato dal passare del tempo"
        );
    }

    #[test]
    fn tick_su_un_tracciatore_vuoto_torna_vuoto() {
        let (_orologio, tracker) = tracciatore();

        assert!(tracker.tick().is_empty());
    }

    // --- disconnessione e riconnessione ---

    #[test]
    fn on_disconnect_durante_il_movimento_porta_a_disconnected() {
        let (_orologio, tracker) = tracciatore();
        porta_in_movimento(&tracker, UserId(1));

        tracker.on_disconnect(UserId(1));

        assert_eq!(
            tracker.current_state(UserId(1)),
            Some(UserState::Disconnected)
        );
    }

    #[test]
    fn on_disconnect_di_un_utente_sconosciuto_non_crea_la_voce() {
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
        let milano = coord(45.4642, 9.19);
        assert_eq!(
            tracker.on_position(UserId(1), milano),
            None,
            "dopo la riconnessione la prima posizione non ha un precedente con cui confrontarsi"
        );
        assert_eq!(tracker.current_state(UserId(1)), Some(UserState::Still));
    }

    // --- forma richiesta dal livello di rete ---

    #[test]
    fn state_tracker_e_usabile_dietro_arc_dyn() {
        let orologio: Arc<dyn Clock> = Arc::new(FakeClock::new(istante_base()));
        // Forma in cui il contesto di connessione tiene l'osservatore: se `StateTracker`
        // perdesse i bound `Send + Sync + 'static`, questa riga non compilerebbe piu'.
        let condiviso: Arc<dyn PositionObserver> = Arc::new(StateTracker::new(orologio));

        condiviso.on_connect(UserId(1));
        assert_eq!(condiviso.on_position(UserId(1), partenza()), None);
        assert_eq!(
            condiviso.on_position(UserId(1), a_cinquanta_metri()),
            Some(UserState::Moving)
        );
    }
}
