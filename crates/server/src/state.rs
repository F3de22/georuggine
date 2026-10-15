//! Macchina a stati fermo/movimento dei veicoli.
//!
//! Il passaggio a [`UserState::Moving`] avviene su una posizione
//! ([`StateTracker::on_position`]); il ritorno a [`UserState::Still`] dipende solo dal
//! tempo trascorso e lo esegue [`StateTracker::tick`], chiamato periodicamente dal
//! livello di rete. I metodi sono sincroni, quindi basta il `Mutex` della libreria
//! standard.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use chrono::{DateTime, Utc};
use georuggine_common::clock::Clock;
use georuggine_common::types::{
    Coordinate, MOVEMENT_THRESHOLD_METERS, STILL_AFTER_SECS, UserId, UserState, haversine_meters,
};

/// [`STILL_AFTER_SECS`] come `i64`, saturando invece di troncare.
fn soglia_fermo_secs() -> i64 {
    i64::try_from(STILL_AFTER_SECS).unwrap_or(i64::MAX)
}

/// Cio' che si ricorda di un utente per decidere se e' fermo o in movimento.
#[derive(Debug, Clone, PartialEq)]
struct UserTracking {
    /// Stato comunicato ai client.
    state: UserState,
    /// Ultima posizione ricevuta; `None` dopo la connessione, quindi la prima posizione
    /// di una sessione non porta mai in movimento.
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

/// Stato di movimento di tutti gli utenti, condiviso fra le connessioni.
pub struct StateTracker {
    /// Le voci restano anche dopo la disconnessione e vengono reimpostate al rientro.
    utenti: Mutex<HashMap<UserId, UserTracking>>,
    /// Sorgente dell'ora.
    orologio: Arc<dyn Clock>,
}

impl StateTracker {
    /// Costruisce un tracciatore vuoto.
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

    /// Registra una posizione e restituisce il nuovo stato solo se e' cambiato.
    ///
    /// Il ritorno a fermo non avviene qui ma in [`Self::tick`].
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

    /// Apre o riapre il tracciamento di un utente, che riparte fermo e senza posizione
    /// precedente: dopo una riconnessione la vecchia coordinata non e' affidabile.
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

    /// Riporta a [`UserState::Still`] chi non si sposta da [`STILL_AFTER_SECS`] secondi
    /// e restituisce gli utenti cambiati, ognuno una volta sola.
    pub fn tick(&self) -> Vec<(UserId, UserState)> {
        let adesso = self.orologio.now();
        let soglia = soglia_fermo_secs();
        let mut passati_a_fermo = Vec::new();

        let mut utenti = self.blocca();
        for (user_id, tracciamento) in utenti.iter_mut() {
            if tracciamento.state != UserState::Moving {
                continue;
            }
            // Con un orologio tornato indietro la differenza e' negativa e l'utente resta
            // in movimento.
            let fermo_da = (adesso - tracciamento.last_movement_at).num_seconds();
            if fermo_da >= soglia {
                tracciamento.state = UserState::Still;
                passati_a_fermo.push((*user_id, UserState::Still));
            }
        }

        passati_a_fermo
    }

    /// Prende il lock anche se avvelenato: ogni sezione critica lascia la mappa coerente.
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

    /// Circa 50 m a nord di [`partenza`], ben oltre la soglia.
    fn a_cinquanta_metri() -> Coordinate {
        coord(45.0703 + 0.000_45, 7.6869)
    }

    /// Circa 100 m a nord di [`partenza`], distante anche da [`a_cinquanta_metri`].
    fn a_cento_metri() -> Coordinate {
        coord(45.0703 + 0.000_9, 7.6869)
    }

    /// Tracciatore con orologio finto, restituito per poterlo spostare.
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

        // Gia' in movimento: nessun cambio da comunicare.
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

        // Il conteggio dei tre minuti riparte dall'ultimo spostamento.
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

        // Punto lontano da quello precedente alla disconnessione: non deve contare come
        // movimento.
        assert_eq!(
            tracker.on_position(UserId(1), coord(45.4642, 9.19)),
            None,
            "la prima posizione dopo il rientro non ha un precedente con cui confrontarsi"
        );
    }
}
