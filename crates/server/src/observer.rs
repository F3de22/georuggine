//! Osservazione degli eventi di una sessione: ingresso, posizioni, uscita.
//!
//! Il gestore di connessione persiste le posizioni ma non decide se un veicolo e' fermo
//! o in movimento: quel calcolo appartiene alla macchina a stati del requisito R5, che
//! ha bisogno di ricordare la posizione precedente di ogni utente e l'istante dell'ultimo
//! spostamento. Metterla dentro [`crate::net`] legherebbe il livello di rete a uno stato
//! che non gli serve; qui c'e' invece il solo confine fra i due, il trait
//! [`PositionObserver`], che il livello di rete chiama senza sapere chi lo implementa.
//!
//! Le implementazioni di questo modulo sono due: [`NoopObserver`], che non fa nulla e
//! serve dove lo stato di movimento non interessa (e finche' la macchina a stati non
//! esiste), e in seguito quella vera. Il trait e' sincrono perche' le sue chiamate
//! aggiornano una struttura in memoria: nessuna delle due tocca il disco o la rete, e
//! renderlo `async` costringerebbe ogni implementazione a un tipo di ritorno che non
//! serve a nessuna.

use georuggine_common::types::{Coordinate, UserId, UserState};

/// Destinatario degli eventi che una connessione autenticata produce.
///
/// I bound `Send + Sync + 'static` stanno sul trait perche' un osservatore e' condiviso
/// fra tutte le connessioni come `Arc<dyn PositionObserver>`, come il [`Clock`] e il
/// [`Repository`] accanto a cui vive nel contesto di connessione.
///
/// Le tre chiamate arrivano dal task che serve la connessione: un'implementazione che vi
/// si fermasse a lungo ne ritarderebbe le risposte. Devono essere brevi.
///
/// [`Clock`]: georuggine_common::clock::Clock
/// [`Repository`]: crate::storage::Repository
pub trait PositionObserver: Send + Sync + 'static {
    /// Prende in carico una posizione appena persistita.
    ///
    /// Restituisce `Some(stato)` se questa posizione fa cambiare stato all'utente, e il
    /// chiamante lo comunica al client con [`ServerMessage::StateChanged`]; `None` se lo
    /// stato resta quello di prima, che e' il caso normale.
    ///
    /// [`ServerMessage::StateChanged`]: georuggine_common::protocol::ServerMessage::StateChanged
    fn on_position(&self, user_id: UserId, coordinate: Coordinate) -> Option<UserState>;

    /// Segnala che l'utente ha aperto una sessione.
    fn on_connect(&self, user_id: UserId);

    /// Segnala che la sessione dell'utente si e' chiusa, per logout, silenzio o guasto.
    fn on_disconnect(&self, user_id: UserId);
}

/// Osservatore che ignora ogni evento.
///
/// E' il valore predefinito finche' la macchina a stati non e' collegata, e resta utile
/// dopo: i test del livello di rete che non riguardano lo stato di movimento lo usano
/// per non dover costruire un osservatore vero.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct NoopObserver;

impl PositionObserver for NoopObserver {
    fn on_position(&self, _user_id: UserId, _coordinate: Coordinate) -> Option<UserState> {
        None
    }

    fn on_connect(&self, _user_id: UserId) {}

    fn on_disconnect(&self, _user_id: UserId) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    /// Coordinata che il test da' per valida.
    fn coordinata() -> Coordinate {
        Coordinate::new(45.0703, 7.6869).expect("coordinata di test valida")
    }

    #[test]
    fn noop_non_segnala_mai_un_cambio_di_stato() {
        let osservatore = NoopObserver;

        osservatore.on_connect(UserId(1));
        assert_eq!(osservatore.on_position(UserId(1), coordinata()), None);
        osservatore.on_disconnect(UserId(1));
    }

    #[test]
    fn noop_e_usabile_dietro_arc_dyn() {
        // Forma in cui il contesto di connessione tiene l'osservatore: se il trait
        // perdesse i bound `Send + Sync + 'static`, questa riga non compilerebbe piu'.
        let condiviso: Arc<dyn PositionObserver> = Arc::new(NoopObserver);

        assert_eq!(condiviso.on_position(UserId(7), coordinata()), None);
    }
}
