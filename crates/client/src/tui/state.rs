//! Stato della TUI, aggiornato solo da [`UiState::applica_evento`] e separato dal
//! disegno per poterlo testare senza terminale.

use std::collections::VecDeque;

use chrono::{DateTime, Utc};
use georuggine_common::protocol::MovementSummary;
use georuggine_common::types::{Coordinate, UserState, Username, haversine_meters};

use crate::core::ClientEvent;

/// Voci massime in [`UiState::storico_posizioni`].
const STORICO_POSIZIONI_MAX: usize = 20;

/// Voci massime in [`UiState::messaggi`].
const MESSAGGI_MAX: usize = 200;

/// Stato mostrato dalla TUI, aggiornato solo da [`UiState::applica_evento`].
#[derive(Debug, Clone)]
pub struct UiState {
    /// Vero se il task di rete ha una connessione aperta in questo momento.
    pub connesso: bool,
    /// Ultimo stato di movimento notificato dal server.
    pub stato_movimento: Option<UserState>,
    /// Nome utente con cui il client si e' autenticato.
    pub username: Username,
    /// Nome della sorgente di posizione in uso.
    pub sorgente: &'static str,
    /// Ultima posizione inviata con successo al server, con l'istante dell'invio.
    pub ultima_posizione: Option<(Coordinate, DateTime<Utc>)>,
    /// Ultime posizioni inviate, con la distanza dalla precedente; al massimo
    /// `STORICO_POSIZIONI_MAX`.
    pub storico_posizioni: VecDeque<(Coordinate, DateTime<Utc>, f64)>,
    /// Messaggi ricevuti: testo, orario e `true` se diretto; al massimo `MESSAGGI_MAX`.
    pub messaggi: VecDeque<(String, DateTime<Utc>, bool)>,
    /// Ultimo riepilogo dei movimenti ricevuto, sovrascritto da ogni nuova risposta.
    pub ultima_analisi: Option<MovementSummary>,
    /// Testo digitato dall'operatore, non ancora confermato con Invio.
    pub riga_di_input: String,
    /// Ultimo errore o motivo di disconnessione; si azzera alla connessione.
    pub ultimo_errore: Option<String>,
}

impl UiState {
    /// Stato iniziale: non connesso, nessuno storico, nessun errore.
    pub fn new(username: Username, sorgente: &'static str) -> Self {
        Self {
            connesso: false,
            stato_movimento: None,
            username,
            sorgente,
            ultima_posizione: None,
            storico_posizioni: VecDeque::new(),
            messaggi: VecDeque::new(),
            ultima_analisi: None,
            riga_di_input: String::new(),
            ultimo_errore: None,
        }
    }

    /// Aggiorna lo stato con un evento; `adesso` e' l'orario da registrare.
    pub fn applica_evento(&mut self, evento: ClientEvent, adesso: DateTime<Utc>) {
        match evento {
            ClientEvent::Connected => {
                self.connesso = true;
                self.ultimo_errore = None;
            }
            ClientEvent::Disconnected(motivo) => {
                self.connesso = false;
                self.ultimo_errore = Some(motivo);
            }
            ClientEvent::ServerText { body, direct } => {
                accoda(&mut self.messaggi, (body, adesso, direct), MESSAGGI_MAX);
            }
            ClientEvent::StateChanged(stato) => {
                self.stato_movimento = Some(stato);
            }
            ClientEvent::PositionSent(coordinate, recorded_at) => {
                let distanza = self
                    .storico_posizioni
                    .back()
                    .map(|(precedente, ..)| haversine_meters(*precedente, coordinate))
                    .unwrap_or(0.0);
                accoda(
                    &mut self.storico_posizioni,
                    (coordinate, recorded_at, distanza),
                    STORICO_POSIZIONI_MAX,
                );
                self.ultima_posizione = Some((coordinate, recorded_at));
            }
            ClientEvent::AnalysisReceived(riepilogo) => {
                self.ultima_analisi = Some(riepilogo);
            }
            ClientEvent::Error(messaggio) => {
                self.ultimo_errore = Some(messaggio);
            }
        }
    }
}

/// Accoda un elemento, scartando il piu' vecchio se la coda e' piena.
fn accoda<T>(coda: &mut VecDeque<T>, elemento: T, capacita: usize) {
    if coda.len() >= capacita {
        coda.pop_front();
    }
    coda.push_back(elemento);
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Istante di riferimento dei test, piu' `secondi`.
    fn a(secondi: i64) -> DateTime<Utc> {
        let base = DateTime::from_timestamp(1_704_067_200, 0).expect("istante valido");
        base + chrono::TimeDelta::try_seconds(secondi).expect("scarto valido")
    }

    /// Costruisce una coordinata che il test da' per valida.
    fn coord(lat: f64, lon: f64) -> Coordinate {
        Coordinate::new(lat, lon).expect("coordinata di test valida")
    }

    /// Stato iniziale di prova.
    fn stato() -> UiState {
        UiState::new(
            Username::new("veicolo_01").expect("username di test valido"),
            "manual",
        )
    }

    #[test]
    fn connected_imposta_connesso_a_true() {
        let mut stato = stato();
        stato.applica_evento(ClientEvent::Connected, a(0));
        assert!(stato.connesso);
    }

    #[test]
    fn connected_azzera_l_errore_precedente() {
        let mut stato = stato();
        stato.applica_evento(ClientEvent::Error("boom".to_owned()), a(0));
        stato.applica_evento(ClientEvent::Connected, a(1));
        assert_eq!(stato.ultimo_errore, None);
    }

    #[test]
    fn disconnected_imposta_connesso_a_false_e_salva_il_motivo() {
        let mut stato = stato();
        stato.applica_evento(ClientEvent::Connected, a(0));
        stato.applica_evento(
            ClientEvent::Disconnected("connessione rifiutata".to_owned()),
            a(1),
        );

        assert!(!stato.connesso);
        assert_eq!(
            stato.ultimo_errore,
            Some("connessione rifiutata".to_owned())
        );
    }

    #[test]
    fn position_sent_registra_la_distanza_dalla_precedente_nello_storico() {
        let mut stato = stato();
        let torino = coord(45.0703, 7.6869);
        let milano = coord(45.4642, 9.1900);

        stato.applica_evento(ClientEvent::PositionSent(torino, a(0)), a(0));
        assert_eq!(
            stato.storico_posizioni.back().map(|(_, _, d)| *d),
            Some(0.0)
        );

        stato.applica_evento(ClientEvent::PositionSent(milano, a(60)), a(60));
        let (coordinata, quando, distanza) = *stato
            .storico_posizioni
            .back()
            .expect("una voce e' stata accodata");
        assert_eq!(coordinata, milano);
        assert_eq!(quando, a(60));
        assert!(
            (distanza - haversine_meters(torino, milano)).abs() < f64::EPSILON,
            "distanza inattesa: {distanza}"
        );
        assert_eq!(stato.ultima_posizione, Some((milano, a(60))));
    }

    #[test]
    fn storico_posizioni_non_supera_venti_elementi() {
        let mut stato = stato();
        for i in 0..30 {
            let punto = coord(45.0 + f64::from(i) * 0.001, 7.0);
            stato.applica_evento(
                ClientEvent::PositionSent(punto, a(i64::from(i))),
                a(i64::from(i)),
            );
        }

        assert_eq!(stato.storico_posizioni.len(), 20);
        // Le dieci piu' vecchie sono state scartate.
        let attesa = coord(45.0 + 10.0 * 0.001, 7.0);
        assert_eq!(
            stato.storico_posizioni.front().map(|(c, ..)| *c),
            Some(attesa)
        );
    }

    #[test]
    fn server_text_registra_testo_orario_e_tipo() {
        let mut stato = stato();
        stato.applica_evento(
            ClientEvent::ServerText {
                body: "rientra in sede".to_owned(),
                direct: true,
            },
            a(5),
        );

        assert_eq!(
            stato.messaggi.back(),
            Some(&("rientra in sede".to_owned(), a(5), true))
        );
    }

    #[test]
    fn messaggi_non_supera_duecento_elementi() {
        let mut stato = stato();
        for i in 0..250 {
            stato.applica_evento(
                ClientEvent::ServerText {
                    body: format!("messaggio {i}"),
                    direct: false,
                },
                a(i64::from(i)),
            );
        }

        assert_eq!(stato.messaggi.len(), 200);
        assert_eq!(
            stato.messaggi.front().map(|(testo, ..)| testo.clone()),
            Some("messaggio 50".to_owned())
        );
    }

    #[test]
    fn state_changed_aggiorna_lo_stato_di_movimento() {
        let mut stato = stato();
        stato.applica_evento(ClientEvent::StateChanged(UserState::Moving), a(0));
        assert_eq!(stato.stato_movimento, Some(UserState::Moving));

        stato.applica_evento(ClientEvent::StateChanged(UserState::Still), a(1));
        assert_eq!(stato.stato_movimento, Some(UserState::Still));
    }

    #[test]
    fn analysis_received_sovrascrive_l_ultima_analisi() {
        let mut stato = stato();
        let primo = MovementSummary {
            total_distance_meters: 100.0,
            moving_duration_secs: 10,
            pause_duration_secs: 0,
            average_speed_kmh: 36.0,
            stop_count: 0,
            route: Vec::new(),
            route_points: 0,
        };
        let secondo = MovementSummary {
            total_distance_meters: 200.0,
            ..primo.clone()
        };

        stato.applica_evento(ClientEvent::AnalysisReceived(primo), a(0));
        stato.applica_evento(ClientEvent::AnalysisReceived(secondo.clone()), a(1));

        assert_eq!(stato.ultima_analisi, Some(secondo));
    }

    #[test]
    fn error_non_tocca_connesso_e_si_registra_in_ultimo_errore() {
        let mut stato = stato();
        stato.applica_evento(ClientEvent::Connected, a(0));
        stato.applica_evento(ClientEvent::Error("richiesta rifiutata".to_owned()), a(1));

        assert!(
            stato.connesso,
            "un errore non fatale non deve chiudere la connessione mostrata"
        );
        assert_eq!(stato.ultimo_errore, Some("richiesta rifiutata".to_owned()));
    }
}
