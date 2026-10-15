//! Protocollo applicativo fra client e server.
//!
//! # Sequenza attesa
//!
//! ```text
//! Hello -> Welcome -> Register|Login -> SessionToken
//!       -> (Position | TextToServer | RequestAnalysis | Ping)*
//!       -> Logout
//! ```
//!
//! Fuori sequenza il server risponde con [`ErrorCode::NotAuthenticated`]. Broadcast,
//! Direct e StateChanged possono arrivare in qualsiasi momento.
//!
//! postcard codifica le varianti per indice: le nuove vanno aggiunte in coda, e ogni
//! modifica incompatibile richiede di alzare [`PROTOCOL_VERSION`].

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::types::{Coordinate, PositionRecord, UserState, Username};

/// Versione del protocollo, scambiata nell'handshake.
///
/// Portata a 2 con l'aggiunta di [`MovementSummary::route_points`].
pub const PROTOCOL_VERSION: u16 = 2;

/// Testo con cui il server annuncia che la sessione e' stata sostituita da una nuova
/// connessione dello stesso utente; il client lo usa per non riconnettersi.
pub const SESSION_REPLACED_MESSAGE: &str =
    "sessione sostituita da un'altra connessione dello stesso utente";

/// Messaggio inviato dal client al server.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ClientMessage {
    /// Apre la connessione dichiarando la versione di protocollo del client.
    Hello {
        /// Versione parlata dal client, di norma [`PROTOCOL_VERSION`].
        protocol_version: u16,
    },
    /// Crea un nuovo utente e ne apre la sessione.
    Register {
        /// Nome scelto, gia' validato dal tipo.
        username: Username,
        /// Password in chiaro: il server la sottopone ad argon2 prima di persisterla.
        password: String,
    },
    /// Apre la sessione di un utente esistente.
    Login {
        /// Nome dell'utente.
        username: Username,
        /// Password in chiaro, verificata dal server contro l'hash memorizzato.
        password: String,
    },
    /// Comunica una nuova posizione rilevata.
    Position {
        /// Punto rilevato.
        coordinate: Coordinate,
        /// Istante della rilevazione, misurato dal client.
        recorded_at: DateTime<Utc>,
    },
    /// Invia un messaggio testuale al server.
    TextToServer {
        /// Corpo del messaggio.
        body: String,
    },
    /// Chiede il riepilogo dei movimenti su un intervallo.
    RequestAnalysis {
        /// Intervallo temporale da analizzare.
        range: AnalysisRange,
    },
    /// Chiude la sessione.
    Logout,
    /// Verifica che la connessione sia viva; il server risponde [`ServerMessage::Pong`].
    Ping,
}

/// Intervallo temporale su cui calcolare un [`MovementSummary`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AnalysisRange {
    /// Il giorno corrente.
    Today,
    /// La settimana corrente.
    CurrentWeek,
    /// Il mese corrente.
    CurrentMonth,
    /// Un intervallo scelto dall'utente.
    Custom {
        /// Inizio dell'intervallo, incluso.
        from: DateTime<Utc>,
        /// Fine dell'intervallo, inclusa.
        to: DateTime<Utc>,
    },
}

/// Messaggio inviato dal server al client.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ServerMessage {
    /// Accetta l'handshake dichiarando la versione parlata dal server.
    Welcome {
        /// Versione parlata dal server, di norma [`PROTOCOL_VERSION`].
        protocol_version: u16,
    },
    /// Conferma la presa in carico di una richiesta del client.
    Ack {
        /// Richiesta a cui la conferma si riferisce.
        request: AckKind,
    },
    /// Segnala il fallimento di una richiesta.
    Error {
        /// Codice trattabile a programma.
        code: ErrorCode,
        /// Dettaglio leggibile, gia' in italiano.
        message: String,
    },
    /// Conferma l'apertura della sessione dopo un [`ClientMessage::Register`] o
    /// [`ClientMessage::Login`] riuscito.
    SessionToken {
        /// Token opaco che identifica la sessione.
        token: String,
    },
    /// Messaggio testuale diretto a tutta la flotta.
    Broadcast {
        /// Corpo del messaggio.
        body: String,
        /// Istante di invio, misurato dal server.
        sent_at: DateTime<Utc>,
    },
    /// Messaggio testuale diretto al solo destinatario.
    Direct {
        /// Corpo del messaggio.
        body: String,
        /// Istante di invio, misurato dal server.
        sent_at: DateTime<Utc>,
    },
    /// Risposta a [`ClientMessage::RequestAnalysis`].
    AnalysisReport(MovementSummary),
    /// Notifica un cambio di stato di movimento.
    StateChanged {
        /// Nuovo stato dell'utente.
        state: UserState,
    },
    /// Risposta a [`ClientMessage::Ping`].
    Pong,
}

/// Richiesta a cui un [`ServerMessage::Ack`] si riferisce.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum AckKind {
    /// Conferma di [`ClientMessage::Position`].
    Position,
    /// Conferma di [`ClientMessage::TextToServer`].
    TextToServer,
    /// Conferma di [`ClientMessage::Logout`].
    Logout,
}

/// Causa del fallimento riportata in [`ServerMessage::Error`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Error)]
pub enum ErrorCode {
    /// Il client parla una versione di protocollo che il server non accetta.
    #[error("versione di protocollo non supportata")]
    UnsupportedVersion,
    /// Username inesistente o password errata.
    #[error("credenziali non valide")]
    InvalidCredentials,
    /// Lo username richiesto in registrazione appartiene gia' a un altro utente.
    #[error("username gia' in uso")]
    UsernameTaken,
    /// La richiesta esige una sessione aperta.
    #[error("sessione non autenticata")]
    NotAuthenticated,
    /// I byte ricevuti non compongono un messaggio valido.
    #[error("messaggio malformato")]
    MalformedMessage,
    /// Il client ha superato la frequenza di richieste ammessa.
    #[error("troppe richieste in poco tempo")]
    RateLimited,
    /// Guasto interno al server, non imputabile alla richiesta.
    #[error("errore interno del server")]
    Internal,
}

/// Riepilogo dei movimenti di un utente su un [`AnalysisRange`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MovementSummary {
    /// Distanza totale percorsa, in metri.
    pub total_distance_meters: f64,
    /// Tempo trascorso in movimento, in secondi.
    pub moving_duration_secs: u64,
    /// Tempo trascorso fermo, in secondi.
    pub pause_duration_secs: u64,
    /// Velocita' media sul tempo in movimento, in km/h.
    pub average_speed_kmh: f64,
    /// Numero di soste rilevate.
    pub stop_count: u32,
    /// Tragitto in ordine cronologico: intero se breve, altrimenti un campione a
    /// intervalli regolari che comprende sempre il primo e l'ultimo punto.
    pub route: Vec<PositionRecord>,
    /// Numero di posizioni del tragitto completo, prima del campionamento.
    pub route_points: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::fmt;

    use serde::de::DeserializeOwned;

    use crate::types::UserId;

    fn istante(secondi: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(secondi, 0).expect("istante di test valido")
    }

    fn username(raw: &str) -> Username {
        Username::new(raw).expect("username di test valido")
    }

    fn coordinata(lat: f64, lon: f64) -> Coordinate {
        Coordinate::new(lat, lon).expect("coordinata di test valida")
    }

    /// Verifica che il valore attraversi postcard senza perdere informazione.
    fn roundtrip<T>(valore: &T)
    where
        T: Serialize + DeserializeOwned + PartialEq + fmt::Debug,
    {
        let bytes = postcard::to_stdvec(valore).expect("serializzazione riuscita");
        let decodificato: T = postcard::from_bytes(&bytes).expect("deserializzazione riuscita");
        assert_eq!(&decodificato, valore, "roundtrip non fedele");
    }

    fn riepilogo() -> MovementSummary {
        MovementSummary {
            total_distance_meters: 12_345.6,
            moving_duration_secs: 900,
            pause_duration_secs: 300,
            average_speed_kmh: 49.4,
            stop_count: 2,
            route: vec![PositionRecord {
                user_id: UserId(7),
                coordinate: coordinata(45.0703, 7.6869),
                recorded_at: istante(1_700_000_000),
            }],
            route_points: 1,
        }
    }

    #[test]
    fn ogni_client_message_sopravvive_al_roundtrip() {
        let intervalli = [
            AnalysisRange::Today,
            AnalysisRange::CurrentWeek,
            AnalysisRange::CurrentMonth,
            AnalysisRange::Custom {
                from: istante(1_700_000_000),
                to: istante(1_700_086_400),
            },
        ];

        let mut messaggi = vec![
            ClientMessage::Hello {
                protocol_version: PROTOCOL_VERSION,
            },
            ClientMessage::Register {
                username: username("veicolo_01"),
                password: "segreto".to_owned(),
            },
            ClientMessage::Login {
                username: username("veicolo_01"),
                password: "segreto".to_owned(),
            },
            ClientMessage::Position {
                coordinate: coordinata(45.0703, 7.6869),
                recorded_at: istante(1_700_000_000),
            },
            ClientMessage::TextToServer {
                body: "in arrivo al deposito".to_owned(),
            },
            ClientMessage::Logout,
            ClientMessage::Ping,
        ];
        messaggi.extend(
            intervalli
                .into_iter()
                .map(|range| ClientMessage::RequestAnalysis { range }),
        );

        for messaggio in &messaggi {
            roundtrip(messaggio);
        }
    }

    #[test]
    fn ogni_server_message_sopravvive_al_roundtrip() {
        let mut messaggi = vec![
            ServerMessage::Welcome {
                protocol_version: PROTOCOL_VERSION,
            },
            ServerMessage::SessionToken {
                token: "3f9a2c".to_owned(),
            },
            ServerMessage::Broadcast {
                body: "manutenzione alle 18".to_owned(),
                sent_at: istante(1_700_000_000),
            },
            ServerMessage::Direct {
                body: "rientra in sede".to_owned(),
                sent_at: istante(1_700_000_000),
            },
            ServerMessage::AnalysisReport(riepilogo()),
            ServerMessage::Pong,
        ];
        messaggi.extend(
            [AckKind::Position, AckKind::TextToServer, AckKind::Logout]
                .map(|request| ServerMessage::Ack { request }),
        );
        messaggi.extend(
            [
                ErrorCode::UnsupportedVersion,
                ErrorCode::InvalidCredentials,
                ErrorCode::UsernameTaken,
                ErrorCode::NotAuthenticated,
                ErrorCode::MalformedMessage,
                ErrorCode::RateLimited,
                ErrorCode::Internal,
            ]
            .map(|code| ServerMessage::Error {
                code,
                message: code.to_string(),
            }),
        );
        messaggi.extend(
            [UserState::Disconnected, UserState::Still, UserState::Moving]
                .map(|state| ServerMessage::StateChanged { state }),
        );

        for messaggio in &messaggi {
            roundtrip(messaggio);
        }
    }

    #[test]
    fn una_versione_sconosciuta_resta_deserializzabile() {
        let sconosciuta = PROTOCOL_VERSION + 41;

        roundtrip(&ClientMessage::Hello {
            protocol_version: sconosciuta,
        });
        roundtrip(&ServerMessage::Welcome {
            protocol_version: sconosciuta,
        });
    }

    #[test]
    fn username_invalido_nel_messaggio_e_rifiutato_in_deserializzazione() {
        // `Register` e' la variante 1: il test fissa questa assunzione sul formato.
        let ammesso = postcard::to_stdvec(&(1_u32, "veicolo_01", "segreto"))
            .expect("serializzazione riuscita");
        assert_eq!(
            postcard::from_bytes::<ClientMessage>(&ammesso).expect("deserializzazione riuscita"),
            ClientMessage::Register {
                username: username("veicolo_01"),
                password: "segreto".to_owned(),
            }
        );

        let rifiutato =
            postcard::to_stdvec(&(1_u32, "ab", "segreto")).expect("serializzazione riuscita");
        assert!(postcard::from_bytes::<ClientMessage>(&rifiutato).is_err());
    }

    #[test]
    fn il_messaggio_di_sessione_sostituita_e_un_testo_stabile() {
        // Il client riconosce la sostituzione da questo testo esatto.
        assert_eq!(
            SESSION_REPLACED_MESSAGE,
            "sessione sostituita da un'altra connessione dello stesso utente"
        );
    }

    #[test]
    fn error_code_si_mostra_in_italiano() {
        assert_eq!(
            ErrorCode::UnsupportedVersion.to_string(),
            "versione di protocollo non supportata"
        );
        assert_eq!(ErrorCode::UsernameTaken.to_string(), "username gia' in uso");
        assert_eq!(ErrorCode::Internal.to_string(), "errore interno del server");
    }
}
