//! Protocollo applicativo fra client e server Georuggine.
//!
//! # Sequenza attesa
//!
//! ```text
//! Hello -> Welcome -> Register|Login -> SessionToken
//!       -> (Position | TextToServer | RequestAnalysis | Ping)*
//!       -> Logout
//! ```
//!
//! Il client apre sempre con [`ClientMessage::Hello`] dichiarando la propria
//! [`PROTOCOL_VERSION`]; il server risponde con [`ServerMessage::Welcome`] se la accetta,
//! altrimenti con [`ServerMessage::Error`] di codice [`ErrorCode::UnsupportedVersion`].
//! Segue esattamente una fra [`ClientMessage::Register`] e [`ClientMessage::Login`], a cui
//! il server replica con [`ServerMessage::SessionToken`]. Solo a sessione aperta il client
//! puo' inviare messaggi operativi, ripetibili in qualsiasi ordine e numero; ognuno riceve
//! [`ServerMessage::Ack`] o una risposta dedicata. La sessione si chiude con
//! [`ClientMessage::Logout`].
//!
//! Fuori sequenza il server risponde con [`ErrorCode::NotAuthenticated`]. Oltre al flusso
//! guidato dal client, il server puo' inviare in qualsiasi momento
//! [`ServerMessage::Broadcast`], [`ServerMessage::Direct`] e
//! [`ServerMessage::StateChanged`], che non sono risposte a una richiesta.
//!
//! # Compatibilita' del formato
//!
//! postcard non e' auto-descrittivo e codifica le varianti con il loro indice
//! posizionale: **aggiungere varianti in coda e' compatibile, riordinarle o rimuoverle
//! rompe i peer gia' in giro**. Per lo stesso motivo la versione si negozia a livello
//! applicativo con [`PROTOCOL_VERSION`] e non tramite il formato: un valore sconosciuto
//! si deserializza senza errori, e sta al server decidere se proseguire.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::types::{Coordinate, PositionRecord, UserState, Username};

/// Versione del protocollo implementata da questo crate, scambiata nell'handshake.
pub const PROTOCOL_VERSION: u16 = 1;

/// Testo di [`ServerMessage::Error`] con cui il server annuncia che la sessione e' stata
/// sostituita da una nuova connessione dello stesso utente.
///
/// # Perche' un messaggio e non un codice
///
/// La sostituzione di sessione non ha un [`ErrorCode`] proprio: aggiungerne uno vorrebbe
/// dire toccare l'enum, e con postcard una variante nuova puo' solo andare in coda, con
/// quel che comporta per i peer gia' in giro (vedi la nota sulla compatibilita' del
/// formato in cima al modulo). Si usa percio' il codice esistente piu' vicino nel
/// significato, [`ErrorCode::NotAuthenticated`] — dopo la sostituzione questa
/// connessione non ha piu' una sessione — e il testo fa da discriminante.
///
/// La costante sta qui e non nei due binari perche' il discriminante e' parte del
/// contratto fra server e client: il server manda esattamente questo testo prima di
/// chiudere la connessione precedente, e il client lo riconosce per non riconnettersi.
/// Duplicare il letterale nei due crate lo lascerebbe divergere in silenzio.
///
/// Quando il protocollo potra' cambiare versione, questo diventera' un [`ErrorCode`]
/// dedicato e la costante sparira'.
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
    /// Consegna il token di sessione dopo un [`ClientMessage::Register`] o
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
    /// Posizioni che compongono il percorso, in ordine cronologico.
    pub route: Vec<PositionRecord>,
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::fmt;

    use serde::de::DeserializeOwned;

    use crate::types::UserId;

    /// Istante UTC deterministico a partire da un timestamp in secondi.
    fn istante(secondi: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(secondi, 0).expect("istante di test valido")
    }

    /// Username che il test da' per valido.
    fn username(raw: &str) -> Username {
        Username::new(raw).expect("username di test valido")
    }

    /// Coordinata che il test da' per valida.
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

    /// Riepilogo di esempio, con una rotta non vuota.
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
        }
    }

    #[test]
    fn ogni_client_message_sopravvive_al_roundtrip() {
        let messaggi = [
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
            ClientMessage::RequestAnalysis {
                range: AnalysisRange::Today,
            },
            ClientMessage::Logout,
            ClientMessage::Ping,
        ];

        for messaggio in &messaggi {
            roundtrip(messaggio);
        }
    }

    #[test]
    fn ogni_analysis_range_sopravvive_al_roundtrip() {
        let intervalli = [
            AnalysisRange::Today,
            AnalysisRange::CurrentWeek,
            AnalysisRange::CurrentMonth,
            AnalysisRange::Custom {
                from: istante(1_700_000_000),
                to: istante(1_700_086_400),
            },
        ];

        for intervallo in &intervalli {
            roundtrip(intervallo);
        }
    }

    #[test]
    fn ogni_server_message_sopravvive_al_roundtrip() {
        let messaggi = [
            ServerMessage::Welcome {
                protocol_version: PROTOCOL_VERSION,
            },
            ServerMessage::Ack {
                request: AckKind::Position,
            },
            ServerMessage::Error {
                code: ErrorCode::InvalidCredentials,
                message: "credenziali non valide".to_owned(),
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
            ServerMessage::StateChanged {
                state: UserState::Moving,
            },
            ServerMessage::Pong,
        ];

        for messaggio in &messaggi {
            roundtrip(messaggio);
        }
    }

    #[test]
    fn ogni_ack_kind_sopravvive_al_roundtrip() {
        for tipo in &[AckKind::Position, AckKind::TextToServer, AckKind::Logout] {
            roundtrip(tipo);
        }
    }

    #[test]
    fn ogni_error_code_sopravvive_al_roundtrip() {
        for codice in &[
            ErrorCode::UnsupportedVersion,
            ErrorCode::InvalidCredentials,
            ErrorCode::UsernameTaken,
            ErrorCode::NotAuthenticated,
            ErrorCode::MalformedMessage,
            ErrorCode::RateLimited,
            ErrorCode::Internal,
        ] {
            roundtrip(codice);
        }
    }

    #[test]
    fn ogni_user_state_sopravvive_al_roundtrip_nel_messaggio() {
        for stato in &[UserState::Disconnected, UserState::Still, UserState::Moving] {
            roundtrip(&ServerMessage::StateChanged { state: *stato });
        }
    }

    #[test]
    fn hello_con_versione_sconosciuta_resta_deserializzabile() {
        let sconosciuta = PROTOCOL_VERSION + 41;
        let messaggio = ClientMessage::Hello {
            protocol_version: sconosciuta,
        };
        let bytes = postcard::to_stdvec(&messaggio).expect("serializzazione riuscita");

        let decodificato: ClientMessage =
            postcard::from_bytes(&bytes).expect("una versione sconosciuta non rompe il formato");

        assert_eq!(decodificato, messaggio);
    }

    #[test]
    fn welcome_con_versione_sconosciuta_resta_deserializzabile() {
        let messaggio = ServerMessage::Welcome {
            protocol_version: PROTOCOL_VERSION + 41,
        };
        let bytes = postcard::to_stdvec(&messaggio).expect("serializzazione riuscita");

        let decodificato: ServerMessage =
            postcard::from_bytes(&bytes).expect("una versione sconosciuta non rompe il formato");

        assert_eq!(decodificato, messaggio);
    }

    #[test]
    fn username_invalido_nel_messaggio_e_rifiutato_in_deserializzazione() {
        // postcard codifica una variante come indice varint seguito dai campi: `Register`
        // e' la variante 1 di `ClientMessage`. La prima meta' del test fissa questa
        // assunzione sul formato, la seconda verifica il rifiuto.
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
        // Server e client si accordano su questo letterale e non su un codice d'errore:
        // il test lo fissa perche' una modifica su un solo lato non passi inosservata.
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
        assert_eq!(
            ErrorCode::InvalidCredentials.to_string(),
            "credenziali non valide"
        );
        assert_eq!(ErrorCode::UsernameTaken.to_string(), "username gia' in uso");
        assert_eq!(
            ErrorCode::NotAuthenticated.to_string(),
            "sessione non autenticata"
        );
        assert_eq!(
            ErrorCode::MalformedMessage.to_string(),
            "messaggio malformato"
        );
        assert_eq!(
            ErrorCode::RateLimited.to_string(),
            "troppe richieste in poco tempo"
        );
        assert_eq!(ErrorCode::Internal.to_string(), "errore interno del server");
    }
}
