//! Nucleo di connessione del client Georuggine (requisito R3).
//!
//! # Disaccoppiamento da una futura interfaccia
//!
//! Nessuna interfaccia utente (la TUI di P20) deve conoscere il socket: comunica solo
//! attraverso [`ClientCommand`] e [`ClientEvent`], scambiati sui due canali di
//! [`ClientHandle`]. Il task di rete vero e proprio vive dietro [`ClientHandle::spawn`]
//! e non e' altrimenti raggiungibile.
//!
//! # Ciclo di vita di una connessione
//!
//! ```text
//! connetti -> Hello -> Welcome -> Register|Login -> SessionToken -> Connected
//!   |                                                                  |
//!   `------------------------- backoff -------------------------------'
//!                        (Disconnected, poi si riprova)
//! ```
//!
//! Un fallimento di rete (connessione rifiutata, frame corrotto, EOF) fa emettere
//! [`ClientEvent::Disconnected`] e riprovare con un backoff esponenziale (1s, 2s, 4s,
//! 8s, poi 30s fisso, con un piccolo jitter casuale per non far ricollegare piu' client
//! nello stesso istante). Un fallimento di credenziali o di versione di protocollo
//! invece non si ripete da solo: emette [`ClientEvent::Error`] e il task termina,
//! perche' riprovare con la stessa password sbagliata non cambierebbe esito.
//!
//! # Sessione sostituita
//!
//! C'e' un terzo percorso di uscita, distinto da entrambi. Il server ammette una sola
//! sessione per utente: se lo stesso account si collega altrove, chiude questa
//! connessione dopo aver mandato un [`ServerMessage::Error`] con il testo
//! [`SESSION_REPLACED_MESSAGE`]. E' una chiusura voluta, non una caduta di rete, e va
//! trattata diversamente: il task emette [`ClientEvent::Disconnected`] con quel motivo e
//! **termina senza riconnettersi**. Riprovare rimetterebbe fuori la connessione che ha
//! preso il posto di questa, la quale a sua volta si riconnetterebbe, in un ciclo che non
//! finisce piu'; sta all'utente decidere se rilanciare il client. Il percorso della
//! riconnessione per errore di rete resta invece quello di sopra, immutato.
//!
//! # Posizioni mentre la connessione e' caduta
//!
//! Il ticker delle posizioni interroga la sorgente ogni trenta secondi **anche senza
//! connessione attiva**, cosi' il ritmo del percorso non si perde durante un tentativo
//! di riconnessione. Le posizioni prodotte in quella finestra si accumulano in una coda
//! locale limitata a [`CAPACITA_CODA_POSIZIONI`] elementi (le piu' vecchie vengono
//! scartate oltre il limite) e vengono inviate in ordine non appena la connessione
//! torna disponibile, prima di riprendere l'invio periodico.

use std::collections::VecDeque;
use std::net::{AddrParseError, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::mpsc::Sender as SyncSender;
use std::time::Duration;

use chrono::{DateTime, Utc};
use futures_util::{SinkExt as _, StreamExt as _};
use georuggine_common::clock::{Clock, SystemClock};
use georuggine_common::codec::{ClientCodec, ServerCodec};
use georuggine_common::protocol::{
    AnalysisRange, ClientMessage, ErrorCode, MovementSummary, PROTOCOL_VERSION,
    SESSION_REPLACED_MESSAGE, ServerMessage,
};
use georuggine_common::types::{Coordinate, UserState, Username, UsernameError};
use thiserror::Error;
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::{Interval, MissedTickBehavior, timeout};
use tokio_util::codec::{FramedRead, FramedWrite};

use crate::sources::{PositionSource, SorgenteCostruita, SourceKind};

/// Frazione massima di `base` aggiunta come jitter a un ritardo di backoff.
const JITTER_FRAZIONE: f64 = 0.2;

/// Posizioni che la coda di disconnessione puo' trattenere prima di scartare le piu'
/// vecchie.
const CAPACITA_CODA_POSIZIONI: usize = 200;

/// Capacita' del canale dei comandi verso il task di rete.
const CAPACITA_COMANDI: usize = 32;

/// Capacita' del canale degli eventi dal task di rete.
const CAPACITA_EVENTI: usize = 32;

/// Attesa massima concessa al task di rete per chiudersi in modo ordinato.
///
/// E' la stessa scadenza che il server concede alla chiusura di una connessione: pochi
/// secondi bastano a mandare un `Logout` e chiudere il socket, e oltre non si aspetta,
/// perche' un server che non risponde non deve tenere appeso il client in uscita.
const ATTESA_CHIUSURA: Duration = Duration::from_secs(5);

/// Testo mostrato da `--help`.
pub const USO: &str = "\
Uso: georuggine-client [OPZIONI]

Opzioni generali:
  --server <INDIRIZZO>   indirizzo e porta del server (obbligatoria)
  --user <USERNAME>      nome utente (obbligatoria)
  --password <PASSWORD>  password (obbligatoria)
  --register             registra un nuovo utente invece di accedere
  --help                 mostra questo messaggio ed esce

Sorgente di posizione, con --source <file|random|route|manual> (obbligatoria):
  --source file          percorso registrato: richiede --file-path, ammette --file-loop
  --source random        cammino casuale: richiede --start, --radius, --step, --seed
  --source route          rotta a velocita' costante: richiede --start, --end, --speed;
                          ammette --pause <frazione>@<secondi>, ripetibile
  --source manual         posizione decisa a mano: richiede --start

  --file-path <PERCORSO> file CSV del percorso (con --source file)
  --file-loop             ripeti il percorso da capo invece di fermarti (con --source file)
  --start <LAT>,<LON>    punto di partenza (con --source random|route|manual)
  --end <LAT>,<LON>      punto di arrivo (con --source route)
  --radius <METRI>       raggio massimo dalla partenza (con --source random)
  --step <METRI>         spostamento massimo per passo (con --source random)
  --seed <NUMERO>        seme del generatore casuale (con --source random)
  --speed <KM/H>         velocita' di percorrenza (con --source route)
  --pause <FRAZ>@<SEC>   sosta lungo il tragitto, ripetibile (con --source route)
";

/// Errore nell'analisi degli argomenti da riga di comando.
#[derive(Debug, Error)]
pub enum ArgsError {
    /// L'opzione non e' fra quelle riconosciute.
    #[error("opzione sconosciuta: {0}")]
    OpzioneSconosciuta(String),
    /// L'opzione richiede un valore che non e' stato fornito.
    #[error("all'opzione {0} manca il valore")]
    ValoreMancante(&'static str),
    /// Il valore di `--server` non e' un indirizzo con porta.
    #[error("indirizzo del server non valido {indirizzo:?}: {causa}")]
    IndirizzoNonValido {
        /// Testo che non si e' potuto interpretare.
        indirizzo: String,
        /// Motivo riportato dalla libreria standard.
        causa: AddrParseError,
    },
    /// Il valore di `--user` non rispetta il formato di [`Username`].
    #[error("username non valido: {0}")]
    UsernameNonValido(#[from] UsernameError),
    /// Un valore numerico o una coordinata non sono nella forma attesa.
    #[error("valore non valido per {opzione}: {valore:?}: {motivo}")]
    ParametroNonValido {
        /// Opzione a cui il valore appartiene.
        opzione: &'static str,
        /// Testo fornito, non interpretabile.
        valore: String,
        /// Perche' non e' interpretabile.
        motivo: String,
    },
    /// Il valore di `--source` non e' una delle quattro strategie ammesse.
    #[error("sorgente sconosciuta {0:?}: valori ammessi file, random, route, manual")]
    SorgenteSconosciuta(String),
    /// Manca un'opzione richiesta, globale o della sorgente scelta.
    #[error("opzione obbligatoria mancante: {0}")]
    OpzioneObbligatoriaMancante(&'static str),
}

/// Configurazione del client.
#[derive(Debug, Clone, PartialEq)]
pub struct ClientConfig {
    /// Indirizzo e porta del server a cui collegarsi.
    pub server_addr: SocketAddr,
    /// Nome utente con cui autenticarsi.
    pub username: Username,
    /// Password in chiaro, verificata dal server.
    pub password: String,
    /// Se vero apre la sessione con [`ClientMessage::Register`] invece che con
    /// [`ClientMessage::Login`].
    pub register: bool,
    /// Strategia con cui il client simula la propria posizione.
    pub source: SourceKind,
}

/// Esito dell'analisi degli argomenti.
///
/// Come per `ServerConfig::from_args` nel server, `--help` non stampa ne' esce da
/// dentro l'analisi: la decisione torna al chiamante, cosi' la funzione resta pura e
/// testabile.
#[derive(Debug, Clone, PartialEq)]
pub enum Avvio {
    /// Il client deve partire con questa configurazione.
    Configurato(ClientConfig),
    /// E' stato chiesto `--help`: va stampato [`USO`] e si esce senza errore.
    Uso,
}

/// Argomenti specifici di `--source`, raccolti indipendentemente dall'ordine in cui
/// compaiono rispetto a `--source` stesso.
#[derive(Default)]
struct SorgenteGrezza {
    file_path: Option<PathBuf>,
    file_loop: bool,
    start: Option<Coordinate>,
    end: Option<Coordinate>,
    radius_m: Option<f64>,
    step_m: Option<f64>,
    seed: Option<u64>,
    speed_kmh: Option<f64>,
    pause: Vec<crate::sources::Pausa>,
}

impl ClientConfig {
    /// Analizza gli argomenti da riga di comando.
    ///
    /// Si aspetta i soli argomenti, senza il nome del programma: il chiamante passa
    /// tipicamente `std::env::args().skip(1)`.
    ///
    /// # Errori
    ///
    /// [`ArgsError`] se un'opzione e' sconosciuta, se manca il valore di un'opzione che
    /// lo richiede, se `--server` non e' un indirizzo valido, se `--user` non rispetta
    /// il formato di [`Username`], se un parametro numerico o una coordinata sono
    /// malformati, se `--source` non e' una delle quattro strategie ammesse o se manca
    /// un'opzione obbligatoria, globale o della sorgente scelta.
    pub fn from_args<I>(argomenti: I) -> Result<Avvio, ArgsError>
    where
        I: IntoIterator<Item = String>,
    {
        let mut server_addr = None;
        let mut username = None;
        let mut password = None;
        let mut register = false;
        let mut tipo_sorgente: Option<String> = None;
        let mut sorgente = SorgenteGrezza::default();
        let mut voci = argomenti.into_iter();

        while let Some(voce) = voci.next() {
            match voce.as_str() {
                "--help" => return Ok(Avvio::Uso),
                "--server" => {
                    let valore = voci.next().ok_or(ArgsError::ValoreMancante("--server"))?;
                    server_addr =
                        Some(
                            valore
                                .parse()
                                .map_err(|causa| ArgsError::IndirizzoNonValido {
                                    indirizzo: valore,
                                    causa,
                                })?,
                        );
                }
                "--user" => {
                    let valore = voci.next().ok_or(ArgsError::ValoreMancante("--user"))?;
                    username = Some(Username::new(&valore)?);
                }
                "--password" => {
                    password = Some(voci.next().ok_or(ArgsError::ValoreMancante("--password"))?);
                }
                "--register" => register = true,
                "--source" => {
                    tipo_sorgente = Some(voci.next().ok_or(ArgsError::ValoreMancante("--source"))?);
                }
                "--file-path" => {
                    let valore = voci
                        .next()
                        .ok_or(ArgsError::ValoreMancante("--file-path"))?;
                    sorgente.file_path = Some(PathBuf::from(valore));
                }
                "--file-loop" => sorgente.file_loop = true,
                "--start" => {
                    let valore = voci.next().ok_or(ArgsError::ValoreMancante("--start"))?;
                    sorgente.start = Some(analizza_coordinata("--start", &valore)?);
                }
                "--end" => {
                    let valore = voci.next().ok_or(ArgsError::ValoreMancante("--end"))?;
                    sorgente.end = Some(analizza_coordinata("--end", &valore)?);
                }
                "--radius" => {
                    let valore = voci.next().ok_or(ArgsError::ValoreMancante("--radius"))?;
                    sorgente.radius_m = Some(analizza_f64("--radius", &valore)?);
                }
                "--step" => {
                    let valore = voci.next().ok_or(ArgsError::ValoreMancante("--step"))?;
                    sorgente.step_m = Some(analizza_f64("--step", &valore)?);
                }
                "--seed" => {
                    let valore = voci.next().ok_or(ArgsError::ValoreMancante("--seed"))?;
                    sorgente.seed = Some(analizza_u64("--seed", &valore)?);
                }
                "--speed" => {
                    let valore = voci.next().ok_or(ArgsError::ValoreMancante("--speed"))?;
                    sorgente.speed_kmh = Some(analizza_f64("--speed", &valore)?);
                }
                "--pause" => {
                    let valore = voci.next().ok_or(ArgsError::ValoreMancante("--pause"))?;
                    sorgente.pause.push(analizza_pausa(&valore)?);
                }
                altro => return Err(ArgsError::OpzioneSconosciuta(altro.to_owned())),
            }
        }

        let source = costruisci_source_kind(tipo_sorgente.as_deref(), sorgente)?;

        Ok(Avvio::Configurato(ClientConfig {
            server_addr: server_addr.ok_or(ArgsError::OpzioneObbligatoriaMancante("--server"))?,
            username: username.ok_or(ArgsError::OpzioneObbligatoriaMancante("--user"))?,
            password: password.ok_or(ArgsError::OpzioneObbligatoriaMancante("--password"))?,
            register,
            source,
        }))
    }
}

/// Interpreta `<lat>,<lon>` come una [`Coordinate`].
fn analizza_coordinata(opzione: &'static str, valore: &str) -> Result<Coordinate, ArgsError> {
    let errore = |motivo: String| ArgsError::ParametroNonValido {
        opzione,
        valore: valore.to_owned(),
        motivo,
    };

    let (lat, lon) = valore
        .split_once(',')
        .ok_or_else(|| errore("attesa la forma <lat>,<lon>".to_owned()))?;
    let lat: f64 = lat
        .trim()
        .parse()
        .map_err(|_| errore("latitudine non numerica".to_owned()))?;
    let lon: f64 = lon
        .trim()
        .parse()
        .map_err(|_| errore("longitudine non numerica".to_owned()))?;

    Coordinate::new(lat, lon).map_err(|causa| errore(causa.to_string()))
}

/// Interpreta un valore come `f64`.
fn analizza_f64(opzione: &'static str, valore: &str) -> Result<f64, ArgsError> {
    valore
        .trim()
        .parse()
        .map_err(|_| ArgsError::ParametroNonValido {
            opzione,
            valore: valore.to_owned(),
            motivo: "atteso un numero".to_owned(),
        })
}

/// Interpreta un valore come `u64`.
fn analizza_u64(opzione: &'static str, valore: &str) -> Result<u64, ArgsError> {
    valore
        .trim()
        .parse()
        .map_err(|_| ArgsError::ParametroNonValido {
            opzione,
            valore: valore.to_owned(),
            motivo: "atteso un intero non negativo".to_owned(),
        })
}

/// Interpreta `<frazione>@<secondi>` come una [`crate::sources::Pausa`].
fn analizza_pausa(valore: &str) -> Result<crate::sources::Pausa, ArgsError> {
    let errore = |motivo: &str| ArgsError::ParametroNonValido {
        opzione: "--pause",
        valore: valore.to_owned(),
        motivo: motivo.to_owned(),
    };

    let (frazione, secondi) = valore
        .split_once('@')
        .ok_or_else(|| errore("attesa la forma <frazione>@<secondi>"))?;
    let a_frazione: f64 = frazione
        .trim()
        .parse()
        .map_err(|_| errore("frazione non numerica"))?;
    let secondi: u64 = secondi
        .trim()
        .parse()
        .map_err(|_| errore("durata non numerica"))?;

    Ok(crate::sources::Pausa {
        a_frazione,
        durata: Duration::from_secs(secondi),
    })
}

/// Costruisce la [`SourceKind`] descritta da `tipo` e dagli argomenti raccolti.
fn costruisci_source_kind(
    tipo: Option<&str>,
    args: SorgenteGrezza,
) -> Result<SourceKind, ArgsError> {
    match tipo.ok_or(ArgsError::OpzioneObbligatoriaMancante("--source"))? {
        "file" => Ok(SourceKind::File {
            percorso: args
                .file_path
                .ok_or(ArgsError::OpzioneObbligatoriaMancante("--file-path"))?,
            loop_forever: args.file_loop,
        }),
        "random" => Ok(SourceKind::RandomWalk {
            partenza: args
                .start
                .ok_or(ArgsError::OpzioneObbligatoriaMancante("--start"))?,
            raggio_massimo_m: args
                .radius_m
                .ok_or(ArgsError::OpzioneObbligatoriaMancante("--radius"))?,
            passo_massimo_m: args
                .step_m
                .ok_or(ArgsError::OpzioneObbligatoriaMancante("--step"))?,
            seed: args
                .seed
                .ok_or(ArgsError::OpzioneObbligatoriaMancante("--seed"))?,
        }),
        "route" => Ok(SourceKind::Route {
            partenza: args
                .start
                .ok_or(ArgsError::OpzioneObbligatoriaMancante("--start"))?,
            arrivo: args
                .end
                .ok_or(ArgsError::OpzioneObbligatoriaMancante("--end"))?,
            velocita_kmh: args
                .speed_kmh
                .ok_or(ArgsError::OpzioneObbligatoriaMancante("--speed"))?,
            pause: args.pause,
        }),
        "manual" => Ok(SourceKind::Manual {
            iniziale: args
                .start
                .ok_or(ArgsError::OpzioneObbligatoriaMancante("--start"))?,
        }),
        altro => Err(ArgsError::SorgenteSconosciuta(altro.to_owned())),
    }
}

/// Comando che una futura interfaccia manda al task di rete.
///
/// Il task lo traduce in un messaggio di protocollo, salvo [`ClientCommand::SetManualPosition`]:
/// quello alimenta direttamente la sorgente manuale (se la configurazione ne usa una) e
/// non produce di per se' alcun messaggio verso il server, che arriva invece al
/// prossimo giro del ticker delle posizioni.
#[derive(Debug, Clone, PartialEq)]
pub enum ClientCommand {
    /// Manda un messaggio testuale al server.
    SendText(String),
    /// Imposta la posizione corrente su una sorgente manuale.
    SetManualPosition(Coordinate),
    /// Chiede il riepilogo dei movimenti su un intervallo.
    RequestAnalysis(AnalysisRange),
    /// Chiude la sessione e termina il task di rete.
    Quit,
}

/// Evento che il task di rete manda a una futura interfaccia.
#[derive(Debug, Clone, PartialEq)]
pub enum ClientEvent {
    /// La sessione e' aperta: handshake, autenticazione e messaggi in attesa consegnati.
    Connected,
    /// La connessione e' caduta; il campo descrive il motivo. Un tentativo di
    /// riconnessione e' gia' in corso, salvo che il task stia per terminare: e' il caso
    /// della sessione sostituita, dove il motivo e' [`SESSION_REPLACED_MESSAGE`] e non
    /// segue nessun tentativo. Vedi la nota nel modulo.
    Disconnected(String),
    /// Testo ricevuto dal server: `direct` distingue un messaggio diretto da un
    /// broadcast.
    ServerText {
        /// Corpo del messaggio.
        body: String,
        /// Vero se il messaggio era diretto a questo utente, falso se era un broadcast.
        direct: bool,
    },
    /// Il server ha notificato un cambio di stato di movimento.
    StateChanged(UserState),
    /// Una posizione e' stata inviata con successo al server.
    PositionSent(Coordinate),
    /// Il server ha risposto a [`ClientCommand::RequestAnalysis`].
    AnalysisReceived(MovementSummary),
    /// Un errore non fatale: una richiesta rifiutata dal server, o l'impossibilita' di
    /// aprire la sessione con le credenziali configurate.
    Error(String),
}

/// Parametri temporali del task di rete: intervallo di invio periodico e progressione
/// del backoff di riconnessione.
///
/// Sono un parametro interno e non costanti fisse per lo stesso motivo di
/// `ConnectionTimeouts` nel server: i test devono poter verificare backoff e invii
/// periodici senza aspettare i valori di produzione.
struct Tempi {
    /// Intervallo fra due interrogazioni della sorgente di posizione.
    intervallo_posizione: Duration,
    /// Ritardo dei primi tentativi di riconnessione, in ordine.
    backoff: [Duration; 4],
    /// Ritardo a cui il backoff resta una volta esaurita la progressione.
    backoff_massimo: Duration,
}

impl Default for Tempi {
    /// Trenta secondi fra un invio e il successivo; backoff 1s, 2s, 4s, 8s, poi 30s
    /// fisso, come da requisito.
    fn default() -> Self {
        Self {
            intervallo_posizione: Duration::from_secs(30),
            backoff: [
                Duration::from_secs(1),
                Duration::from_secs(2),
                Duration::from_secs(4),
                Duration::from_secs(8),
            ],
            backoff_massimo: Duration::from_secs(30),
        }
    }
}

impl Tempi {
    /// Ritardo del tentativo numero `tentativo` (il primo e' 1), con un jitter casuale
    /// fino al 20% in piu' del valore base: senza, client diversi riavviati insieme
    /// proverebbero a riconnettersi tutti nello stesso istante.
    fn ritardo_di(&self, tentativo: u32) -> Duration {
        let indice = usize::try_from(tentativo.saturating_sub(1)).unwrap_or(usize::MAX);
        let base = self
            .backoff
            .get(indice)
            .copied()
            .unwrap_or(self.backoff_massimo);
        let jitter = rand::random::<f64>() * JITTER_FRAZIONE * base.as_secs_f64();
        base + Duration::from_secs_f64(jitter)
    }
}

/// Stato che il task di rete porta attraverso le fasi connessa e disconnessa: la
/// sorgente di posizione, il canale della sorgente manuale se c'e', la coda delle
/// posizioni non ancora inviate, il ticker periodico e l'orologio.
///
/// E' una struttura di sole referenze e non un insieme di parametri separati perche'
/// altrimenti le funzioni che la usano supererebbero il limite di argomenti di clippy.
struct StatoRete<'a> {
    sorgente: &'a mut dyn PositionSource,
    manuale: Option<&'a SyncSender<Coordinate>>,
    coda: &'a mut VecDeque<(Coordinate, DateTime<Utc>)>,
    ticker: &'a mut Interval,
    clock: &'a Arc<dyn Clock>,
}

/// Motivo per cui l'apertura della sessione non e' riuscita.
enum MotivoFallimento {
    /// Un guasto di rete o un protocollo violato: vale la pena riprovare.
    Rete(String),
    /// Credenziali rifiutate o versione di protocollo incompatibile: riprovare con la
    /// stessa configurazione darebbe di nuovo lo stesso esito, quindi il task termina
    /// invece di ripetere il login all'infinito con la stessa password sbagliata.
    Terminale(String),
}

/// Esito della fase connessa del task di rete.
enum EsitoFase {
    /// La connessione e' caduta: il chiamante riprova con il backoff.
    Riconnetti(String),
    /// Il server ha chiuso la sessione apposta, perche' lo stesso utente si e' collegato
    /// da un'altra parte: il chiamante emette [`ClientEvent::Disconnected`] con il motivo
    /// e il task termina **senza** riprovare.
    ///
    /// E' una variante a se' e non un [`EsitoFase::Riconnetti`] proprio perche' la
    /// differenza sta tutta nel seguito: riconnettersi qui espellerebbe la connessione
    /// che ha preso il posto di questa, la quale a sua volta si riconnetterebbe,
    /// generando un ciclo perpetuo fra i due client. Non e' nemmeno un
    /// [`EsitoFase::Termina`], che e' l'uscita silenziosa di [`ClientCommand::Quit`]:
    /// qui l'utente va avvisato di cosa e' successo.
    SessioneSostituita(String),
    /// [`ClientCommand::Quit`] o la caduta del canale dei comandi: il task termina.
    Termina,
}

/// Esito dell'attesa fra un tentativo di riconnessione e il successivo.
enum EsitoAttesa {
    /// Il ritardo e' scaduto: si puo' tentare di nuovo la connessione.
    Riprova,
    /// [`ClientCommand::Quit`] o la caduta del canale dei comandi: il task termina.
    Termina,
}

/// Nucleo di rete: avvia un task interno e lo disaccoppia dal resto del programma
/// attraverso due canali.
///
/// Nessuna futura interfaccia deve conoscere il socket: comunica solo mandando
/// [`ClientCommand`] su `command_tx` e leggendo [`ClientEvent`] da `event_rx`. Il
/// canale degli eventi si chiude (restituendo `None`) quando il task termina, per
/// esempio dopo [`ClientCommand::Quit`] o dopo un fallimento non ripetibile
/// dell'autenticazione.
///
/// Chi lo avvia deve chiuderlo con [`ClientHandle::chiudi`]: lasciar cadere l'handle non
/// basta, per il motivo spiegato li'.
pub struct ClientHandle {
    /// Comandi da mandare al task di rete.
    pub command_tx: mpsc::Sender<ClientCommand>,
    /// Eventi che il task di rete produce.
    pub event_rx: mpsc::Receiver<ClientEvent>,
    /// Task di rete, atteso da [`ClientHandle::chiudi`].
    ///
    /// Non e' pubblico: chi usa l'handle non deve poter attendere o interrompere il task
    /// se non passando dalla chiusura ordinata.
    task: JoinHandle<()>,
}

impl ClientHandle {
    /// Avvia il task di rete con l'orologio e i tempi di produzione.
    ///
    /// `sorgente` arriva gia' costruita da [`SourceKind::build`] e non viene costruita
    /// qui. Una sorgente che non si inizializza (il file di un percorso mancante o
    /// illeggibile) e' un errore di configurazione: va scoperto dal chiamante **prima**
    /// di aprire qualunque connessione, non dentro un task gia' avviato, dove
    /// diventerebbe un evento di errore su un canale che l'interfaccia non ha ancora
    /// avuto il tempo di leggere. Cosi' la lettura del file resta inoltre fuori da un
    /// task async, dove sarebbe un'operazione bloccante.
    pub fn spawn(config: ClientConfig, sorgente: SorgenteCostruita) -> Self {
        Self::spawn_con(config, sorgente, Arc::new(SystemClock), Tempi::default())
    }

    /// Avvia il task di rete con orologio e tempi scelti dal chiamante.
    ///
    /// Non e' pubblica: serve ai test per accorciare l'intervallo di invio e il
    /// backoff senza aspettare i valori di produzione, come `ConnectionTimeouts` nel
    /// server.
    fn spawn_con(
        config: ClientConfig,
        sorgente: SorgenteCostruita,
        clock: Arc<dyn Clock>,
        tempi: Tempi,
    ) -> Self {
        let (command_tx, command_rx) = mpsc::channel(CAPACITA_COMANDI);
        let (event_tx, event_rx) = mpsc::channel(CAPACITA_EVENTI);
        let task = tokio::spawn(esegui(config, sorgente, clock, tempi, command_rx, event_tx));
        Self {
            command_tx,
            event_rx,
            task,
        }
    }

    /// Chiude la sessione e attende che il task di rete sia davvero terminato.
    ///
    /// Restituisce vero se il task si e' chiuso entro [`ATTESA_CHIUSURA`], falso se la
    /// scadenza e' passata per prima.
    ///
    /// Lasciar cadere l'handle non basta: al ritorno di `main` il runtime viene distrutto
    /// e i task ancora vivi spariscono, quindi il `Logout` e la chiusura del socket
    /// andrebbero persi in una corsa e il server libererebbe la sessione solo alla
    /// scadenza di inattivita'. L'ordine delle operazioni conta:
    ///
    /// 1. cade il ricevitore degli eventi, altrimenti un task fermo su un canale degli
    ///    eventi pieno — nessuno lo drena piu' — non arriverebbe mai a leggere il comando
    ///    di chiusura;
    /// 2. parte [`ClientCommand::Quit`], che fa inviare `Logout` e chiudere il lato di
    ///    scrittura;
    /// 3. cade anche il mittente dei comandi, cosi' un task che fosse gia' oltre la
    ///    lettura del comando termina comunque sul canale chiuso;
    /// 4. si attende la fine del task, con un tetto complessivo sulle tre attese.
    pub async fn chiudi(self) -> bool {
        let Self {
            command_tx,
            event_rx,
            task,
        } = self;
        drop(event_rx);

        timeout(ATTESA_CHIUSURA, async move {
            let _ = command_tx.send(ClientCommand::Quit).await;
            drop(command_tx);
            // L'errore di join significa che il task e' andato in panico: non c'e'
            // nulla da fare oltre a smettere di aspettarlo, e il chiamante sta gia'
            // uscendo.
            let _ = task.await;
        })
        .await
        .is_ok()
    }
}

/// Corpo del task di rete: alterna connessione e attesa di riconnessione finche' non
/// arriva [`ClientCommand::Quit`] o un fallimento non ripetibile dell'autenticazione.
///
/// La sorgente arriva gia' costruita dal chiamante: vedi [`ClientHandle::spawn`].
async fn esegui(
    config: ClientConfig,
    costruita: SorgenteCostruita,
    clock: Arc<dyn Clock>,
    tempi: Tempi,
    mut command_rx: mpsc::Receiver<ClientCommand>,
    event_tx: mpsc::Sender<ClientEvent>,
) {
    let mut sorgente = costruita.sorgente;
    let manual_tx = costruita.comandi;
    let mut coda: VecDeque<(Coordinate, DateTime<Utc>)> = VecDeque::new();

    let mut ticker = tokio::time::interval(tempi.intervallo_posizione);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    // Il primo tick di un `interval` matura subito: lo si consuma qui, altrimenti la
    // prima posizione partirebbe non appena la connessione si apre invece che trenta
    // secondi dopo l'avvio del task.
    ticker.tick().await;

    let mut tentativo: u32 = 0;

    loop {
        match connetti_e_autentica(&config).await {
            Ok((mut ingresso, mut uscita)) => {
                tentativo = 0;
                let _ = event_tx.send(ClientEvent::Connected).await;

                let mut stato = StatoRete {
                    sorgente: &mut *sorgente,
                    manuale: manual_tx.as_ref(),
                    coda: &mut coda,
                    ticker: &mut ticker,
                    clock: &clock,
                };

                match fase_connessa(
                    &mut ingresso,
                    &mut uscita,
                    &mut stato,
                    &mut command_rx,
                    &event_tx,
                )
                .await
                {
                    EsitoFase::Termina => return,
                    // Chiusura voluta dal server: si avvisa e si esce dal ciclo, senza
                    // passare dal backoff. Rilanciare il client tocca all'utente.
                    EsitoFase::SessioneSostituita(motivo) => {
                        let _ = event_tx.send(ClientEvent::Disconnected(motivo)).await;
                        return;
                    }
                    EsitoFase::Riconnetti(motivo) => {
                        let _ = event_tx.send(ClientEvent::Disconnected(motivo)).await;
                    }
                }
            }
            Err(MotivoFallimento::Terminale(messaggio)) => {
                let _ = event_tx.send(ClientEvent::Error(messaggio)).await;
                return;
            }
            Err(MotivoFallimento::Rete(messaggio)) => {
                let _ = event_tx.send(ClientEvent::Disconnected(messaggio)).await;
            }
        }

        tentativo += 1;
        let mut stato = StatoRete {
            sorgente: &mut *sorgente,
            manuale: manual_tx.as_ref(),
            coda: &mut coda,
            ticker: &mut ticker,
            clock: &clock,
        };
        let esito = attesa_riconnessione(
            tempi.ritardo_di(tentativo),
            &mut stato,
            &mut command_rx,
            &event_tx,
        )
        .await;
        if matches!(esito, EsitoAttesa::Termina) {
            return;
        }
    }
}

/// Apre la connessione ed esegue l'handshake e l'autenticazione.
///
/// # Errori
///
/// [`MotivoFallimento::Rete`] per ogni guasto di rete o violazione del protocollo, da
/// cui vale la pena riprovare; [`MotivoFallimento::Terminale`] per credenziali
/// rifiutate o versione di protocollo incompatibile, che riprovare non risolverebbe.
async fn connetti_e_autentica(
    config: &ClientConfig,
) -> Result<
    (
        FramedRead<OwnedReadHalf, ServerCodec>,
        FramedWrite<OwnedWriteHalf, ClientCodec>,
    ),
    MotivoFallimento,
> {
    let stream = TcpStream::connect(config.server_addr)
        .await
        .map_err(|errore| MotivoFallimento::Rete(errore.to_string()))?;
    let (lettura, scrittura) = stream.into_split();
    let mut ingresso = FramedRead::new(lettura, ServerCodec::new());
    let mut uscita = FramedWrite::new(scrittura, ClientCodec::new());

    uscita
        .send(ClientMessage::Hello {
            protocol_version: PROTOCOL_VERSION,
        })
        .await
        .map_err(|errore| MotivoFallimento::Rete(errore.to_string()))?;

    match prossimo(&mut ingresso).await? {
        ServerMessage::Welcome { .. } => {}
        ServerMessage::Error { code, message } => return Err(esito_errore(code, message)),
        altro => {
            return Err(MotivoFallimento::Rete(format!(
                "risposta inattesa all'handshake: {altro:?}"
            )));
        }
    }

    let richiesta = if config.register {
        ClientMessage::Register {
            username: config.username.clone(),
            password: config.password.clone(),
        }
    } else {
        ClientMessage::Login {
            username: config.username.clone(),
            password: config.password.clone(),
        }
    };
    uscita
        .send(richiesta)
        .await
        .map_err(|errore| MotivoFallimento::Rete(errore.to_string()))?;

    match prossimo(&mut ingresso).await? {
        ServerMessage::SessionToken { .. } => Ok((ingresso, uscita)),
        ServerMessage::Error { code, message } => Err(esito_errore(code, message)),
        altro => Err(MotivoFallimento::Rete(format!(
            "risposta inattesa all'autenticazione: {altro:?}"
        ))),
    }
}

/// Legge il prossimo messaggio, traducendo EOF ed errori di frame in
/// [`MotivoFallimento::Rete`].
async fn prossimo(
    ingresso: &mut FramedRead<OwnedReadHalf, ServerCodec>,
) -> Result<ServerMessage, MotivoFallimento> {
    match ingresso.next().await {
        Some(Ok(messaggio)) => Ok(messaggio),
        Some(Err(errore)) => Err(MotivoFallimento::Rete(errore.to_string())),
        None => Err(MotivoFallimento::Rete(
            "connessione chiusa dal server".to_owned(),
        )),
    }
}

/// Traduce un [`ServerMessage::Error`] ricevuto durante l'handshake o l'autenticazione
/// in un [`MotivoFallimento`].
fn esito_errore(code: ErrorCode, message: String) -> MotivoFallimento {
    match code {
        ErrorCode::InvalidCredentials
        | ErrorCode::UsernameTaken
        | ErrorCode::UnsupportedVersion => MotivoFallimento::Terminale(message),
        _ => MotivoFallimento::Rete(message),
    }
}

/// Ciclo principale a connessione aperta: invii periodici, comandi in arrivo e
/// messaggi dal server, finche' la connessione non cade o arriva
/// [`ClientCommand::Quit`].
async fn fase_connessa(
    ingresso: &mut FramedRead<OwnedReadHalf, ServerCodec>,
    uscita: &mut FramedWrite<OwnedWriteHalf, ClientCodec>,
    stato: &mut StatoRete<'_>,
    command_rx: &mut mpsc::Receiver<ClientCommand>,
    event_tx: &mpsc::Sender<ClientEvent>,
) -> EsitoFase {
    // Le posizioni accumulate durante una disconnessione precedente partono subito,
    // prima di riprendere l'invio periodico.
    if let Err(errore) = svuota_coda(stato.coda, uscita, event_tx).await {
        return EsitoFase::Riconnetti(errore);
    }

    loop {
        tokio::select! {
            _ = stato.ticker.tick() => {
                let ora = stato.clock.now();
                match stato.sorgente.next_position(ora) {
                    Some(coordinate) => accoda(stato.coda, coordinate, ora),
                    // Sorgente esaurita: un percorso da file arrivato in fondo senza
                    // ripetizione non produrra' piu' nulla, e una connessione muta viene
                    // chiusa dal server alla scadenza di inattivita'. Senza questo ramo
                    // il client entrerebbe in un ciclo perpetuo di riconnessioni che non
                    // porta nessuna posizione nuova; un `Ping` al ritmo del ticker, ben
                    // dentro quella scadenza, tiene invece viva la sessione e lascia il
                    // veicolo raggiungibile dai messaggi del server.
                    None => {
                        if uscita.send(ClientMessage::Ping).await.is_err() {
                            return EsitoFase::Riconnetti("scrittura fallita".to_owned());
                        }
                    }
                }
                if let Err(errore) = svuota_coda(stato.coda, uscita, event_tx).await {
                    return EsitoFase::Riconnetti(errore);
                }
            }
            comando = command_rx.recv() => {
                match comando {
                    Some(ClientCommand::SendText(body)) => {
                        if uscita.send(ClientMessage::TextToServer { body }).await.is_err() {
                            return EsitoFase::Riconnetti("scrittura fallita".to_owned());
                        }
                    }
                    Some(ClientCommand::RequestAnalysis(range)) => {
                        if uscita.send(ClientMessage::RequestAnalysis { range }).await.is_err() {
                            return EsitoFase::Riconnetti("scrittura fallita".to_owned());
                        }
                    }
                    Some(ClientCommand::SetManualPosition(coordinate)) => {
                        match stato.manuale {
                            Some(tx) => {
                                let _ = tx.send(coordinate);
                            }
                            None => {
                                let _ = event_tx
                                    .send(ClientEvent::Error(
                                        "la sorgente attiva non accetta posizioni manuali"
                                            .to_owned(),
                                    ))
                                    .await;
                            }
                        }
                    }
                    Some(ClientCommand::Quit) => {
                        let _ = uscita.send(ClientMessage::Logout).await;
                        let _ = uscita.close().await;
                        return EsitoFase::Termina;
                    }
                    None => return EsitoFase::Termina,
                }
            }
            messaggio = ingresso.next() => {
                match messaggio {
                    Some(Ok(ServerMessage::Broadcast { body, .. })) => {
                        let _ = event_tx
                            .send(ClientEvent::ServerText { body, direct: false })
                            .await;
                    }
                    Some(Ok(ServerMessage::Direct { body, .. })) => {
                        let _ = event_tx
                            .send(ClientEvent::ServerText { body, direct: true })
                            .await;
                    }
                    Some(Ok(ServerMessage::StateChanged { state })) => {
                        let _ = event_tx.send(ClientEvent::StateChanged(state)).await;
                    }
                    Some(Ok(ServerMessage::AnalysisReport(riepilogo))) => {
                        let _ = event_tx.send(ClientEvent::AnalysisReceived(riepilogo)).await;
                    }
                    // La sostituzione di sessione si riconosce dal testo e non dal
                    // codice: il protocollo non ha un `ErrorCode` dedicato, vedi
                    // `SESSION_REPLACED_MESSAGE`. Il ramo precede quello generale
                    // apposta, perche' altrimenti finirebbe fra gli errori qualunque.
                    Some(Ok(ServerMessage::Error { message, .. }))
                        if message == SESSION_REPLACED_MESSAGE =>
                    {
                        return EsitoFase::SessioneSostituita(message);
                    }
                    Some(Ok(ServerMessage::Error { message, .. })) => {
                        let _ = event_tx.send(ClientEvent::Error(message)).await;
                    }
                    // Welcome, SessionToken, Ack e Pong fuori sequenza: non c'e' nulla da fare.
                    Some(Ok(_)) => {}
                    Some(Err(errore)) => return EsitoFase::Riconnetti(errore.to_string()),
                    None => {
                        return EsitoFase::Riconnetti("connessione chiusa dal server".to_owned());
                    }
                }
            }
        }
    }
}

/// Attende il ritardo di backoff, continuando a interrogare la sorgente e a servire i
/// comandi che non richiedono una connessione.
///
/// `SendText` e `RequestAnalysis` non vengono accodati: senza connessione non c'e' un
/// server a cui recapitarli, e la coda locale e' riservata alle sole posizioni, come
/// da requisito.
async fn attesa_riconnessione(
    ritardo: Duration,
    stato: &mut StatoRete<'_>,
    command_rx: &mut mpsc::Receiver<ClientCommand>,
    event_tx: &mpsc::Sender<ClientEvent>,
) -> EsitoAttesa {
    let scadenza = tokio::time::sleep(ritardo);
    tokio::pin!(scadenza);

    loop {
        tokio::select! {
            _ = &mut scadenza => return EsitoAttesa::Riprova,
            _ = stato.ticker.tick() => {
                let ora = stato.clock.now();
                if let Some(coordinate) = stato.sorgente.next_position(ora) {
                    accoda(stato.coda, coordinate, ora);
                }
            }
            comando = command_rx.recv() => {
                match comando {
                    Some(ClientCommand::Quit) | None => return EsitoAttesa::Termina,
                    Some(ClientCommand::SetManualPosition(coordinate)) => {
                        if let Some(tx) = stato.manuale {
                            let _ = tx.send(coordinate);
                        }
                    }
                    Some(ClientCommand::SendText(_)) | Some(ClientCommand::RequestAnalysis(_)) => {
                        let _ = event_tx
                            .send(ClientEvent::Error(
                                "non connesso: comando scartato".to_owned(),
                            ))
                            .await;
                    }
                }
            }
        }
    }
}

/// Accoda una posizione, scartando la piu' vecchia se la coda e' gia' al limite.
fn accoda(
    coda: &mut VecDeque<(Coordinate, DateTime<Utc>)>,
    coordinate: Coordinate,
    quando: DateTime<Utc>,
) {
    if coda.len() >= CAPACITA_CODA_POSIZIONI {
        coda.pop_front();
    }
    coda.push_back((coordinate, quando));
}

/// Invia in ordine tutte le posizioni in coda, emettendo [`ClientEvent::PositionSent`]
/// per ognuna.
///
/// # Errori
///
/// La descrizione dell'errore di scrittura, se la connessione cade a meta': la
/// posizione che non e' partita resta in testa alla coda per il prossimo tentativo.
async fn svuota_coda(
    coda: &mut VecDeque<(Coordinate, DateTime<Utc>)>,
    uscita: &mut FramedWrite<OwnedWriteHalf, ClientCodec>,
    event_tx: &mpsc::Sender<ClientEvent>,
) -> Result<(), String> {
    while let Some((coordinate, recorded_at)) = coda.pop_front() {
        if let Err(errore) = uscita
            .send(ClientMessage::Position {
                coordinate,
                recorded_at,
            })
            .await
        {
            coda.push_front((coordinate, recorded_at));
            return Err(errore.to_string());
        }
        let _ = event_tx.send(ClientEvent::PositionSent(coordinate)).await;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::net::SocketAddr;

    use georuggine_common::protocol::{ClientMessage, PROTOCOL_VERSION, ServerMessage};
    use tokio::net::{TcpListener, TcpStream};
    use tokio::time::timeout;

    /// Quanto un test aspetta un evento o un messaggio prima di considerarlo mancato.
    const ATTESA: Duration = Duration::from_secs(5);

    /// Tempi accorciati per i test che riguardano backoff e invio periodico: reali,
    /// ma troppo brevi perche' un test debba aspettare i valori di produzione.
    fn tempi_di_prova() -> Tempi {
        Tempi {
            intervallo_posizione: Duration::from_millis(80),
            backoff: [Duration::from_millis(20); 4],
            backoff_massimo: Duration::from_millis(20),
        }
    }

    /// Configurazione di test verso `indirizzo`, con una sorgente manuale che non
    /// produce mai posizioni nuove da sola.
    fn config(indirizzo: SocketAddr) -> ClientConfig {
        ClientConfig {
            server_addr: indirizzo,
            username: Username::new("veicolo_01").expect("username di test valido"),
            password: "scorta12".to_owned(),
            register: false,
            source: SourceKind::Manual {
                iniziale: Coordinate::new(45.0703, 7.6869).expect("coordinata di test valida"),
            },
        }
    }

    /// Configurazione di test con un cammino casuale, per i test sull'invio periodico:
    /// produce posizioni diverse a ogni chiamata.
    fn config_con_random(indirizzo: SocketAddr) -> ClientConfig {
        ClientConfig {
            source: SourceKind::RandomWalk {
                partenza: Coordinate::new(45.0703, 7.6869).expect("coordinata di test valida"),
                raggio_massimo_m: 1_000.0,
                passo_massimo_m: 50.0,
                seed: 7,
            },
            ..config(indirizzo)
        }
    }

    /// Avvia il task di rete costruendo la sorgente dalla configurazione, come fa il
    /// binario prima di chiamare [`ClientHandle::spawn`].
    fn avvia(config: ClientConfig) -> ClientHandle {
        let sorgente = config.source.build().expect("sorgente di test costruibile");
        ClientHandle::spawn(config, sorgente)
    }

    /// Come [`avvia`], con orologio e tempi scelti dal test.
    fn avvia_con(config: ClientConfig, clock: Arc<dyn Clock>, tempi: Tempi) -> ClientHandle {
        let sorgente = config.source.build().expect("sorgente di test costruibile");
        ClientHandle::spawn_con(config, sorgente, clock, tempi)
    }

    /// File CSV di prova, cancellato quando il test finisce.
    ///
    /// Serve al solo test sulla sorgente esaurita: e' l'unica strategia che, finito il
    /// percorso, smette davvero di produrre posizioni.
    struct FileDiProva {
        /// Percorso del file creato.
        percorso: PathBuf,
    }

    impl FileDiProva {
        /// Scrive `contenuto` in un file temporaneo unico per esecuzione.
        fn nuovo(nome: &str, contenuto: &str) -> Self {
            let unico = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|scarto| scarto.as_nanos())
                .unwrap_or(0);
            let percorso = std::env::temp_dir().join(format!(
                "georuggine-core-{nome}-{}-{unico}.csv",
                std::process::id()
            ));
            std::fs::write(&percorso, contenuto).expect("scrittura del file di prova riuscita");
            Self { percorso }
        }
    }

    impl Drop for FileDiProva {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.percorso);
        }
    }

    /// Prossimo evento, o un fallimento del test se non arriva entro [`ATTESA`].
    async fn prossimo_evento(handle: &mut ClientHandle) -> ClientEvent {
        timeout(ATTESA, handle.event_rx.recv())
            .await
            .expect("il task di rete deve produrre un evento entro l'attesa")
            .expect("il canale degli eventi non deve chiudersi qui")
    }

    /// Server finto in ascolto su una porta effimera: parla il protocollo vero con i
    /// codec di `common`, dal lato opposto del "Cliente" di test di `net.rs`.
    struct ServerFinto {
        listener: TcpListener,
        indirizzo: SocketAddr,
    }

    impl ServerFinto {
        async fn avvia() -> Self {
            let listener = TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind sulla porta effimera riuscito");
            let indirizzo = listener.local_addr().expect("indirizzo locale disponibile");
            Self {
                listener,
                indirizzo,
            }
        }

        /// Accetta la prossima connessione in arrivo.
        async fn accetta(&self) -> ConnessioneFinta {
            let (stream, _) = timeout(ATTESA, self.listener.accept())
                .await
                .expect("il client deve collegarsi entro l'attesa")
                .expect("connessione accettata");
            ConnessioneFinta::da_stream(stream)
        }

        /// Vero se entro `durata` nessuno prova a collegarsi.
        ///
        /// E' l'opposto di [`accetta`](Self::accetta): serve a verificare che un
        /// tentativo di riconnessione **non** parta. Va usata con un backoff di prova
        /// molto piu' corto della durata attesa, altrimenti direbbe vero anche di un
        /// client che si sta semplicemente ancora preparando a riprovare.
        async fn nessuna_connessione_entro(&self, durata: Duration) -> bool {
            timeout(durata, self.listener.accept()).await.is_err()
        }
    }

    /// Connessione accettata dal server finto, sul verso opposto del `Cliente` di
    /// `net.rs`: legge [`ClientMessage`] e scrive [`ServerMessage`].
    struct ConnessioneFinta {
        lettura: FramedRead<OwnedReadHalf, ClientCodec>,
        scrittura: FramedWrite<OwnedWriteHalf, ServerCodec>,
    }

    impl ConnessioneFinta {
        fn da_stream(stream: TcpStream) -> Self {
            let (lettura, scrittura) = stream.into_split();
            Self {
                lettura: FramedRead::new(lettura, ClientCodec::new()),
                scrittura: FramedWrite::new(scrittura, ServerCodec::new()),
            }
        }

        async fn invia(&mut self, messaggio: ServerMessage) {
            self.scrittura
                .send(messaggio)
                .await
                .expect("invio riuscito");
        }

        /// Prossimo messaggio del client; fa fallire il test se non arriva.
        async fn ricevi(&mut self) -> ClientMessage {
            timeout(ATTESA, self.lettura.next())
                .await
                .expect("il client deve inviare entro l'attesa")
                .expect("la connessione non deve chiudersi qui")
                .expect("il frame ricevuto deve essere valido")
        }

        /// Risponde Welcome a Hello e SessionToken a Login o Register.
        async fn accetta_login(&mut self) {
            match self.ricevi().await {
                ClientMessage::Hello { protocol_version } => {
                    assert_eq!(protocol_version, PROTOCOL_VERSION);
                }
                altro => panic!("atteso Hello, ottenuto {altro:?}"),
            }
            self.invia(ServerMessage::Welcome {
                protocol_version: PROTOCOL_VERSION,
            })
            .await;

            match self.ricevi().await {
                ClientMessage::Login { .. } | ClientMessage::Register { .. } => {}
                altro => panic!("atteso Login o Register, ottenuto {altro:?}"),
            }
            self.invia(ServerMessage::SessionToken {
                token: "a".repeat(64),
            })
            .await;
        }
    }

    // --- analisi degli argomenti ---

    /// Trasforma gli argomenti di un test nella forma che [`ClientConfig::from_args`]
    /// si aspetta.
    fn argomenti(elenco: &[&str]) -> Vec<String> {
        elenco.iter().map(|voce| (*voce).to_owned()).collect()
    }

    #[test]
    fn help_restituisce_uso_senza_altri_controlli() {
        let avvio = ClientConfig::from_args(argomenti(&["--help"])).expect("analisi riuscita");
        assert_eq!(avvio, Avvio::Uso);
    }

    #[test]
    fn configurazione_minima_con_sorgente_manuale_e_accettata() {
        let avvio = ClientConfig::from_args(argomenti(&[
            "--server",
            "127.0.0.1:7878",
            "--user",
            "veicolo_01",
            "--password",
            "scorta12",
            "--source",
            "manual",
            "--start",
            "45.07,7.68",
        ]))
        .expect("analisi riuscita");

        match avvio {
            Avvio::Configurato(config) => {
                assert_eq!(
                    config.server_addr,
                    "127.0.0.1:7878"
                        .parse::<SocketAddr>()
                        .expect("indirizzo di test valido")
                );
                assert!(!config.register);
                assert_eq!(
                    config.source,
                    SourceKind::Manual {
                        iniziale: Coordinate::new(45.07, 7.68).expect("coordinata valida"),
                    }
                );
            }
            Avvio::Uso => panic!("atteso Configurato"),
        }
    }

    #[test]
    fn register_alza_il_flag() {
        let avvio = ClientConfig::from_args(argomenti(&[
            "--server",
            "127.0.0.1:7878",
            "--user",
            "veicolo_01",
            "--password",
            "scorta12",
            "--register",
            "--source",
            "manual",
            "--start",
            "45.07,7.68",
        ]))
        .expect("analisi riuscita");

        match avvio {
            Avvio::Configurato(config) => assert!(config.register),
            Avvio::Uso => panic!("atteso Configurato"),
        }
    }

    #[test]
    fn source_file_richiede_file_path() {
        let esito = ClientConfig::from_args(argomenti(&[
            "--server",
            "127.0.0.1:7878",
            "--user",
            "veicolo_01",
            "--password",
            "scorta12",
            "--source",
            "file",
        ]));

        assert!(matches!(
            esito,
            Err(ArgsError::OpzioneObbligatoriaMancante("--file-path"))
        ));
    }

    #[test]
    fn source_file_con_percorso_e_loop() {
        let avvio = ClientConfig::from_args(argomenti(&[
            "--server",
            "127.0.0.1:7878",
            "--user",
            "veicolo_01",
            "--password",
            "scorta12",
            "--source",
            "file",
            "--file-path",
            "percorso.csv",
            "--file-loop",
        ]))
        .expect("analisi riuscita");

        match avvio {
            Avvio::Configurato(config) => assert_eq!(
                config.source,
                SourceKind::File {
                    percorso: PathBuf::from("percorso.csv"),
                    loop_forever: true,
                }
            ),
            Avvio::Uso => panic!("atteso Configurato"),
        }
    }

    #[test]
    fn source_random_completa_con_tutti_i_parametri() {
        let avvio = ClientConfig::from_args(argomenti(&[
            "--server",
            "127.0.0.1:7878",
            "--user",
            "veicolo_01",
            "--password",
            "scorta12",
            "--source",
            "random",
            "--start",
            "45.07,7.68",
            "--radius",
            "500",
            "--step",
            "20",
            "--seed",
            "42",
        ]))
        .expect("analisi riuscita");

        match avvio {
            Avvio::Configurato(config) => assert_eq!(
                config.source,
                SourceKind::RandomWalk {
                    partenza: Coordinate::new(45.07, 7.68).expect("coordinata valida"),
                    raggio_massimo_m: 500.0,
                    passo_massimo_m: 20.0,
                    seed: 42,
                }
            ),
            Avvio::Uso => panic!("atteso Configurato"),
        }
    }

    #[test]
    fn source_route_con_una_pausa() {
        let avvio = ClientConfig::from_args(argomenti(&[
            "--server",
            "127.0.0.1:7878",
            "--user",
            "veicolo_01",
            "--password",
            "scorta12",
            "--source",
            "route",
            "--start",
            "45.07,7.68",
            "--end",
            "45.46,9.19",
            "--speed",
            "90",
            "--pause",
            "0.5@600",
        ]))
        .expect("analisi riuscita");

        match avvio {
            Avvio::Configurato(config) => assert_eq!(
                config.source,
                SourceKind::Route {
                    partenza: Coordinate::new(45.07, 7.68).expect("coordinata valida"),
                    arrivo: Coordinate::new(45.46, 9.19).expect("coordinata valida"),
                    velocita_kmh: 90.0,
                    pause: vec![crate::sources::Pausa {
                        a_frazione: 0.5,
                        durata: Duration::from_secs(600),
                    }],
                }
            ),
            Avvio::Uso => panic!("atteso Configurato"),
        }
    }

    #[test]
    fn source_sconosciuta_e_rifiutata() {
        let esito = ClientConfig::from_args(argomenti(&[
            "--server",
            "127.0.0.1:7878",
            "--user",
            "veicolo_01",
            "--password",
            "scorta12",
            "--source",
            "satellite",
        ]));

        assert!(matches!(
            esito,
            Err(ArgsError::SorgenteSconosciuta(sorgente)) if sorgente == "satellite"
        ));
    }

    #[test]
    fn opzione_sconosciuta_e_rifiutata() {
        let esito = ClientConfig::from_args(argomenti(&["--boh"]));
        assert!(matches!(esito, Err(ArgsError::OpzioneSconosciuta(opzione)) if opzione == "--boh"));
    }

    #[test]
    fn valore_mancante_e_rifiutato() {
        let esito = ClientConfig::from_args(argomenti(&["--server"]));
        assert!(matches!(esito, Err(ArgsError::ValoreMancante("--server"))));
    }

    #[test]
    fn indirizzo_non_valido_e_rifiutato() {
        let esito = ClientConfig::from_args(argomenti(&["--server", "non-un-indirizzo"]));
        assert!(matches!(esito, Err(ArgsError::IndirizzoNonValido { .. })));
    }

    #[test]
    fn username_non_valido_e_rifiutato() {
        let esito = ClientConfig::from_args(argomenti(&["--user", "ab"]));
        assert!(matches!(esito, Err(ArgsError::UsernameNonValido(_))));
    }

    #[test]
    fn coordinata_malformata_e_rifiutata() {
        let esito = ClientConfig::from_args(argomenti(&["--start", "non-una-coordinata"]));
        assert!(matches!(
            esito,
            Err(ArgsError::ParametroNonValido {
                opzione: "--start",
                ..
            })
        ));
    }

    #[test]
    fn senza_server_manca_l_opzione_obbligatoria() {
        let esito = ClientConfig::from_args(argomenti(&[
            "--user",
            "veicolo_01",
            "--password",
            "scorta12",
            "--source",
            "manual",
            "--start",
            "45.07,7.68",
        ]));
        assert!(matches!(
            esito,
            Err(ArgsError::OpzioneObbligatoriaMancante("--server"))
        ));
    }

    // --- nucleo di rete ---

    #[tokio::test]
    async fn handshake_completo_produce_evento_connesso() {
        let finto = ServerFinto::avvia().await;
        let mut handle = avvia(config(finto.indirizzo));

        let mut connessione = finto.accetta().await;
        connessione.accetta_login().await;

        assert_eq!(prossimo_evento(&mut handle).await, ClientEvent::Connected);
    }

    #[tokio::test(start_paused = true)]
    async fn il_ticker_invia_una_posizione_ogni_trenta_secondi_simulati() {
        let finto = ServerFinto::avvia().await;
        let mut handle = avvia(config_con_random(finto.indirizzo));

        let mut connessione = finto.accetta().await;
        connessione.accetta_login().await;
        assert_eq!(prossimo_evento(&mut handle).await, ClientEvent::Connected);

        tokio::time::advance(Duration::from_secs(30)).await;

        let coordinate = match connessione.ricevi().await {
            ClientMessage::Position { coordinate, .. } => coordinate,
            altro => panic!("atteso Position, ottenuto {altro:?}"),
        };
        assert_eq!(
            prossimo_evento(&mut handle).await,
            ClientEvent::PositionSent(coordinate)
        );
    }

    #[tokio::test]
    async fn send_text_produce_text_to_server() {
        let finto = ServerFinto::avvia().await;
        let mut handle = avvia(config(finto.indirizzo));

        let mut connessione = finto.accetta().await;
        connessione.accetta_login().await;
        assert_eq!(prossimo_evento(&mut handle).await, ClientEvent::Connected);

        handle
            .command_tx
            .send(ClientCommand::SendText("ciao".to_owned()))
            .await
            .expect("invio del comando riuscito");

        match connessione.ricevi().await {
            ClientMessage::TextToServer { body } => assert_eq!(body, "ciao"),
            altro => panic!("atteso TextToServer, ottenuto {altro:?}"),
        }
    }

    #[tokio::test]
    async fn alla_disconnessione_il_client_si_riconnette_da_solo() {
        let finto = ServerFinto::avvia().await;
        let mut handle = avvia(config(finto.indirizzo));

        let mut connessione = finto.accetta().await;
        connessione.accetta_login().await;
        assert_eq!(prossimo_evento(&mut handle).await, ClientEvent::Connected);

        // Chiusura brusca lato server: il banco resta comunque in ascolto sullo
        // stesso indirizzo, pronto ad accettare il tentativo di riconnessione.
        drop(connessione);

        assert!(matches!(
            prossimo_evento(&mut handle).await,
            ClientEvent::Disconnected(_)
        ));

        let mut connessione = finto.accetta().await;
        connessione.accetta_login().await;
        assert_eq!(prossimo_evento(&mut handle).await, ClientEvent::Connected);
    }

    #[tokio::test]
    async fn la_sessione_sostituita_non_fa_ripartire_la_riconnessione() {
        let finto = ServerFinto::avvia().await;
        // Backoff cortissimo e invio periodico lasciato a trenta secondi: se il client
        // provasse a riconnettersi lo farebbe entro pochi decine di millisecondi, ben
        // dentro l'attesa qui sotto, e nel frattempo il ticker delle posizioni non
        // interferisce con la sequenza degli eventi.
        let tempi = Tempi {
            intervallo_posizione: Duration::from_secs(30),
            backoff: [Duration::from_millis(20); 4],
            backoff_massimo: Duration::from_millis(20),
        };
        let mut handle = avvia_con(config(finto.indirizzo), Arc::new(SystemClock), tempi);

        let mut connessione = finto.accetta().await;
        connessione.accetta_login().await;
        assert_eq!(prossimo_evento(&mut handle).await, ClientEvent::Connected);

        // Il server non chiude di brutto: annuncia la sostituzione, come fa il server
        // vero prima di lasciar cadere il canale della connessione precedente.
        connessione
            .invia(ServerMessage::Error {
                code: ErrorCode::NotAuthenticated,
                message: SESSION_REPLACED_MESSAGE.to_owned(),
            })
            .await;

        assert_eq!(
            prossimo_evento(&mut handle).await,
            ClientEvent::Disconnected(SESSION_REPLACED_MESSAGE.to_owned()),
            "la sostituzione e' una disconnessione con un motivo, non un errore qualunque"
        );

        // Il punto del test: a differenza di
        // `alla_disconnessione_il_client_si_riconnette_da_solo`, qui nessun nuovo
        // handshake deve arrivare. Riconnettersi espellerebbe la connessione che ha
        // preso il posto di questa, che a sua volta si riconnetterebbe, e cosi' via.
        assert!(
            finto
                .nessuna_connessione_entro(Duration::from_millis(500))
                .await,
            "dopo una sostituzione di sessione il client non deve ritentare da solo"
        );

        // Il task e' terminato: tocca all'utente decidere se rilanciare il client.
        assert_eq!(
            timeout(ATTESA, handle.event_rx.recv())
                .await
                .expect("il task deve essere gia' terminato"),
            None,
            "il canale degli eventi si chiude con il task"
        );
    }

    #[tokio::test]
    async fn le_posizioni_accumulate_durante_la_disconnessione_arrivano_in_ordine() {
        let finto = ServerFinto::avvia().await;
        let indirizzo = finto.indirizzo;
        let mut handle = avvia_con(
            config_con_random(indirizzo),
            Arc::new(SystemClock),
            tempi_di_prova(),
        );

        let mut connessione = finto.accetta().await;
        connessione.accetta_login().await;
        assert_eq!(prossimo_evento(&mut handle).await, ClientEvent::Connected);

        // Chiude anche il banco: i tentativi di riconnessione devono fallire per
        // restare nella fase di attesa, dove il ticker continua a interrogare la
        // sorgente.
        drop(connessione);
        drop(finto);
        assert!(matches!(
            prossimo_evento(&mut handle).await,
            ClientEvent::Disconnected(_)
        ));

        // Con un intervallo di prova di 80ms, 250ms bastano ad accumulare piu' di
        // una posizione mentre i tentativi di riconnessione falliscono.
        tokio::time::sleep(Duration::from_millis(250)).await;

        let ripartito = TcpListener::bind(indirizzo)
            .await
            .expect("bind sullo stesso indirizzo riuscito");
        let (stream, _) = timeout(ATTESA, ripartito.accept())
            .await
            .expect("il client deve ricollegarsi entro l'attesa")
            .expect("accept riuscito");
        let mut connessione = ConnessioneFinta::da_stream(stream);
        connessione.accetta_login().await;

        // Drena gli eventuali `Disconnected` dei tentativi falliti nel frattempo,
        // fino al nuovo `Connected`.
        loop {
            match prossimo_evento(&mut handle).await {
                ClientEvent::Connected => break,
                ClientEvent::Disconnected(_) => continue,
                altro => panic!("evento inatteso in attesa di Connected: {altro:?}"),
            }
        }

        let prima = match connessione.ricevi().await {
            ClientMessage::Position { coordinate, .. } => coordinate,
            altro => panic!("atteso Position, ottenuto {altro:?}"),
        };
        let seconda = match connessione.ricevi().await {
            ClientMessage::Position { coordinate, .. } => coordinate,
            altro => panic!("atteso Position, ottenuto {altro:?}"),
        };

        assert_ne!(
            prima, seconda,
            "le due posizioni accumulate devono restare distinte e nell'ordine di generazione"
        );
    }

    #[tokio::test]
    async fn quit_chiude_il_task_e_il_canale_eventi_si_chiude() {
        let finto = ServerFinto::avvia().await;
        let mut handle = avvia(config(finto.indirizzo));

        let mut connessione = finto.accetta().await;
        connessione.accetta_login().await;
        assert_eq!(prossimo_evento(&mut handle).await, ClientEvent::Connected);

        handle
            .command_tx
            .send(ClientCommand::Quit)
            .await
            .expect("invio del comando riuscito");

        match connessione.ricevi().await {
            ClientMessage::Logout => {}
            altro => panic!("atteso Logout, ottenuto {altro:?}"),
        }

        let chiuso = timeout(ATTESA, handle.event_rx.recv())
            .await
            .expect("il canale deve chiudersi entro l'attesa");
        assert_eq!(
            chiuso, None,
            "il ricevitore degli eventi deve vedere None dopo Quit"
        );
    }

    #[tokio::test]
    async fn chiudi_manda_logout_e_attende_la_fine_del_task() {
        let finto = ServerFinto::avvia().await;
        let mut handle = avvia(config(finto.indirizzo));

        let mut connessione = finto.accetta().await;
        connessione.accetta_login().await;
        assert_eq!(prossimo_evento(&mut handle).await, ClientEvent::Connected);

        assert!(
            handle.chiudi().await,
            "il task di rete deve chiudersi entro l'attesa"
        );

        // Il `Logout` e' partito prima che il task terminasse: `chiudi` ha atteso la
        // chiusura ordinata invece di abbandonare il task a meta'.
        match connessione.ricevi().await {
            ClientMessage::Logout => {}
            altro => panic!("atteso Logout, ottenuto {altro:?}"),
        }
    }

    #[tokio::test]
    async fn chiudi_termina_anche_senza_connessione_aperta() {
        // Nessuno in ascolto sull'indirizzo: il task resta nel ciclo di riconnessione,
        // dove la chiusura deve funzionare esattamente come a connessione aperta.
        let indirizzo: SocketAddr = "127.0.0.1:1".parse().expect("indirizzo di test valido");
        let handle = avvia_con(config(indirizzo), Arc::new(SystemClock), tempi_di_prova());

        assert!(
            handle.chiudi().await,
            "anche senza connessione il task deve chiudersi entro l'attesa"
        );
    }

    #[tokio::test]
    async fn una_sorgente_esaurita_manda_ping_invece_di_restare_muta() {
        // Un percorso di un solo punto, senza ripetizione: dopo il primo invio la
        // sorgente e' esaurita e non produrra' mai piu' una posizione.
        let file = FileDiProva::nuovo("esaurita", "lat,lon,offset_secs\n45.07,7.68,0\n");
        let finto = ServerFinto::avvia().await;
        let config = ClientConfig {
            source: SourceKind::File {
                percorso: file.percorso.clone(),
                loop_forever: false,
            },
            ..config(finto.indirizzo)
        };
        let mut handle = avvia_con(config, Arc::new(SystemClock), tempi_di_prova());

        let mut connessione = finto.accetta().await;
        connessione.accetta_login().await;
        assert_eq!(prossimo_evento(&mut handle).await, ClientEvent::Connected);

        match connessione.ricevi().await {
            ClientMessage::Position { .. } => {}
            altro => panic!("atteso il punto del percorso, ottenuto {altro:?}"),
        }

        // Da qui la sorgente restituisce sempre `None`: senza il `Ping` la connessione
        // resterebbe muta fino alla scadenza di inattivita' del server.
        match connessione.ricevi().await {
            ClientMessage::Ping => {}
            altro => panic!("atteso Ping a sorgente esaurita, ottenuto {altro:?}"),
        }
        match connessione.ricevi().await {
            ClientMessage::Ping => {}
            altro => panic!("atteso un secondo Ping, ottenuto {altro:?}"),
        }
    }
}
