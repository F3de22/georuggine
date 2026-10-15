//! Nucleo di rete del client: configurazione, sessione e ciclo di invio delle posizioni.
//!
//! [`ClientHandle::spawn`] avvia un task che apre la connessione, si autentica e resta in
//! ascolto. L'interfaccia non conosce il socket: le manda [`ClientCommand`] e ne legge
//! [`ClientEvent`], attraverso due canali.
//!
//! # Riconnessione
//!
//! Una connessione che cade non ferma il client: si riprova con un backoff crescente,
//! continuando nel frattempo a interrogare la sorgente e ad accumulare in una coda locale
//! le posizioni che non si possono inviare. Due casi fanno invece terminare il task:
//! credenziali rifiutate o versione di protocollo incompatibile (riprovare darebbe lo
//! stesso esito) e la sostituzione di sessione, dove riconnettersi espellerebbe la
//! connessione che ha preso il posto di questa, in un ciclo perpetuo fra i due client.

use std::collections::VecDeque;
use std::net::{AddrParseError, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::mpsc::Sender as SorgenteManuale;
use std::time::Duration;

use chrono::{DateTime, Utc};
use futures_util::{SinkExt as _, StreamExt as _};
use georuggine_common::clock::Clock;
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

/// Posizioni che la coda di disconnessione trattiene prima di scartare le piu' vecchie.
const CAPACITA_CODA_POSIZIONI: usize = 200;

/// Capacita' del canale dei comandi verso il task di rete.
const CAPACITA_COMANDI: usize = 32;

/// Capacita' del canale degli eventi dal task di rete.
const CAPACITA_EVENTI: usize = 32;

/// Attesa massima concessa al task di rete per chiudersi in modo ordinato.
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
/// Come nel server, `--help` non stampa ne' esce da dentro l'analisi: la decisione torna
/// al chiamante, cosi' la funzione resta pura e verificabile.
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
    /// Analizza gli argomenti da riga di comando, senza il nome del programma.
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

/// Comando che l'interfaccia manda al task di rete. Per invio di messaggi
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

/// Evento che il task di rete manda all'interfaccia.
#[derive(Debug, Clone, PartialEq)]
pub enum ClientEvent {
    /// La sessione e' aperta: handshake, autenticazione e messaggi in attesa consegnati.
    Connected,
    /// La connessione e' caduta; il campo descrive il motivo. Un tentativo di
    /// riconnessione e' gia' in corso, salvo che il task stia per terminare
    Disconnected(String),
    /// Testo ricevuto dal server.
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

/// Parametri temporali del task di rete: intervallo di invio periodico e progressione del
/// backoff di riconnessione.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Tempi {
    /// Intervallo fra due interrogazioni della sorgente di posizione.
    pub intervallo_posizione: Duration,
    /// Ritardo dei primi tentativi di riconnessione, in ordine.
    pub backoff: [Duration; 4],
    /// Ritardo a cui il backoff resta una volta esaurita la progressione.
    pub backoff_massimo: Duration,
}

impl Default for Tempi {
    /// Trenta secondi fra un invio e il successivo, come chiede la traccia; backoff 1s,
    /// 2s, 4s, 8s, poi 30s fisso.
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
    /// Ritardo del tentativo numero `tentativo` (il primo e' 1), con un jitter casuale fino al 20% in piu'
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

/// Stato che il task di rete porta attraverso le fasi connessa e disconnessa.
struct StatoRete<'a> {
    sorgente: &'a mut dyn PositionSource,
    manuale: Option<&'a SorgenteManuale<Coordinate>>,
    coda: &'a mut VecDeque<(Coordinate, DateTime<Utc>)>,
    ticker: &'a mut Interval,
    clock: &'a Arc<dyn Clock>,
}

/// Motivo per cui l'apertura della sessione non e' riuscita.
enum MotivoFallimento {
    /// Un guasto di rete o un protocollo violato: vale la pena riprovare.
    Rete(String),
    /// Credenziali rifiutate o versione incompatibile: riprovare con la stessa
    /// configurazione darebbe di nuovo lo stesso esito, quindi il task termina.
    Terminale(String),
}

/// Esito della fase connessa del task di rete.
enum EsitoFase {
    /// La connessione e' caduta: il chiamante riprova con il backoff.
    Riconnetti(String),
    /// Il server ha chiuso la sessione apposta, perche' lo stesso utente si e' collegato
    /// da un'altra parte: il task termina **senza** riprovare, ma l'utente va avvisato.
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

/// Nucleo di rete: avvia un task interno e lo disaccoppia dal resto del programma con due
/// canali.
///
/// Il canale degli eventi si chiude (restituendo `None`) quando il task termina. Chi lo
/// avvia deve chiuderlo con [`ClientHandle::chiudi`]: lasciar cadere l'handle non basta.
pub struct ClientHandle {
    /// Comandi da mandare al task di rete.
    pub command_tx: mpsc::Sender<ClientCommand>,
    /// Eventi che il task di rete produce.
    pub event_rx: mpsc::Receiver<ClientEvent>,
    /// Task di rete, atteso da [`ClientHandle::chiudi`]. Non e' pubblico: chi usa
    /// l'handle non deve poterlo interrompere se non passando dalla chiusura ordinata.
    task: JoinHandle<()>,
}

impl ClientHandle {
    /// Avvia il task di rete.
    ///
    /// `sorgente` arriva gia' costruita da [SourceKind::build]
    /// `clock` e `tempi` sono parametri e non valori fissi perche' i test possano
    /// accorciare intervallo e backoff e iniettare un orologio deterministico. In produzione si passano
    /// `SystemClock` e [`Tempi::default`].
    pub fn spawn(
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

    /// Chiude la sessione e attende che il task di rete sia davvero terminato; falso se
    /// la scadenza passa per prima.
    ///
    /// Lasciar cadere l'handle non basta: al ritorno di `main` il runtime viene distrutto
    /// e i task ancora vivi spariscono, quindi il `Logout` andrebbe perso in una corsa e
    /// il server libererebbe la sessione solo alla scadenza di inattivita'. L'ordine
    /// conta:
    ///
    /// 1. cade il ricevitore degli eventi, altrimenti un task fermo su un canale pieno —
    ///    nessuno lo drena piu' — non arriverebbe mai a leggere il comando di chiusura;
    /// 2. parte [`ClientCommand::Quit`], che fa inviare `Logout` e chiudere la scrittura;
    /// 3. cade anche il mittente dei comandi, cosi' un task gia' oltre la lettura del
    ///    comando termina comunque sul canale chiuso;
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
            // L'errore di join significa che il task e' andato in panico: non c'e' nulla
            // da fare oltre a smettere di aspettarlo.
            let _ = task.await;
        })
        .await
        .is_ok()
    }
}

/// Corpo del task di rete: alterna connessione e attesa di riconnessione finche' non
/// arriva [`ClientCommand::Quit`] o un fallimento non ripetibile dell'autenticazione.
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
    // Il primo tick matura subito: lo si consuma qui, altrimenti la prima posizione
    // partirebbe all'apertura della connessione invece che dopo un intervallo.
    ticker.tick().await;

    let mut tentativo: u32 = 0;
    // `config.register` dice come aprire la *prima* sessione, non tutte: una volta
    // registrato l'utente esiste, e ripresentare `Register` a ogni riconnessione si
    // prenderebbe `UsernameTaken`, che e' terminale e farebbe uscire il task. Il flag vive
    // percio' nel ciclo, e lo abbassa solo un `Register` a cui il server ha risposto
    // `SessionToken`.
    let mut da_registrare = config.register;

    loop {
        match connetti_e_autentica(&config, da_registrare).await {
            Ok((mut ingresso, mut uscita)) => {
                da_registrare = false;
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
                    // Chiusura voluta dal server: si avvisa e si esce, senza passare dal
                    // backoff. Rilanciare il client tocca all'utente.
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

/// Apre la connessione ed esegue handshake e autenticazione.
///
/// `registra` decide fra [`ClientMessage::Register`] e [`ClientMessage::Login`], e non e'
/// `config.register` perche' quel campo vale per la prima sessione e non per le successive.
async fn connetti_e_autentica(
    config: &ClientConfig,
    registra: bool,
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

    let richiesta = if registra {
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

/// Traduce un [`ServerMessage::Error`] ricevuto durante handshake o autenticazione.
fn esito_errore(code: ErrorCode, message: String) -> MotivoFallimento {
    match code {
        ErrorCode::InvalidCredentials
        | ErrorCode::UsernameTaken
        | ErrorCode::UnsupportedVersion => MotivoFallimento::Terminale(message),
        _ => MotivoFallimento::Rete(message),
    }
}

/// Ciclo principale a connessione aperta: invii periodici, comandi in arrivo e messaggi
/// dal server, finche' la connessione non cade o arriva [`ClientCommand::Quit`].
async fn fase_connessa(
    ingresso: &mut FramedRead<OwnedReadHalf, ServerCodec>,
    uscita: &mut FramedWrite<OwnedWriteHalf, ClientCodec>,
    stato: &mut StatoRete<'_>,
    command_rx: &mut mpsc::Receiver<ClientCommand>,
    event_tx: &mpsc::Sender<ClientEvent>,
) -> EsitoFase {
    // Le posizioni accumulate durante una disconnessione precedente partono subito.
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
                    // chiusa dal server alla scadenza di inattivita'
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
                    // La sostituzione di sessione si riconosce dal testo e non dal codice:
                    // il protocollo non ha un `ErrorCode` dedicato. Il ramo precede quello
                    // generale apposta, altrimenti finirebbe fra gli errori qualunque.
                    Some(Ok(ServerMessage::Error { message, .. }))
                        if message == SESSION_REPLACED_MESSAGE =>
                    {
                        return EsitoFase::SessioneSostituita(message);
                    }
                    Some(Ok(ServerMessage::Error { message, .. })) => {
                        let _ = event_tx.send(ClientEvent::Error(message)).await;
                    }
                    // Welcome, SessionToken, Ack e Pong fuori sequenza: nulla da fare.
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

/// Invia in ordine tutte le posizioni in coda, emettendo [`ClientEvent::PositionSent`] per
/// ognuna.
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

    use std::time::Instant;

    use georuggine_common::clock::SystemClock;
    use tokio::net::TcpListener;

    /// Quanto un test aspetta un evento o un messaggio prima di considerarlo mancato.
    const ATTESA: Duration = Duration::from_secs(5);

    /// Tempi accorciati per i test su backoff e invio periodico: reali, ma troppo brevi
    /// perche' un test debba aspettare i valori di produzione.
    fn tempi_di_prova() -> Tempi {
        Tempi {
            intervallo_posizione: Duration::from_millis(80),
            backoff: [Duration::from_millis(20); 4],
            backoff_massimo: Duration::from_millis(20),
        }
    }

    /// Backoff cortissimo, cosi' la riconnessione arriva subito, e invio periodico lasciato
    /// a trenta secondi, cosi' il ticker non infila messaggi in mezzo alla sequenza
    /// osservata.
    fn tempi_di_riconnessione() -> Tempi {
        Tempi {
            intervallo_posizione: Duration::from_secs(30),
            backoff: [Duration::from_millis(20); 4],
            backoff_massimo: Duration::from_millis(20),
        }
    }

    fn coordinata(lat: f64, lon: f64) -> Coordinate {
        Coordinate::new(lat, lon).expect("coordinata di test valida")
    }

    /// Configurazione di test con una sorgente manuale, che non produce mai posizioni
    /// nuove da sola.
    fn config(indirizzo: SocketAddr) -> ClientConfig {
        ClientConfig {
            server_addr: indirizzo,
            username: Username::new("veicolo_01").expect("username di test valido"),
            password: "scorta12".to_owned(),
            register: false,
            source: SourceKind::Manual {
                iniziale: coordinata(45.0703, 7.6869),
            },
        }
    }

    /// Configurazione con un cammino casuale: produce posizioni diverse a ogni chiamata.
    fn config_con_random(indirizzo: SocketAddr) -> ClientConfig {
        ClientConfig {
            source: SourceKind::RandomWalk {
                partenza: coordinata(45.0703, 7.6869),
                raggio_massimo_m: 1_000.0,
                passo_massimo_m: 50.0,
                seed: 7,
            },
            ..config(indirizzo)
        }
    }

    /// Avvia il task di rete costruendo la sorgente dalla configurazione, come fa il
    /// binario.
    fn avvia(config: ClientConfig, tempi: Tempi) -> ClientHandle {
        let sorgente = config.source.build().expect("sorgente di test costruibile");
        ClientHandle::spawn(config, sorgente, Arc::new(SystemClock), tempi)
    }

    /// File CSV di prova, cancellato quando il test finisce.
    struct FileDiProva {
        percorso: PathBuf,
    }

    impl FileDiProva {
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

    /// Server finto in ascolto su una porta effimera: parla il protocollo vero con i codec
    /// di `common`.
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

        async fn accetta(&self) -> ConnessioneFinta {
            let (stream, _) = timeout(ATTESA, self.listener.accept())
                .await
                .expect("il client deve collegarsi entro l'attesa")
                .expect("connessione accettata");
            ConnessioneFinta::da_stream(stream)
        }

        /// Vero se entro `durata` nessuno prova a collegarsi.
        ///
        /// Serve a verificare che un tentativo di riconnessione **non** parta. Va usata
        /// con un backoff di prova molto piu' corto della durata attesa, altrimenti
        /// direbbe vero anche di un client che si sta ancora preparando a riprovare.
        async fn nessuna_connessione_entro(&self, durata: Duration) -> bool {
            timeout(durata, self.listener.accept()).await.is_err()
        }
    }

    /// Connessione accettata dal server finto: legge [`ClientMessage`] e scrive
    /// [`ServerMessage`].
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

        /// Risponde Welcome a Hello e restituisce il messaggio di autenticazione che segue,
        /// lasciando al chiamante la risposta: restituirlo invece di scartarlo serve ai
        /// test che devono distinguere `Register` da `Login` tentativo per tentativo.
        async fn handshake(&mut self) -> ClientMessage {
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
                autenticazione @ (ClientMessage::Login { .. } | ClientMessage::Register { .. }) => {
                    autenticazione
                }
                altro => panic!("atteso Login o Register, ottenuto {altro:?}"),
            }
        }

        /// Handshake seguito da un token di sessione: la sessione si apre.
        async fn accetta_login(&mut self) -> ClientMessage {
            let autenticazione = self.handshake().await;
            self.invia(ServerMessage::SessionToken {
                token: "t".repeat(64),
            })
            .await;
            autenticazione
        }

        /// Handshake seguito da un rifiuto delle credenziali.
        async fn rifiuta_login(&mut self, code: ErrorCode, message: &str) -> ClientMessage {
            let autenticazione = self.handshake().await;
            self.invia(ServerMessage::Error {
                code,
                message: message.to_owned(),
            })
            .await;
            autenticazione
        }
    }

    fn argomenti(elenco: &[&str]) -> Vec<String> {
        elenco.iter().map(|voce| (*voce).to_owned()).collect()
    }

    /// Argomenti comuni a tutte le configurazioni di prova, senza la sorgente.
    fn base() -> Vec<&'static str> {
        vec![
            "--server",
            "127.0.0.1:7878",
            "--user",
            "veicolo_01",
            "--password",
            "scorta12",
        ]
    }

    /// Analizza `base()` piu' le opzioni indicate, pretendendo che riesca.
    fn configura(extra: &[&str]) -> ClientConfig {
        let mut elenco = base();
        elenco.extend_from_slice(extra);
        match ClientConfig::from_args(argomenti(&elenco)).expect("analisi riuscita") {
            Avvio::Configurato(config) => config,
            Avvio::Uso => panic!("attesa una configurazione, ottenuto --help"),
        }
    }

    #[test]
    fn la_configurazione_minima_e_accettata_e_help_precede_ogni_controllo() {
        let config = configura(&["--source", "manual", "--start", "45.0703,7.6869"]);

        assert_eq!(config.username.as_str(), "veicolo_01");
        assert_eq!(config.password, "scorta12");
        assert!(!config.register, "senza --register si accede e basta");
        assert_eq!(
            config.source,
            SourceKind::Manual {
                iniziale: coordinata(45.0703, 7.6869)
            }
        );

        assert!(configura(&["--source", "manual", "--start", "45.0,7.0", "--register"]).register);

        // `--help` vince anche su una riga che sarebbe incompleta.
        assert_eq!(
            ClientConfig::from_args(argomenti(&["--help"])).expect("analisi riuscita"),
            Avvio::Uso
        );
    }

    #[test]
    fn ogni_sorgente_raccoglie_i_propri_parametri() {
        assert_eq!(
            configura(&[
                "--source",
                "file",
                "--file-path",
                "percorso.csv",
                "--file-loop"
            ])
            .source,
            SourceKind::File {
                percorso: PathBuf::from("percorso.csv"),
                loop_forever: true,
            }
        );
        assert_eq!(
            configura(&[
                "--source", "random", "--start", "45.0,7.0", "--radius", "500", "--step", "25",
                "--seed", "42",
            ])
            .source,
            SourceKind::RandomWalk {
                partenza: coordinata(45.0, 7.0),
                raggio_massimo_m: 500.0,
                passo_massimo_m: 25.0,
                seed: 42,
            }
        );
        assert_eq!(
            configura(&[
                "--source", "route", "--start", "45.0,7.0", "--end", "45.5,9.0", "--speed", "50",
                "--pause", "0.5@120",
            ])
            .source,
            SourceKind::Route {
                partenza: coordinata(45.0, 7.0),
                arrivo: coordinata(45.5, 9.0),
                velocita_kmh: 50.0,
                pause: vec![crate::sources::Pausa {
                    a_frazione: 0.5,
                    durata: Duration::from_secs(120),
                }],
            }
        );
    }

    #[test]
    fn gli_argomenti_incompleti_o_malformati_sono_errori_non_panici() {
        /// Argomenti di un caso e predicato che riconosce l'errore atteso.
        type Caso = (&'static [&'static str], fn(&ArgsError) -> bool);

        let casi: [Caso; 8] = [
            (&["--sconosciuta"], |e| {
                matches!(e, ArgsError::OpzioneSconosciuta(_))
            }),
            (&["--server"], |e| matches!(e, ArgsError::ValoreMancante(_))),
            (&["--server", "non-un-indirizzo"], |e| {
                matches!(e, ArgsError::IndirizzoNonValido { .. })
            }),
            (&["--user", "ab"], |e| {
                matches!(e, ArgsError::UsernameNonValido(_))
            }),
            (&["--source", "quantistica"], |e| {
                matches!(e, ArgsError::SorgenteSconosciuta(_))
            }),
            (&["--start", "45.0"], |e| {
                matches!(e, ArgsError::ParametroNonValido { .. })
            }),
            (&["--start", "91.0,7.0"], |e| {
                matches!(e, ArgsError::ParametroNonValido { .. })
            }),
            (&["--pause", "meta@sempre"], |e| {
                matches!(e, ArgsError::ParametroNonValido { .. })
            }),
        ];

        for (argomenti_del_caso, atteso) in casi {
            let errore = ClientConfig::from_args(argomenti(argomenti_del_caso))
                .expect_err("gli argomenti del caso non sono validi");
            assert!(atteso(&errore), "errore inatteso: {errore:?}");
        }

        // Le opzioni obbligatorie mancanti, globali e della sorgente scelta.
        let senza_server = vec![
            "--user",
            "veicolo_01",
            "--password",
            "scorta12",
            "--source",
            "manual",
            "--start",
            "45.0,7.0",
        ];
        let mut senza_percorso = base();
        senza_percorso.extend_from_slice(&["--source", "file"]);

        for (elenco, mancante) in [
            (senza_server, "--server"),
            (senza_percorso, "--file-path"),
            (base(), "--source"),
        ] {
            match ClientConfig::from_args(argomenti(&elenco)) {
                Err(ArgsError::OpzioneObbligatoriaMancante(opzione)) => {
                    assert_eq!(opzione, mancante);
                }
                altro => panic!("atteso {mancante} mancante, ottenuto {altro:?}"),
            }
        }
    }

    #[tokio::test]
    async fn l_handshake_completo_produce_l_evento_connesso() {
        let finto = ServerFinto::avvia().await;
        let mut handle = avvia(config(finto.indirizzo), Tempi::default());

        let mut connessione = finto.accetta().await;
        connessione.accetta_login().await;

        assert_eq!(prossimo_evento(&mut handle).await, ClientEvent::Connected);
    }

    /// Il ticker scatta a ogni intervallo, non solo una volta.
    ///
    /// Il tempo qui e' vero, solo accorciato: `start_paused` non puo' convivere con un
    /// `TcpListener` vero, perche' tokio farebbe avanzare l'orologio appena tutti i task
    /// sono sospesi e su alcune piattaforme una connessione di loopback non e' ancora
    /// pronta in quell'istante, facendo scadere l'handshake prima che arrivi.
    #[tokio::test]
    async fn il_ticker_invia_una_posizione_a_ogni_intervallo() {
        let intervallo = tempi_di_prova().intervallo_posizione;
        let finto = ServerFinto::avvia().await;
        let avvio = Instant::now();
        let mut handle = avvia(config_con_random(finto.indirizzo), tempi_di_prova());

        let mut connessione = finto.accetta().await;
        connessione.accetta_login().await;
        assert_eq!(prossimo_evento(&mut handle).await, ClientEvent::Connected);

        // Due scatti di fila: uno solo direbbe che il ticker parte, non che si ripete.
        for numero in 1..=2u32 {
            let coordinate = match connessione.ricevi().await {
                ClientMessage::Position { coordinate, .. } => coordinate,
                altro => panic!("atteso Position, ottenuto {altro:?}"),
            };
            assert_eq!(
                prossimo_evento(&mut handle).await,
                ClientEvent::PositionSent(coordinate)
            );

            // Solo un minimo. Il primo tick e' consumato all'avvio del task, quindi lo
            // scatto n-esimo non puo' maturare prima di n intervalli: e' questo a dire che
            // le posizioni seguono il ritmo invece di partire tutte insieme. Un massimo
            // sarebbe instabile, perche' su una macchina carica uno scatto puo' ritardare.
            let trascorso = avvio.elapsed();
            assert!(
                trascorso >= intervallo * numero,
                "lo scatto {numero} e' arrivato dopo {trascorso:?}, prima dei {numero} \
                 intervalli da {intervallo:?} che deve attendere"
            );
        }
    }

    #[tokio::test]
    async fn send_text_produce_text_to_server() {
        let finto = ServerFinto::avvia().await;
        let mut handle = avvia(config(finto.indirizzo), Tempi::default());

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
        let mut handle = avvia(config(finto.indirizzo), tempi_di_riconnessione());

        let mut connessione = finto.accetta().await;
        connessione.accetta_login().await;
        assert_eq!(prossimo_evento(&mut handle).await, ClientEvent::Connected);

        // Chiusura brusca lato server: il banco resta in ascolto sullo stesso indirizzo,
        // pronto ad accettare il tentativo di riconnessione.
        drop(connessione);
        assert!(matches!(
            prossimo_evento(&mut handle).await,
            ClientEvent::Disconnected(_)
        ));

        let mut connessione = finto.accetta().await;
        connessione.accetta_login().await;
        assert_eq!(prossimo_evento(&mut handle).await, ClientEvent::Connected);
    }

    /// Con `--register` il primo tentativo registra, ma solo quello: una volta creato,
    /// l'utente esiste, e un secondo `Register` si prenderebbe `UsernameTaken`, che e'
    /// terminale e farebbe uscire il task.
    #[tokio::test]
    async fn dopo_una_registrazione_riuscita_la_riconnessione_accede() {
        let finto = ServerFinto::avvia().await;
        let config = ClientConfig {
            register: true,
            ..config(finto.indirizzo)
        };
        let mut handle = avvia(config, tempi_di_riconnessione());

        let mut connessione = finto.accetta().await;
        match connessione.accetta_login().await {
            ClientMessage::Register { username, password } => {
                assert_eq!(username.as_str(), "veicolo_01");
                assert_eq!(password, "scorta12");
            }
            altro => panic!("atteso Register al primo tentativo, ottenuto {altro:?}"),
        }
        assert_eq!(prossimo_evento(&mut handle).await, ClientEvent::Connected);

        drop(connessione);
        assert!(matches!(
            prossimo_evento(&mut handle).await,
            ClientEvent::Disconnected(_)
        ));

        let mut connessione = finto.accetta().await;
        assert!(
            matches!(
                connessione.accetta_login().await,
                ClientMessage::Login { .. }
            ),
            "dopo una registrazione riuscita il tentativo seguente deve accedere con Login"
        );
        assert_eq!(prossimo_evento(&mut handle).await, ClientEvent::Connected);
    }

    #[tokio::test]
    async fn senza_register_ogni_tentativo_manda_login() {
        let finto = ServerFinto::avvia().await;
        let mut handle = avvia(config(finto.indirizzo), tempi_di_riconnessione());

        let mut connessione = finto.accetta().await;
        assert!(matches!(
            connessione.accetta_login().await,
            ClientMessage::Login { .. }
        ));
        assert_eq!(prossimo_evento(&mut handle).await, ClientEvent::Connected);

        drop(connessione);
        assert!(matches!(
            prossimo_evento(&mut handle).await,
            ClientEvent::Disconnected(_)
        ));

        let mut connessione = finto.accetta().await;
        assert!(
            matches!(
                connessione.accetta_login().await,
                ClientMessage::Login { .. }
            ),
            "senza register anche la riconnessione accede"
        );
    }

    /// Solo un `Register` **riuscito** abbassa il flag. Se il primo tentativo si prende
    /// `UsernameTaken` l'errore resta terminale: non sappiamo se la password data
    /// corrisponde davvero a quell'utente, quindi ricadere su `Login` sarebbe un tentativo
    /// alla cieca sulle credenziali di qualcun altro.
    #[tokio::test]
    async fn un_register_rifiutato_resta_terminale_e_non_ricade_su_login() {
        let finto = ServerFinto::avvia().await;
        let config = ClientConfig {
            register: true,
            ..config(finto.indirizzo)
        };
        let mut handle = avvia(config, tempi_di_riconnessione());

        let mut connessione = finto.accetta().await;
        assert!(matches!(
            connessione
                .rifiuta_login(ErrorCode::UsernameTaken, "nome utente gia' in uso")
                .await,
            ClientMessage::Register { .. }
        ));

        assert_eq!(
            prossimo_evento(&mut handle).await,
            ClientEvent::Error("nome utente gia' in uso".to_owned()),
            "le credenziali rifiutate sono un errore, non una disconnessione"
        );
        assert!(
            finto
                .nessuna_connessione_entro(Duration::from_millis(500))
                .await,
            "dopo UsernameTaken il client non deve ritentare, ne' con Register ne' con Login"
        );
        assert_eq!(
            timeout(ATTESA, handle.event_rx.recv())
                .await
                .expect("il task deve essere gia' terminato"),
            None,
            "il canale degli eventi si chiude con il task"
        );
    }

    #[tokio::test]
    async fn la_sessione_sostituita_non_fa_ripartire_la_riconnessione() {
        let finto = ServerFinto::avvia().await;
        let mut handle = avvia(config(finto.indirizzo), tempi_di_riconnessione());

        let mut connessione = finto.accetta().await;
        connessione.accetta_login().await;
        assert_eq!(prossimo_evento(&mut handle).await, ClientEvent::Connected);

        // Il server non chiude di brutto: annuncia la sostituzione, come fa il server vero
        // prima di lasciar cadere il canale della connessione precedente.
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

        // Il punto del test: a differenza di una caduta qualunque, qui nessun nuovo
        // handshake deve arrivare. Riconnettersi espellerebbe la connessione che ha preso
        // il posto di questa, che a sua volta si riconnetterebbe, e cosi' via.
        assert!(
            finto
                .nessuna_connessione_entro(Duration::from_millis(500))
                .await,
            "dopo una sostituzione di sessione il client non deve ritentare da solo"
        );
        assert_eq!(
            timeout(ATTESA, handle.event_rx.recv())
                .await
                .expect("il task deve essere gia' terminato"),
            None
        );
    }

    #[tokio::test]
    async fn le_posizioni_accumulate_durante_la_disconnessione_arrivano_in_ordine() {
        let finto = ServerFinto::avvia().await;
        let indirizzo = finto.indirizzo;
        let mut handle = avvia(config_con_random(indirizzo), tempi_di_prova());

        let mut connessione = finto.accetta().await;
        connessione.accetta_login().await;
        assert_eq!(prossimo_evento(&mut handle).await, ClientEvent::Connected);

        // Chiude anche il banco: i tentativi di riconnessione devono fallire per restare
        // nella fase di attesa, dove il ticker continua a interrogare la sorgente.
        drop(connessione);
        drop(finto);
        assert!(matches!(
            prossimo_evento(&mut handle).await,
            ClientEvent::Disconnected(_)
        ));

        // Con un intervallo di prova di 80 ms, 250 ms bastano ad accumulare piu' di una
        // posizione mentre i tentativi di riconnessione falliscono.
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

        // Drena gli eventuali `Disconnected` dei tentativi falliti nel frattempo.
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
            "le posizioni accumulate devono restare distinte e nell'ordine di generazione"
        );
    }

    #[tokio::test]
    async fn chiudi_manda_logout_e_attende_la_fine_del_task() {
        let finto = ServerFinto::avvia().await;
        let mut handle = avvia(config(finto.indirizzo), Tempi::default());

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
        // Nessuno in ascolto sull'indirizzo: il task resta nel ciclo di riconnessione, dove
        // la chiusura deve funzionare esattamente come a connessione aperta.
        let indirizzo: SocketAddr = "127.0.0.1:1".parse().expect("indirizzo di test valido");
        let handle = avvia(config(indirizzo), tempi_di_prova());

        assert!(
            handle.chiudi().await,
            "anche senza connessione il task deve chiudersi entro l'attesa"
        );
    }

    #[tokio::test]
    async fn una_sorgente_esaurita_manda_ping_invece_di_restare_muta() {
        // Un percorso di un solo punto, senza ripetizione: dopo il primo invio la sorgente
        // e' esaurita e non produrra' mai piu' una posizione.
        let file = FileDiProva::nuovo("esaurita", "lat,lon,offset_secs\n45.07,7.68,0\n");
        let finto = ServerFinto::avvia().await;
        let config = ClientConfig {
            source: SourceKind::File {
                percorso: file.percorso.clone(),
                loop_forever: false,
            },
            ..config(finto.indirizzo)
        };
        let mut handle = avvia(config, tempi_di_prova());

        let mut connessione = finto.accetta().await;
        connessione.accetta_login().await;
        assert_eq!(prossimo_evento(&mut handle).await, ClientEvent::Connected);

        match connessione.ricevi().await {
            ClientMessage::Position { .. } => {}
            altro => panic!("atteso il punto del percorso, ottenuto {altro:?}"),
        }

        // Da qui la sorgente restituisce sempre `None`: senza il `Ping` la connessione
        // resterebbe muta fino alla scadenza di inattivita' del server.
        for numero in 1..=2 {
            match connessione.ricevi().await {
                ClientMessage::Ping => {}
                altro => panic!("atteso il Ping numero {numero}, ottenuto {altro:?}"),
            }
        }
    }
}
