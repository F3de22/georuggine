//! Livello di rete del server: ascolto, ciclo di vita delle connessioni e sessioni.
//!
//! Ogni connessione e' divisa in due meta': il gestore legge i messaggi del client, mentre
//! le scritture passano da un task dedicato che consuma una coda mpsc.
//!
//! ```text
//!            +-- FramedRead <ClientCodec> --> gestore
//! TcpStream -+
//!            +-- FramedWrite<ServerCodec> <-- task di scrittura <-- mpsc
//! ```
//!
//! Dopo l'autenticazione il mittente della coda appartiene al [`ConnectionRegistry`] e il
//! gestore ne tiene solo un riferimento debole: togliere o sostituire la voce nel registro
//! chiude la connessione. Alla sostituzione il client precedente riceve prima
//! [`SESSION_REPLACED_MESSAGE`], per non riconnettersi.
//!
//! Il ritorno a fermo dipende solo dal tempo e lo gestisce `batti_gli_stati`.

use std::collections::HashMap;
use std::io;
use std::net::{AddrParseError, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use futures_util::{SinkExt as _, StreamExt as _};
use georuggine_common::clock::Clock;
use georuggine_common::codec::{ClientCodec, ServerCodec};
use georuggine_common::protocol::{
    AckKind, AnalysisRange, ClientMessage, ErrorCode, PROTOCOL_VERSION, SESSION_REPLACED_MESSAGE,
    ServerMessage,
};
use georuggine_common::types::{Coordinate, PositionRecord, UserId, UserState, Username};
use thiserror::Error;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{RwLock, broadcast, mpsc};
use tokio::task::{self, JoinHandle};
use tokio::time::timeout;
use tokio_util::codec::{FramedRead, FramedWrite};

use crate::analysis::{self, AnalysisRangeExt as _};
use crate::auth::{self, AuthConfig, AuthError, SessionToken};
use crate::messaging::{MessageHub, MessagingError};
use crate::state::StateTracker;
use crate::storage::Repository;

/// Capacita' della coda di scrittura verso un client.
const CAPACITA_CODA: usize = 32;

/// Posizioni accettate per connessione in una finestra di [`FINESTRA_SECS`] secondi.
const MAX_POSIZIONI_PER_FINESTRA: u32 = 10;

/// Durata in secondi della finestra su cui si conta [`MAX_POSIZIONI_PER_FINESTRA`].
const FINESTRA_SECS: i64 = 60;

/// Anticipo massimo tollerato su `recorded_at`, in secondi, per la deriva dell'orologio
/// del client.
const ANTICIPO_MASSIMO_SECS: i64 = 300;

/// Posizioni massime lette da una singola analisi, per limitare la memoria usata.
const MAX_POSIZIONI_ANALIZZATE: usize = 50_000;

/// Indirizzo di ascolto predefinito.
const BIND_PREDEFINITO: &str = "127.0.0.1:7878";

/// File di persistenza predefinito.
const DB_PREDEFINITO: &str = "georuggine.db";

/// Cartella predefinita dei log, relativa alla directory di lavoro.
const LOG_DIR_PREDEFINITA: &str = "logs";

/// Testo mostrato da `--help`.
pub const USO: &str = "\
Uso: georuggine-server [OPZIONI]

Opzioni:
  --bind <INDIRIZZO>    indirizzo e porta di ascolto (predefinito 127.0.0.1:7878)
  --db <PERCORSO>       file SQLite della persistenza (predefinito georuggine.db)
  --log-dir <PERCORSO>  cartella dei log del tempo di CPU (predefinito logs)
  --help                mostra questo messaggio ed esce
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
    /// Il valore di `--bind` non e' un indirizzo con porta.
    #[error("indirizzo di ascolto non valido {indirizzo:?}: {causa}")]
    IndirizzoNonValido {
        /// Testo che non si e' potuto interpretare.
        indirizzo: String,
        /// Motivo riportato dalla libreria standard.
        causa: AddrParseError,
    },
}

/// Configurazione del server.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerConfig {
    /// Indirizzo e porta su cui accettare connessioni.
    pub bind_addr: SocketAddr,
    /// File SQLite in cui persistere utenti, posizioni e messaggi.
    pub db_path: PathBuf,
    /// Cartella dei log del tempo di CPU.
    pub log_dir: PathBuf,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            // Costante valida: il ripiego evita comunque un panico.
            bind_addr: BIND_PREDEFINITO
                .parse()
                .unwrap_or_else(|_| SocketAddr::from(([127, 0, 0, 1], 7878))),
            db_path: PathBuf::from(DB_PREDEFINITO),
            log_dir: PathBuf::from(LOG_DIR_PREDEFINITA),
        }
    }
}

/// Esito dell'analisi degli argomenti; `--help` viene gestito dal chiamante.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Avvio {
    /// Il server deve partire con questa configurazione.
    Configurato(ServerConfig),
    /// E' stato chiesto `--help`: va stampato [`USO`] e si esce senza errore.
    Uso,
}

impl ServerConfig {
    /// Analizza gli argomenti, senza il nome del programma.
    ///
    /// # Errori
    ///
    /// [`ArgsError`] per opzioni sconosciute, valori mancanti o indirizzo non valido.
    pub fn from_args<I>(argomenti: I) -> Result<Avvio, ArgsError>
    where
        I: IntoIterator<Item = String>,
    {
        let mut configurazione = Self::default();
        let mut voci = argomenti.into_iter();

        while let Some(voce) = voci.next() {
            match voce.as_str() {
                "--help" => return Ok(Avvio::Uso),
                "--bind" => {
                    let valore = voci.next().ok_or(ArgsError::ValoreMancante("--bind"))?;
                    configurazione.bind_addr =
                        valore
                            .parse()
                            .map_err(|causa| ArgsError::IndirizzoNonValido {
                                indirizzo: valore,
                                causa,
                            })?;
                }
                "--db" => {
                    let valore = voci.next().ok_or(ArgsError::ValoreMancante("--db"))?;
                    configurazione.db_path = PathBuf::from(valore);
                }
                "--log-dir" => {
                    let valore = voci.next().ok_or(ArgsError::ValoreMancante("--log-dir"))?;
                    configurazione.log_dir = PathBuf::from(valore);
                }
                altro => return Err(ArgsError::OpzioneSconosciuta(altro.to_owned())),
            }
        }

        Ok(Avvio::Configurato(configurazione))
    }
}

/// Scadenze del ciclo di vita di una connessione; i test le accorciano.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConnectionTimeouts {
    /// Attesa massima del primo [`ClientMessage::Hello`].
    pub handshake: Duration,
    /// Silenzio massimo del client prima che la connessione venga chiusa.
    pub idle: Duration,
    /// Attesa massima della chiusura ordinata dopo il segnale di spegnimento.
    pub shutdown: Duration,
    /// Intervallo fra due passaggi della macchina a stati.
    pub tick_interval: Duration,
}

impl Default for ConnectionTimeouts {
    fn default() -> Self {
        Self {
            handshake: Duration::from_secs(5),
            idle: Duration::from_secs(90),
            shutdown: Duration::from_secs(5),
            tick_interval: Duration::from_secs(10),
        }
    }
}

/// Parametri con cui [`run`] gestisce le connessioni: costo dell'hashing e scadenze.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Regolazioni {
    /// Parametri di costo dell'hashing delle password.
    pub auth: AuthConfig,
    /// Scadenze del ciclo di vita e battito della macchina a stati.
    pub timeouts: ConnectionTimeouts,
}

/// Risorse condivise passate a [`run`].
pub struct Dipendenze {
    /// Persistenza di utenti, posizioni e messaggi.
    pub repo: Arc<dyn Repository>,
    /// Registro delle connessioni aperte, una per utente.
    pub registry: Arc<ConnectionRegistry>,
    /// Messaggistica, costruita sullo stesso [`registry`](Self::registry).
    pub messaging: Arc<MessageHub>,
    /// Sorgente dell'ora.
    pub clock: Arc<dyn Clock>,
    /// Macchina a stati fermo/movimento.
    pub stati: Arc<StateTracker>,
}

/// Dipendenze condivise da tutte le connessioni.
pub struct ConnectionContext {
    /// Persistenza di utenti, posizioni e messaggi.
    pub repo: Arc<dyn Repository>,
    /// Registro delle connessioni aperte, una per utente.
    pub registry: Arc<ConnectionRegistry>,
    /// Messaggistica.
    pub messaging: Arc<MessageHub>,
    /// Sorgente dell'ora.
    pub clock: Arc<dyn Clock>,
    /// Macchina a stati fermo/movimento.
    pub stati: Arc<StateTracker>,
    /// Parametri di costo dell'hashing delle password.
    pub auth: AuthConfig,
    /// Scadenze del ciclo di vita.
    pub timeouts: ConnectionTimeouts,
}

/// Connessioni aperte, una per utente autenticato.
///
/// Ogni voce e' il mittente della coda di scrittura della connessione: da qui qualunque
/// parte del server puo' inviare un messaggio a un utente. Il lock non viene mai tenuto
/// durante un `.await`.
#[derive(Debug, Default)]
pub struct ConnectionRegistry {
    /// Utente connesso e canale verso la sua connessione.
    connessioni: RwLock<HashMap<UserId, mpsc::Sender<ServerMessage>>>,
}

impl ConnectionRegistry {
    /// Costruisce un registro vuoto.
    pub fn new() -> Self {
        Self::default()
    }

    /// Registra la connessione di un utente e restituisce l'eventuale mittente
    /// precedente: lasciandolo cadere, il chiamante chiude la vecchia connessione.
    pub async fn register(
        &self,
        user_id: UserId,
        sender: mpsc::Sender<ServerMessage>,
    ) -> Option<mpsc::Sender<ServerMessage>> {
        self.connessioni.write().await.insert(user_id, sender)
    }

    /// Toglie la connessione di un utente dal registro, chiudendola.
    pub async fn unregister(&self, user_id: UserId) {
        self.connessioni.write().await.remove(&user_id);
    }

    /// Invia un messaggio a un utente; `false` se non e' connesso.
    pub async fn send_to(&self, user_id: UserId, message: ServerMessage) -> bool {
        // Si clona il mittente e si invia fuori dal lock.
        let destinatario = self.connessioni.read().await.get(&user_id).cloned();

        match destinatario {
            Some(mittente) => mittente.send(message).await.is_ok(),
            None => false,
        }
    }

    /// Invia lo stesso messaggio a tutte le connessioni e restituisce quante lo hanno
    /// accodato. Non attende: chi ha la coda piena perde il messaggio.
    pub async fn broadcast_all(&self, message: ServerMessage) -> usize {
        let destinatari: Vec<mpsc::Sender<ServerMessage>> =
            self.connessioni.read().await.values().cloned().collect();

        destinatari
            .iter()
            .filter(|mittente| mittente.try_send(message.clone()).is_ok())
            .count()
    }

    /// Utenti connessi, in ordine di id.
    pub async fn connected_users(&self) -> Vec<UserId> {
        let mut utenti: Vec<UserId> = self.connessioni.read().await.keys().copied().collect();
        utenti.sort_unstable();
        utenti
    }

    /// Toglie la voce dell'utente solo se appartiene ancora alla connessione indicata.
    ///
    /// Restituisce `false` se nel frattempo la sessione e' passata a una nuova connessione.
    async fn unregister_se_propria(
        &self,
        user_id: UserId,
        proprio: &mpsc::WeakSender<ServerMessage>,
    ) -> bool {
        let mut connessioni = self.connessioni.write().await;

        let (Some(registrato), Some(nostro)) = (connessioni.get(&user_id), proprio.upgrade())
        else {
            return false;
        };
        if registrato.same_channel(&nostro) {
            connessioni.remove(&user_id);
            true
        } else {
            false
        }
    }
}

/// Lato di scrittura di una connessione, visto dal gestore.
enum Uscita {
    /// Prima dell'autenticazione il gestore possiede il canale.
    Propria(mpsc::Sender<ServerMessage>),
    /// Dopo l'autenticazione il canale appartiene al registro e qui resta un riferimento
    /// debole.
    NelRegistro(mpsc::WeakSender<ServerMessage>),
}

impl Uscita {
    /// Accoda un messaggio verso il client; falso se la connessione non e' piu' scrivibile.
    async fn invia(&self, messaggio: ServerMessage) -> bool {
        // Il mittente ottenuto da `upgrade` vive solo per la durata dell'invio, per non
        // impedire la chiusura.
        let mittente = match self {
            Self::Propria(mittente) => Some(mittente.clone()),
            Self::NelRegistro(debole) => debole.upgrade(),
        };

        match mittente {
            Some(mittente) => mittente.send(messaggio).await.is_ok(),
            None => false,
        }
    }

    /// Riferimento debole al canale, presente solo dopo l'autenticazione.
    fn debole(&self) -> Option<&mpsc::WeakSender<ServerMessage>> {
        match self {
            Self::Propria(_) => None,
            Self::NelRegistro(debole) => Some(debole),
        }
    }
}

/// Limite a finestra fissa sulle posizioni accettate da una connessione.
struct Limitatore {
    /// Istante di inizio della finestra corrente.
    inizio: DateTime<Utc>,
    /// Posizioni gia' conteggiate in questa finestra.
    conteggio: u32,
}

impl Limitatore {
    /// Apre la prima finestra all'istante indicato.
    fn nuovo(adesso: DateTime<Utc>) -> Self {
        Self {
            inizio: adesso,
            conteggio: 0,
        }
    }

    /// Conta una posizione e dice se rientra nel limite; conta anche quelle poi rifiutate.
    fn consenti(&mut self, adesso: DateTime<Utc>) -> bool {
        if adesso.signed_duration_since(self.inizio).num_seconds() >= FINESTRA_SECS {
            self.inizio = adesso;
            self.conteggio = 0;
        }

        if self.conteggio < MAX_POSIZIONI_PER_FINESTRA {
            self.conteggio += 1;
            true
        } else {
            false
        }
    }
}

/// Stato della singola connessione.
struct StatoConnessione {
    /// Utente autenticato, presente solo dopo un accesso riuscito.
    utente: Option<UserId>,
    /// Username dell'utente autenticato, per i messaggi del log.
    nome: Option<Username>,
    /// Limite di frequenza delle posizioni di questa connessione.
    limitatore: Limitatore,
}

impl StatoConnessione {
    /// Stato di una connessione appena aperta, senza sessione.
    fn nuovo(adesso: DateTime<Utc>) -> Self {
        Self {
            utente: None,
            nome: None,
            limitatore: Limitatore::nuovo(adesso),
        }
    }
}

/// Accetta connessioni finche' non arriva il segnale di spegnimento.
///
/// # Errori
///
/// [`io::Error`] se non si riesce ad ascoltare su `config.bind_addr`; gli errori di una
/// singola `accept` vengono registrati e ignorati.
pub async fn run(
    config: ServerConfig,
    dipendenze: Dipendenze,
    regolazioni: Regolazioni,
    shutdown: broadcast::Sender<()>,
) -> io::Result<()> {
    let listener = TcpListener::bind(config.bind_addr).await?;
    let indirizzo = listener.local_addr()?;
    tracing::info!(%indirizzo, "server in ascolto");

    let Dipendenze {
        repo,
        registry,
        messaging,
        clock,
        stati,
    } = dipendenze;
    let contesto = Arc::new(ConnectionContext {
        repo,
        registry,
        messaging,
        clock,
        stati,
        auth: regolazioni.auth,
        timeouts: regolazioni.timeouts,
    });
    let mut spegnimento = shutdown.subscribe();

    tokio::spawn(batti_gli_stati(Arc::clone(&contesto), shutdown.subscribe()));

    loop {
        tokio::select! {
            _ = spegnimento.recv() => {
                tracing::info!("spegnimento richiesto: il server smette di accettare");
                break;
            }
            accettata = listener.accept() => {
                match accettata {
                    Ok((stream, peer)) => {
                        tokio::spawn(handle_connection(
                            stream,
                            peer,
                            Arc::clone(&contesto),
                            shutdown.subscribe(),
                        ));
                    }
                    Err(errore) => {
                        tracing::warn!(%errore, "connessione non accettata");
                    }
                }
            }
        }
    }

    Ok(())
}

/// Fa avanzare la macchina a stati a intervalli regolari e notifica i cambi agli utenti.
async fn batti_gli_stati(contesto: Arc<ConnectionContext>, mut shutdown: broadcast::Receiver<()>) {
    let mut battito = tokio::time::interval(contesto.timeouts.tick_interval);
    // Si scarta la prima scadenza, che matura subito.
    battito.tick().await;

    loop {
        tokio::select! {
            _ = shutdown.recv() => {
                tracing::info!("spegnimento: la macchina a stati smette di battere");
                return;
            }
            _ = battito.tick() => {
                for (user_id, stato) in contesto.stati.tick() {
                    let recapitato = contesto
                        .registry
                        .send_to(user_id, ServerMessage::StateChanged { state: stato })
                        .await;
                    if !recapitato {
                        tracing::debug!(
                            utente = user_id.0,
                            "cambio di stato non recapitato: l'utente non e' piu' connesso"
                        );
                    }
                }
            }
        }
    }
}

/// Gestisce una connessione dall'apertura alla chiusura; gli errori finiscono nel log.
async fn handle_connection(
    stream: TcpStream,
    peer: SocketAddr,
    contesto: Arc<ConnectionContext>,
    mut shutdown: broadcast::Receiver<()>,
) {
    tracing::info!(%peer, "connessione aperta");

    let (lettura, scrittura) = stream.into_split();
    let mut ingresso = FramedRead::new(lettura, ClientCodec::new());
    let (mittente, coda) = mpsc::channel(CAPACITA_CODA);
    let mut scrittore = tokio::spawn(task_di_scrittura(
        FramedWrite::new(scrittura, ServerCodec::new()),
        coda,
    ));

    let mut uscita = Uscita::Propria(mittente);
    let mut stato = StatoConnessione::nuovo(contesto.clock.now());

    // Se l'handshake fallisce il task di scrittura e' ancora attivo.
    let scrittore_a_fine_dialogo = if handshake(&mut ingresso, &uscita, &contesto, peer).await {
        dialogo(
            &mut ingresso,
            &mut uscita,
            &mut stato,
            &contesto,
            &mut shutdown,
            &mut scrittore,
            peer,
        )
        .await
    } else {
        StatoScrittore::Vivo
    };

    // La sessione si sgancia qualunque sia il motivo della chiusura.
    chiudi_sessione(&mut stato.utente, &uscita, &contesto).await;

    // Se il task di scrittura e' ancora attivo, si lascia cadere il mittente e lo si
    // attende entro la scadenza, cosi' svuota la coda e chiude il socket. Se e' gia'
    // terminato, il suo `JoinHandle` e' stato consumato in `dialogo` e non va riletto.
    drop(uscita);
    match scrittore_a_fine_dialogo {
        StatoScrittore::Concluso => {}
        StatoScrittore::Vivo => {
            if timeout(contesto.timeouts.shutdown, &mut scrittore)
                .await
                .is_err()
            {
                tracing::warn!(%peer, "chiusura non completata entro la scadenza");
                scrittore.abort();
            }
        }
    }

    tracing::info!(%peer, "connessione chiusa");
}

/// Scrive sul socket tutto cio' che arriva in coda, poi lo chiude.
async fn task_di_scrittura(
    mut uscita: FramedWrite<OwnedWriteHalf, ServerCodec>,
    mut coda: mpsc::Receiver<ServerMessage>,
) {
    while let Some(messaggio) = coda.recv().await {
        if uscita.send(messaggio).await.is_err() {
            // Il peer non e' piu' raggiungibile.
            break;
        }
    }

    // `close` chiude il verso di scrittura del socket: il client vede subito la fine del
    // flusso.
    let _ = uscita.close().await;
}

/// Attende e valuta il primo messaggio; vero se il dialogo puo' proseguire.
async fn handshake(
    ingresso: &mut FramedRead<OwnedReadHalf, ClientCodec>,
    uscita: &Uscita,
    contesto: &ConnectionContext,
    peer: SocketAddr,
) -> bool {
    let primo = match timeout(contesto.timeouts.handshake, ingresso.next()).await {
        Err(_) => {
            tracing::warn!(%peer, "handshake non arrivato entro la scadenza");
            return false;
        }
        Ok(None) => return false,
        Ok(Some(Err(errore))) => {
            tracing::warn!(%peer, %errore, "frame non valido durante l'handshake");
            return false;
        }
        Ok(Some(Ok(messaggio))) => messaggio,
    };

    match primo {
        ClientMessage::Hello { protocol_version } if protocol_version == PROTOCOL_VERSION => {
            uscita
                .invia(ServerMessage::Welcome {
                    protocol_version: PROTOCOL_VERSION,
                })
                .await
        }
        ClientMessage::Hello { protocol_version } => {
            tracing::warn!(%peer, protocol_version, "versione di protocollo rifiutata");
            uscita
                .invia(ServerMessage::Error {
                    code: ErrorCode::UnsupportedVersion,
                    message: format!(
                        "il client parla la versione {protocol_version}, il server la \
                         {PROTOCOL_VERSION}"
                    ),
                })
                .await;
            false
        }
        _ => {
            // Il primo messaggio deve essere Hello.
            tracing::warn!(%peer, "primo messaggio diverso da Hello");
            uscita
                .invia(ServerMessage::Error {
                    code: ErrorCode::NotAuthenticated,
                    message: "la connessione si apre con Hello".to_owned(),
                })
                .await;
            false
        }
    }
}

/// Stato del task di scrittura alla fine del dialogo: un `JoinHandle` gia' risolto non
/// va riletto, altrimenti tokio va in panico.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StatoScrittore {
    /// Il task e' ancora attivo e va atteso.
    Vivo,
    /// Il task e' terminato e il suo `JoinHandle` e' gia' stato consumato.
    Concluso,
}

/// Serve i messaggi del client finche' la connessione resta aperta; restituisce lo stato
/// del task di scrittura.
async fn dialogo(
    ingresso: &mut FramedRead<OwnedReadHalf, ClientCodec>,
    uscita: &mut Uscita,
    stato: &mut StatoConnessione,
    contesto: &ConnectionContext,
    shutdown: &mut broadcast::Receiver<()>,
    scrittore: &mut JoinHandle<()>,
    peer: SocketAddr,
) -> StatoScrittore {
    loop {
        tokio::select! {
            _ = shutdown.recv() => {
                tracing::info!(%peer, "spegnimento: chiusura della connessione");
                return StatoScrittore::Vivo;
            }
            _ = &mut *scrittore => {
                // Il task di scrittura e' terminato: peer irraggiungibile o sessione
                // sostituita.
                tracing::info!(%peer, "canale di scrittura chiuso");
                return StatoScrittore::Concluso;
            }
            ricevuto = timeout(contesto.timeouts.idle, ingresso.next()) => {
                let messaggio = match ricevuto {
                    Err(_) => {
                        tracing::info!(%peer, "nessun messaggio entro la scadenza di inattivita'");
                        return StatoScrittore::Vivo;
                    }
                    Ok(None) => return StatoScrittore::Vivo,
                    Ok(Some(Err(errore))) => {
                        // Un frame non valido disallinea il flusso: si chiude.
                        tracing::warn!(%peer, %errore, "frame non valido: chiusura");
                        return StatoScrittore::Vivo;
                    }
                    Ok(Some(Ok(messaggio))) => messaggio,
                };

                if !servi(messaggio, uscita, stato, contesto, peer).await {
                    return StatoScrittore::Vivo;
                }
            }
        }
    }
}

/// Serve un messaggio del client; vero se la connessione resta aperta.
async fn servi(
    messaggio: ClientMessage,
    uscita: &mut Uscita,
    stato: &mut StatoConnessione,
    contesto: &ConnectionContext,
    peer: SocketAddr,
) -> bool {
    let messaggio = match (stato.utente, messaggio) {
        (None, ClientMessage::Register { username, password }) => {
            let esito = auth::register(&contesto.repo, &contesto.auth, &username, &password).await;
            let aperta = concludi_autenticazione(
                esito,
                &username,
                uscita,
                &mut stato.utente,
                contesto,
                peer,
            )
            .await;
            if stato.utente.is_some() {
                stato.nome = Some(username);
            }
            return aperta;
        }
        (None, ClientMessage::Login { username, password }) => {
            let esito = auth::login(&contesto.repo, &username, &password).await;
            let aperta = concludi_autenticazione(
                esito,
                &username,
                uscita,
                &mut stato.utente,
                contesto,
                peer,
            )
            .await;
            if stato.utente.is_some() {
                stato.nome = Some(username);
            }
            return aperta;
        }
        (None, _) => {
            return uscita
                .invia(ServerMessage::Error {
                    code: ErrorCode::NotAuthenticated,
                    message: "serve una sessione aperta: usa Register o Login".to_owned(),
                })
                .await;
        }
        (Some(_), messaggio) => messaggio,
    };

    let Some(user_id) = stato.utente else {
        return true;
    };

    match messaggio {
        ClientMessage::Ping => uscita.invia(ServerMessage::Pong).await,
        ClientMessage::Logout => {
            // L'Ack si accoda prima dello sgancio, quindi parte prima della chiusura.
            uscita
                .invia(ServerMessage::Ack {
                    request: AckKind::Logout,
                })
                .await;
            chiudi_sessione(&mut stato.utente, uscita, contesto).await;
            tracing::info!(%peer, "logout richiesto dal client");
            false
        }
        ClientMessage::Position {
            coordinate,
            recorded_at,
        } => {
            servi_posizione(
                user_id,
                coordinate,
                recorded_at,
                uscita,
                &mut stato.limitatore,
                contesto,
                peer,
            )
            .await
        }
        ClientMessage::RequestAnalysis { range } => {
            servi_richiesta_analisi(user_id, range, uscita, contesto, peer).await
        }
        ClientMessage::TextToServer { body } => {
            servi_testo(user_id, stato.nome.as_ref(), &body, uscita, contesto, peer).await
        }
        // Hello, Register o Login a sessione gia' aperta: fuori sequenza.
        _ => {
            uscita
                .invia(ServerMessage::Error {
                    code: ErrorCode::NotAuthenticated,
                    message: "richiesta fuori sequenza".to_owned(),
                })
                .await
        }
    }
}

/// Salva una posizione e la conferma al client; `false` se la connessione va chiusa.
///
/// Limite di frequenza e istante nel futuro sono errori della singola richiesta e non
/// chiudono la connessione.
async fn servi_posizione(
    user_id: UserId,
    coordinate: Coordinate,
    recorded_at: DateTime<Utc>,
    uscita: &Uscita,
    limitatore: &mut Limitatore,
    contesto: &ConnectionContext,
    peer: SocketAddr,
) -> bool {
    let adesso = contesto.clock.now();

    if !limitatore.consenti(adesso) {
        tracing::warn!(%peer, utente = user_id.0, "limite di posizioni superato");
        return uscita
            .invia(ServerMessage::Error {
                code: ErrorCode::RateLimited,
                message: format!(
                    "al massimo {MAX_POSIZIONI_PER_FINESTRA} posizioni ogni {FINESTRA_SECS} secondi"
                ),
            })
            .await;
    }

    // Una posizione datata nel futuro falserebbe le analisi.
    if recorded_at.signed_duration_since(adesso).num_seconds() > ANTICIPO_MASSIMO_SECS {
        tracing::warn!(%peer, utente = user_id.0, %recorded_at, "posizione datata troppo avanti");
        return uscita
            .invia(ServerMessage::Error {
                code: ErrorCode::MalformedMessage,
                message: format!(
                    "recorded_at e' piu' di {} minuti avanti rispetto all'ora del server",
                    ANTICIPO_MASSIMO_SECS / 60
                ),
            })
            .await;
    }

    let record = PositionRecord {
        user_id,
        coordinate,
        recorded_at,
    };
    let repo = Arc::clone(&contesto.repo);
    let esito = task::spawn_blocking(move || repo.insert_position(&record)).await;

    match esito {
        Ok(Ok(())) => {}
        Ok(Err(errore)) => {
            // Al client va solo il codice, non i dettagli del database.
            tracing::error!(%peer, utente = user_id.0, %errore, "posizione non persistita");
            return errore_interno(uscita).await;
        }
        Err(errore) => {
            tracing::error!(%peer, %errore, "attivita' di scrittura non completata");
            return errore_interno(uscita).await;
        }
    }

    if !uscita
        .invia(ServerMessage::Ack {
            request: AckKind::Position,
        })
        .await
    {
        return false;
    }

    // La macchina a stati vede solo le posizioni salvate, dopo l'Ack.
    match contesto.stati.on_position(user_id, coordinate) {
        Some(state) => uscita.invia(ServerMessage::StateChanged { state }).await,
        None => true,
    }
}

/// Salva un testo inviato dal client, lo registra nel log del server e risponde con un
/// `Ack`; `false` se la connessione va chiusa.
async fn servi_testo(
    user_id: UserId,
    nome: Option<&Username>,
    body: &str,
    uscita: &Uscita,
    contesto: &ConnectionContext,
    peer: SocketAddr,
) -> bool {
    match contesto.messaging.from_user(user_id, body).await {
        Ok(()) => {
            // Senza username si mostra l'id.
            let mittente =
                nome.map_or_else(|| format!("#{}", user_id.0), |n| n.as_str().to_owned());
            tracing::info!(%peer, utente = %mittente, testo = body, "messaggio ricevuto da un veicolo");
            uscita
                .invia(ServerMessage::Ack {
                    request: AckKind::TextToServer,
                })
                .await
        }
        Err(errore @ MessagingError::TooLong { .. }) => {
            tracing::warn!(%peer, utente = user_id.0, %errore, "testo rifiutato");
            uscita
                .invia(ServerMessage::Error {
                    code: ErrorCode::MalformedMessage,
                    message: errore.to_string(),
                })
                .await
        }
        Err(errore) => {
            tracing::error!(%peer, utente = user_id.0, %errore, "testo non persistito");
            errore_interno(uscita).await
        }
    }
}

/// Consegna i messaggi diretti arrivati mentre l'utente era scollegato; `false` se la
/// connessione va chiusa.
///
/// L'invio passa da `uscita` e non dal registro, dove la voce potrebbe gia' appartenere a
/// un'altra connessione. Un errore di lettura o di marcatura non chiude la sessione.
async fn consegna_messaggi_in_attesa(
    user_id: UserId,
    uscita: &Uscita,
    contesto: &ConnectionContext,
    peer: SocketAddr,
) -> bool {
    let repo = Arc::clone(&contesto.repo);
    let in_attesa = match task::spawn_blocking(move || repo.pending_messages(user_id)).await {
        Ok(Ok(messaggi)) => messaggi,
        Ok(Err(errore)) => {
            tracing::error!(%peer, utente = user_id.0, %errore, "messaggi in attesa non letti");
            return true;
        }
        Err(errore) => {
            tracing::error!(%peer, %errore, "attivita' di lettura non completata");
            return true;
        }
    };

    if in_attesa.is_empty() {
        return true;
    }

    for messaggio in &in_attesa {
        if !uscita
            .invia(ServerMessage::Direct {
                body: messaggio.body.clone(),
                sent_at: messaggio.sent_at,
            })
            .await
        {
            // Connessione chiusa a meta': i messaggi restano in attesa.
            tracing::warn!(%peer, utente = user_id.0, "consegna interrotta: connessione chiusa");
            return false;
        }
    }

    // La marcatura avviene solo dopo l'invio riuscito.
    let repo = Arc::clone(&contesto.repo);
    match task::spawn_blocking(move || repo.clear_pending_messages(user_id)).await {
        Ok(Ok(())) => {
            tracing::info!(
                %peer,
                utente = user_id.0,
                quanti = in_attesa.len(),
                "messaggi in attesa consegnati"
            );
        }
        Ok(Err(errore)) => {
            tracing::error!(%peer, utente = user_id.0, %errore, "messaggi non marcati");
        }
        Err(errore) => {
            tracing::error!(%peer, %errore, "attivita' di marcatura non completata");
        }
    }

    true
}

/// Calcola il riepilogo dei movimenti richiesto; `false` se la connessione va chiusa.
///
/// Un intervallo senza posizioni produce un riepilogo a zero, non un errore.
async fn servi_richiesta_analisi(
    user_id: UserId,
    range: AnalysisRange,
    uscita: &Uscita,
    contesto: &ConnectionContext,
    peer: SocketAddr,
) -> bool {
    let (from, to) = range.to_bounds(contesto.clock.now());

    // Solo un intervallo Custom puo' avere gli estremi invertiti.
    if from > to {
        tracing::warn!(%peer, utente = user_id.0, %from, %to, "intervallo di analisi rovesciato");
        return uscita
            .invia(ServerMessage::Error {
                code: ErrorCode::MalformedMessage,
                message: "l'inizio dell'intervallo richiesto viene dopo la fine".to_owned(),
            })
            .await;
    }

    let repo = Arc::clone(&contesto.repo);
    let esito = task::spawn_blocking(move || repo.positions_in_range(user_id, from, to)).await;

    let lette = match esito {
        Ok(Ok(posizioni)) => posizioni,
        Ok(Err(errore)) => {
            tracing::error!(%peer, utente = user_id.0, %errore, "posizioni non lette");
            return errore_interno(uscita).await;
        }
        Err(errore) => {
            tracing::error!(%peer, %errore, "attivita' di lettura non completata");
            return errore_interno(uscita).await;
        }
    };

    let (posizioni, troncate_da) = tronca_alle_piu_recenti(lette, MAX_POSIZIONI_ANALIZZATE);
    if let Some(totale) = troncate_da {
        tracing::warn!(
            %peer,
            utente = user_id.0,
            totale,
            analizzate = MAX_POSIZIONI_ANALIZZATE,
            "posizioni oltre il limite: analizzate solo le piu' recenti"
        );
    }

    uscita
        .invia(ServerMessage::AnalysisReport(analysis::analyze(&posizioni)))
        .await
}

/// Invia al client solo il codice di errore interno, senza dettagli.
async fn errore_interno(uscita: &Uscita) -> bool {
    uscita
        .invia(ServerMessage::Error {
            code: ErrorCode::Internal,
            message: ErrorCode::Internal.to_string(),
        })
        .await
}

/// Tiene le ultime `massimo` posizioni e, se ha troncato, restituisce il totale.
fn tronca_alle_piu_recenti(
    mut posizioni: Vec<PositionRecord>,
    massimo: usize,
) -> (Vec<PositionRecord>, Option<usize>) {
    let totale = posizioni.len();
    if totale <= massimo {
        return (posizioni, None);
    }

    posizioni.drain(..totale - massimo);
    (posizioni, Some(totale))
}

/// Traduce l'esito dell'autenticazione in una risposta al client.
async fn concludi_autenticazione(
    esito: Result<UserId, AuthError>,
    username: &Username,
    uscita: &mut Uscita,
    utente: &mut Option<UserId>,
    contesto: &ConnectionContext,
    peer: SocketAddr,
) -> bool {
    let user_id = match esito {
        Ok(user_id) => user_id,
        Err(errore) => {
            let (code, message) = risposta_a(&errore);
            // Nel log va lo username, mai la password.
            tracing::warn!(%peer, utente = username.as_str(), %errore, "autenticazione fallita");
            return uscita.invia(ServerMessage::Error { code, message }).await;
        }
    };

    let token = match SessionToken::generate() {
        Ok(token) => token,
        Err(errore) => {
            tracing::error!(%peer, %errore, "generazione del token di sessione fallita");
            return errore_interno(uscita).await;
        }
    };

    // Da qui il canale appartiene al registro; il gestore tiene un riferimento debole.
    let (debole, per_il_registro) = match uscita {
        Uscita::Propria(mittente) => (mittente.downgrade(), mittente.clone()),
        Uscita::NelRegistro(_) => return true,
    };
    let precedente = contesto.registry.register(user_id, per_il_registro).await;
    *uscita = Uscita::NelRegistro(debole);

    if let Some(precedente) = precedente {
        // Una sola sessione per utente: la connessione precedente viene avvisata e poi
        // chiusa lasciando cadere il suo mittente. L'avviso non attende, per non bloccare
        // il nuovo accesso se il vecchio client non legge.
        let avviso = ServerMessage::Error {
            code: ErrorCode::NotAuthenticated,
            message: SESSION_REPLACED_MESSAGE.to_owned(),
        };
        let annunciata = precedente.try_send(avviso).is_ok();
        tracing::info!(
            %peer,
            utente = username.as_str(),
            annunciata,
            "sessione precedente dello stesso utente sostituita"
        );
        drop(precedente);
    }

    *utente = Some(user_id);
    contesto.stati.on_connect(user_id);
    tracing::info!(%peer, utente = username.as_str(), "accesso riuscito");

    // Ordine: token, stato iniziale, messaggi in attesa.
    if !uscita
        .invia(ServerMessage::SessionToken {
            token: token.as_str().to_owned(),
        })
        .await
    {
        return false;
    }
    let stato_iniziale = contesto
        .stati
        .current_state(user_id)
        .unwrap_or(UserState::Still);
    if !uscita
        .invia(ServerMessage::StateChanged {
            state: stato_iniziale,
        })
        .await
    {
        return false;
    }

    consegna_messaggi_in_attesa(user_id, uscita, contesto, peer).await
}

/// Codice e messaggio da inviare al client per un errore di autenticazione; gli errori
/// interni non riportano dettagli.
fn risposta_a(errore: &AuthError) -> (ErrorCode, String) {
    match errore {
        AuthError::WeakPassword(dettaglio) => (
            ErrorCode::InvalidCredentials,
            format!("password troppo debole: {dettaglio}"),
        ),
        AuthError::InvalidCredentials => (
            ErrorCode::InvalidCredentials,
            ErrorCode::InvalidCredentials.to_string(),
        ),
        AuthError::UsernameTaken => (
            ErrorCode::UsernameTaken,
            ErrorCode::UsernameTaken.to_string(),
        ),
        AuthError::Hashing(_) | AuthError::Storage(_) => {
            (ErrorCode::Internal, ErrorCode::Internal.to_string())
        }
    }
}

/// Sgancia la sessione dal registro e dalla macchina a stati; idempotente.
async fn chiudi_sessione(
    utente: &mut Option<UserId>,
    uscita: &Uscita,
    contesto: &ConnectionContext,
) {
    let Some(user_id) = utente.take() else {
        return;
    };

    let propria = match uscita.debole() {
        Some(debole) => {
            contesto
                .registry
                .unregister_se_propria(user_id, debole)
                .await
        }
        None => true,
    };
    // Se la sessione e' stata sostituita, l'utente resta connesso con quella nuova.
    if propria {
        contesto.stati.on_disconnect(user_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use chrono::TimeDelta;
    use georuggine_common::clock::FakeClock;
    use georuggine_common::protocol::MovementSummary;
    use georuggine_common::types::{STILL_AFTER_SECS, UserState, haversine_meters};
    use tokio_util::bytes::Bytes;
    use tokio_util::codec::BytesCodec;

    use crate::messaging::MAX_CARATTERI;
    use crate::storage::{PendingMessage, SqliteRepository};

    /// Attesa massima di una risposta nei test.
    const ATTESA: Duration = Duration::from_secs(5);

    /// Password che rispetta la policy di [`crate::auth::valida_password`].
    const PASSWORD: &str = "scorta12";

    /// Timeout ridotti per i test.
    ///
    /// L'inattivita' resta lunga apposta: quasi tutti i test verificano che la connessione
    /// si chiuda per un motivo preciso (logout, sostituzione, spegnimento), e con una
    /// scadenza breve la chiusura arriverebbe comunque, facendoli passare anche se il
    /// meccanismo sotto esame non funzionasse.
    ///
    /// `tick_interval` e' tempo vero, che nessun orologio finto puo' saltare: e' solo la
    /// frequenza con cui il task si sveglia, e va accorciato. La soglia dei tre minuti e'
    /// invece tempo di dominio, che i test spostano con `FakeClock::advance`.
    fn tempi() -> ConnectionTimeouts {
        ConnectionTimeouts {
            handshake: Duration::from_millis(200),
            idle: Duration::from_secs(30),
            shutdown: Duration::from_millis(200),
            tick_interval: Duration::from_millis(50),
        }
    }

    /// Un secondo oltre la soglia di ritorno a fermo, nel tipo di chrono.
    fn oltre_la_soglia_di_fermo() -> TimeDelta {
        TimeDelta::seconds(i64::try_from(STILL_AFTER_SECS).expect("la soglia entra in i64") + 1)
    }

    /// Parametri di Argon2 al minimo: l'hashing di produzione e' lento apposta.
    fn autenticazione() -> AuthConfig {
        AuthConfig {
            memory_kib: 32,
            iterations: 1,
            parallelism: 1,
        }
    }

    fn utente(nome: &str) -> Username {
        Username::new(nome).expect("username di test valido")
    }

    fn istante_base() -> DateTime<Utc> {
        DateTime::from_timestamp(1_704_067_200, 0).expect("istante valido")
    }

    /// Coordinata associata all'offset in secondi di una posizione di prova.
    ///
    /// Insieme a `recorded_at` fa da impronta del record: se il server persistesse la
    /// posizione sbagliata, o la attribuisse a un altro utente, le coordinate rilette non
    /// tornerebbero. Un millesimo di grado per offset e' oltre un chilometro, quindi ogni
    /// segmento e' inequivocabilmente un movimento.
    fn punto(offset: i64) -> Coordinate {
        Coordinate::new(45.0 + offset as f64 / 1_000.0, 7.6869).expect("coordinata valida")
    }

    /// Server di prova in ascolto su una porta effimera.
    ///
    /// Non usa [`run`] ma un ciclo di accept equivalente, per due motivi: la porta 0 va
    /// risolta in quella davvero assegnata, e il contesto deve restare in mano al test per
    /// poter ispezionare persistenza e connessioni registrate.
    struct Banco {
        /// Indirizzo su cui i client si collegano.
        indirizzo: SocketAddr,
        /// Dipendenze condivise, ispezionabili dal test.
        contesto: Arc<ConnectionContext>,
        /// Orologio del server, che il test puo' spostare a mano.
        orologio: Arc<FakeClock>,
        /// Segnale di spegnimento propagato ai gestori.
        spegnimento: broadcast::Sender<()>,
        /// Task periodico della macchina a stati, che il test attende per verificare che
        /// lo spegnimento lo faccia terminare.
        battito: JoinHandle<()>,
    }

    async fn avvia_banco() -> Banco {
        avvia_banco_con(tempi()).await
    }

    /// Monta un banco con la macchina a stati vera e il suo task periodico.
    ///
    /// Il tracciatore riceve l'orologio del banco, come in produzione riceve quello del
    /// processo: spostando `banco.orologio` il test muove anche il tempo su cui la macchina
    /// a stati misura i tre minuti.
    async fn avvia_banco_con(timeouts: ConnectionTimeouts) -> Banco {
        let orologio = Arc::new(FakeClock::new(istante_base()));
        let sorgente: Arc<dyn Clock> = orologio.clone();
        let repo: Arc<dyn Repository> = Arc::new(
            SqliteRepository::new(":memory:", Arc::clone(&sorgente))
                .expect("apertura in memoria riuscita"),
        );

        let registry = Arc::new(ConnectionRegistry::new());
        let messaging = Arc::new(MessageHub::new(
            Arc::clone(&repo),
            Arc::clone(&registry),
            Arc::clone(&sorgente),
        ));
        let contesto = Arc::new(ConnectionContext {
            repo,
            registry,
            messaging,
            clock: Arc::clone(&sorgente),
            stati: Arc::new(StateTracker::new(sorgente)),
            auth: autenticazione(),
            timeouts,
        });
        let (spegnimento, _) = broadcast::channel(1);

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind sulla porta effimera riuscito");
        let indirizzo = listener.local_addr().expect("indirizzo locale disponibile");

        let battito = tokio::spawn(batti_gli_stati(
            Arc::clone(&contesto),
            spegnimento.subscribe(),
        ));

        let condiviso = Arc::clone(&contesto);
        let segnale = spegnimento.clone();
        tokio::spawn(async move {
            while let Ok((stream, peer)) = listener.accept().await {
                tokio::spawn(handle_connection(
                    stream,
                    peer,
                    Arc::clone(&condiviso),
                    segnale.subscribe(),
                ));
            }
        });

        Banco {
            indirizzo,
            contesto,
            orologio,
            spegnimento,
            battito,
        }
    }

    impl Banco {
        /// Posizioni salvate per l'utente, su un intervallo ampio.
        fn posizioni_di(&self, user_id: UserId) -> Vec<PositionRecord> {
            self.contesto
                .repo
                .positions_in_range(
                    user_id,
                    istante_base() - TimeDelta::days(1),
                    istante_base() + TimeDelta::days(1),
                )
                .expect("lettura delle posizioni riuscita")
        }

        /// Messaggi ancora in attesa di consegna per l'utente.
        fn pendenti_di(&self, user_id: UserId) -> Vec<PendingMessage> {
            self.contesto
                .repo
                .pending_messages(user_id)
                .expect("lettura dei messaggi in attesa riuscita")
        }

        /// Stato che la macchina a stati del banco attribuisce all'utente.
        fn stato_di(&self, user_id: UserId) -> Option<UserState> {
            self.contesto.stati.current_state(user_id)
        }
    }

    /// Client di prova: parla il protocollo con i codec veri.
    struct Cliente {
        scrittura: FramedWrite<OwnedWriteHalf, ClientCodec>,
        lettura: FramedRead<OwnedReadHalf, ServerCodec>,
    }

    impl Cliente {
        async fn connetti(indirizzo: SocketAddr) -> Self {
            let stream = TcpStream::connect(indirizzo)
                .await
                .expect("connessione al banco riuscita");
            let (lettura, scrittura) = stream.into_split();
            Self {
                scrittura: FramedWrite::new(scrittura, ClientCodec::new()),
                lettura: FramedRead::new(lettura, ServerCodec::new()),
            }
        }

        async fn invia(&mut self, messaggio: ClientMessage) {
            self.scrittura
                .send(messaggio)
                .await
                .expect("invio riuscito");
        }

        /// Prossima risposta del server; fa fallire il test se non arriva.
        async fn ricevi(&mut self) -> ServerMessage {
            timeout(ATTESA, self.lettura.next())
                .await
                .expect("il server deve rispondere entro l'attesa")
                .expect("la connessione non deve chiudersi qui")
                .expect("il frame ricevuto deve essere valido")
        }

        /// Vero se la connessione risulta chiusa; un timeout significa che e' ancora aperta.
        async fn e_chiusa(&mut self) -> bool {
            match timeout(ATTESA, self.lettura.next()).await {
                Ok(None) | Ok(Some(Err(_))) => true,
                Ok(Some(Ok(_))) | Err(_) => false,
            }
        }

        /// Esegue l'handshake e restituisce la risposta del server.
        async fn saluta(&mut self) -> ServerMessage {
            self.invia(ClientMessage::Hello {
                protocol_version: PROTOCOL_VERSION,
            })
            .await;
            self.ricevi().await
        }

        /// Manda le credenziali su una connessione che ha gia' salutato.
        async fn autentica(&mut self, nome: &str, registrandosi: bool) {
            let credenziali = if registrandosi {
                ClientMessage::Register {
                    username: utente(nome),
                    password: PASSWORD.to_owned(),
                }
            } else {
                ClientMessage::Login {
                    username: utente(nome),
                    password: PASSWORD.to_owned(),
                }
            };
            self.invia(credenziali).await;
            match self.ricevi().await {
                ServerMessage::SessionToken { .. } => {}
                altro => panic!("atteso SessionToken, ottenuto {altro:?}"),
            }
            // Subito dopo il token il server comunica lo stato iniziale.
            assert_eq!(
                self.ricevi().await,
                ServerMessage::StateChanged {
                    state: UserState::Still
                }
            );
        }

        async fn registra(&mut self, nome: &str) {
            assert!(matches!(self.saluta().await, ServerMessage::Welcome { .. }));
            self.autentica(nome, true).await;
        }

        async fn accedi(&mut self, nome: &str) {
            assert!(matches!(self.saluta().await, ServerMessage::Welcome { .. }));
            self.autentica(nome, false).await;
        }

        /// Invia le posizioni di prova e ne consuma le conferme, compreso l'eventuale
        /// passaggio in movimento dopo la seconda.
        async fn invia_percorso(&mut self, offsets: &[i64]) {
            for (indice, offset) in offsets.iter().enumerate() {
                self.invia(ClientMessage::Position {
                    coordinate: punto(*offset),
                    recorded_at: istante_base() + TimeDelta::seconds(*offset),
                })
                .await;
                assert_eq!(
                    self.ricevi().await,
                    ServerMessage::Ack {
                        request: AckKind::Position
                    }
                );

                if indice == 1 {
                    assert_eq!(
                        self.ricevi().await,
                        ServerMessage::StateChanged {
                            state: UserState::Moving
                        },
                        "il secondo punto dista oltre la soglia: porta in movimento"
                    );
                }
            }
        }

        /// Chiede l'analisi e pretende che la risposta sia un [`MovementSummary`].
        async fn chiedi_analisi(&mut self, range: AnalysisRange) -> MovementSummary {
            self.invia(ClientMessage::RequestAnalysis { range }).await;
            match self.ricevi().await {
                ServerMessage::AnalysisReport(riepilogo) => riepilogo,
                altro => panic!("atteso AnalysisReport, ottenuto {altro:?}"),
            }
        }

        /// Chiude la sessione con un logout ordinato.
        async fn esci(&mut self) {
            self.invia(ClientMessage::Logout).await;
            assert_eq!(
                self.ricevi().await,
                ServerMessage::Ack {
                    request: AckKind::Logout
                }
            );
            assert!(self.e_chiusa().await, "il logout chiude la connessione");
        }

        /// Porta l'utente in [`UserState::Moving`] con due posizioni distanti.
        async fn porta_in_movimento(&mut self) {
            self.invia_percorso(&[0, 1]).await;
        }
    }

    fn argomenti(elenco: &[&str]) -> Vec<String> {
        elenco.iter().map(|voce| (*voce).to_owned()).collect()
    }

    #[test]
    fn senza_argomenti_valgono_i_valori_predefiniti() {
        assert_eq!(
            ServerConfig::from_args(argomenti(&[])).expect("analisi riuscita"),
            Avvio::Configurato(ServerConfig::default())
        );
        assert_eq!(
            ServerConfig::from_args(argomenti(&["--help"])).expect("analisi riuscita"),
            Avvio::Uso
        );
    }

    #[test]
    fn ogni_opzione_sovrascrive_il_proprio_predefinito() {
        let avvio = ServerConfig::from_args(argomenti(&[
            "--bind",
            "0.0.0.0:9000",
            "--db",
            "flotta.db",
            "--log-dir",
            "misure",
        ]))
        .expect("analisi riuscita");

        assert_eq!(
            avvio,
            Avvio::Configurato(ServerConfig {
                bind_addr: "0.0.0.0:9000".parse().expect("indirizzo di test valido"),
                db_path: PathBuf::from("flotta.db"),
                log_dir: PathBuf::from("misure"),
            })
        );
        // Ogni opzione documentata deve comparire in `--help`.
        for opzione in ["--bind", "--db", "--log-dir", "--help"] {
            assert!(USO.contains(opzione), "l'uso non nomina {opzione}");
        }
    }

    #[test]
    fn gli_argomenti_non_validi_sono_errori_non_panici() {
        assert!(matches!(
            ServerConfig::from_args(argomenti(&["--sconosciuta"])),
            Err(ArgsError::OpzioneSconosciuta(_))
        ));
        assert!(matches!(
            ServerConfig::from_args(argomenti(&["--bind"])),
            Err(ArgsError::ValoreMancante(_))
        ));
        assert!(matches!(
            ServerConfig::from_args(argomenti(&["--log-dir"])),
            Err(ArgsError::ValoreMancante(_))
        ));
        assert!(matches!(
            ServerConfig::from_args(argomenti(&["--bind", "non-un-indirizzo"])),
            Err(ArgsError::IndirizzoNonValido { .. })
        ));
    }

    #[tokio::test]
    async fn il_registro_tiene_una_connessione_per_utente_in_ordine_di_id() {
        let registro = ConnectionRegistry::new();
        let (secondo_utente, _coda_secondo) = mpsc::channel(4);
        let (primo_utente, mut coda_primo) = mpsc::channel(4);
        let (sostituto, _coda_sostituto) = mpsc::channel(4);

        assert!(registro.register(UserId(2), secondo_utente).await.is_none());
        assert!(registro.register(UserId(1), primo_utente).await.is_none());
        assert_eq!(registro.connected_users().await, vec![UserId(1), UserId(2)]);

        assert!(
            registro.register(UserId(2), sostituto).await.is_some(),
            "la seconda registrazione deve restituire il canale precedente"
        );

        assert!(registro.send_to(UserId(1), ServerMessage::Pong).await);
        assert_eq!(coda_primo.recv().await, Some(ServerMessage::Pong));
        assert!(
            !registro.send_to(UserId(9), ServerMessage::Pong).await,
            "un utente non registrato non e' raggiungibile"
        );

        registro.unregister(UserId(1)).await;
        assert_eq!(registro.connected_users().await, vec![UserId(2)]);
    }

    #[tokio::test]
    async fn broadcast_all_prosegue_oltre_un_destinatario_irraggiungibile() {
        let registro = ConnectionRegistry::new();
        assert_eq!(registro.broadcast_all(ServerMessage::Pong).await, 0);

        let (primo, mut primo_ricevitore) = mpsc::channel(4);
        // Capacita' uno e un messaggio gia' in coda: canale pieno ma non chiuso.
        let (pieno, _pieno_ricevitore) = mpsc::channel(1);
        pieno
            .send(ServerMessage::Welcome {
                protocol_version: PROTOCOL_VERSION,
            })
            .await
            .expect("la prima accodata riempie la coda");
        let (terzo, mut terzo_ricevitore) = mpsc::channel(4);
        registro.register(UserId(1), primo).await;
        registro.register(UserId(2), pieno).await;
        registro.register(UserId(3), terzo).await;

        assert_eq!(
            registro.broadcast_all(ServerMessage::Pong).await,
            2,
            "il destinatario con la coda piena non va contato"
        );
        assert_eq!(primo_ricevitore.recv().await, Some(ServerMessage::Pong));
        assert_eq!(terzo_ricevitore.recv().await, Some(ServerMessage::Pong));
    }

    #[tokio::test]
    async fn l_handshake_accetta_solo_hello_con_la_versione_giusta() {
        let banco = avvia_banco().await;

        let mut giusto = Cliente::connetti(banco.indirizzo).await;
        assert_eq!(
            giusto.saluta().await,
            ServerMessage::Welcome {
                protocol_version: PROTOCOL_VERSION
            }
        );

        let mut sbagliato = Cliente::connetti(banco.indirizzo).await;
        sbagliato
            .invia(ClientMessage::Hello {
                protocol_version: PROTOCOL_VERSION + 41,
            })
            .await;
        assert!(matches!(
            sbagliato.ricevi().await,
            ServerMessage::Error {
                code: ErrorCode::UnsupportedVersion,
                ..
            }
        ));
        assert!(
            sbagliato.e_chiusa().await,
            "dopo una versione rifiutata la connessione deve chiudersi"
        );

        // Chi non manda nulla viene chiuso allo scadere della scadenza di handshake.
        let mut muto = Cliente::connetti(banco.indirizzo).await;
        assert!(muto.e_chiusa().await);
    }

    #[tokio::test]
    async fn register_apre_la_sessione_e_un_login_successivo_riesce() {
        let banco = avvia_banco().await;

        let mut primo = Cliente::connetti(banco.indirizzo).await;
        primo.registra("veicolo_01").await;
        primo.esci().await;

        let mut secondo = Cliente::connetti(banco.indirizzo).await;
        secondo.accedi("veicolo_01").await;
        secondo.invia(ClientMessage::Ping).await;
        assert_eq!(secondo.ricevi().await, ServerMessage::Pong);
    }

    #[tokio::test]
    async fn le_credenziali_rifiutate_lasciano_la_connessione_utilizzabile() {
        let banco = avvia_banco().await;
        let mut primo = Cliente::connetti(banco.indirizzo).await;
        primo.registra("veicolo_01").await;

        let mut altro = Cliente::connetti(banco.indirizzo).await;
        assert!(matches!(
            altro.saluta().await,
            ServerMessage::Welcome { .. }
        ));

        // Tre rifiuti sulla stessa connessione, che deve restare aperta.
        for (messaggio, atteso) in [
            (
                ClientMessage::Register {
                    username: utente("veicolo_01"),
                    password: PASSWORD.to_owned(),
                },
                ErrorCode::UsernameTaken,
            ),
            (
                ClientMessage::Login {
                    username: utente("veicolo_01"),
                    password: "sbagliata9".to_owned(),
                },
                ErrorCode::InvalidCredentials,
            ),
            (
                ClientMessage::Register {
                    username: utente("veicolo_02"),
                    password: "corta".to_owned(),
                },
                ErrorCode::InvalidCredentials,
            ),
        ] {
            altro.invia(messaggio).await;
            match altro.ricevi().await {
                ServerMessage::Error { code, .. } => assert_eq!(code, atteso),
                altro => panic!("atteso un errore {atteso:?}, ottenuto {altro:?}"),
            }
        }

        // Nessun rifiuto ha creato un utente.
        altro
            .invia(ClientMessage::Login {
                username: utente("veicolo_02"),
                password: "corta".to_owned(),
            })
            .await;
        assert!(matches!(
            altro.ricevi().await,
            ServerMessage::Error {
                code: ErrorCode::InvalidCredentials,
                ..
            }
        ));

        // La connessione e' ancora utilizzabile senza rifare l'handshake.
        altro.autentica("veicolo_03", true).await;
    }

    #[tokio::test]
    async fn ogni_richiesta_prima_dell_accesso_riceve_not_authenticated() {
        let banco = avvia_banco().await;

        let messaggi = [
            ClientMessage::Position {
                coordinate: punto(0),
                recorded_at: istante_base(),
            },
            ClientMessage::TextToServer {
                body: "in arrivo".to_owned(),
            },
            ClientMessage::RequestAnalysis {
                range: AnalysisRange::Today,
            },
            ClientMessage::Ping,
            ClientMessage::Logout,
        ];

        for (indice, messaggio) in messaggi.into_iter().enumerate() {
            let mut cliente = Cliente::connetti(banco.indirizzo).await;
            assert!(matches!(
                cliente.saluta().await,
                ServerMessage::Welcome { .. }
            ));
            cliente.invia(messaggio.clone()).await;

            assert!(
                matches!(
                    cliente.ricevi().await,
                    ServerMessage::Error {
                        code: ErrorCode::NotAuthenticated,
                        ..
                    }
                ),
                "{messaggio:?} prima dell'accesso deve essere rifiutato"
            );
            // Il rifiuto riguarda la richiesta, non la sequenza: si puo' ancora accedere.
            cliente.autentica(&format!("veicolo_{indice}"), true).await;
            cliente.esci().await;
        }
    }

    #[tokio::test]
    async fn le_posizioni_valide_finiscono_nel_repository() {
        let banco = avvia_banco().await;
        let mut cliente = Cliente::connetti(banco.indirizzo).await;
        cliente.registra("veicolo_01").await;
        let t0 = istante_base();

        cliente.invia_percorso(&[0, 10, 20]).await;

        let salvate = banco.posizioni_di(UserId(1));
        assert_eq!(
            salvate.iter().map(|p| p.recorded_at).collect::<Vec<_>>(),
            vec![t0, t0 + TimeDelta::seconds(10), t0 + TimeDelta::seconds(20)]
        );
        for (posizione, offset) in salvate.iter().zip([0, 10, 20]) {
            assert_eq!(
                posizione.user_id,
                UserId(1),
                "la posizione va attribuita all'utente della sessione"
            );
            assert_eq!(posizione.coordinate, punto(offset));
        }
    }

    #[tokio::test]
    async fn una_posizione_troppo_nel_futuro_e_rifiutata_ma_la_tolleranza_e_inclusa() {
        let banco = avvia_banco().await;
        let mut cliente = Cliente::connetti(banco.indirizzo).await;
        cliente.registra("veicolo_01").await;

        cliente
            .invia(ClientMessage::Position {
                coordinate: punto(0),
                recorded_at: istante_base() + TimeDelta::minutes(10),
            })
            .await;
        assert!(matches!(
            cliente.ricevi().await,
            ServerMessage::Error {
                code: ErrorCode::MalformedMessage,
                ..
            }
        ));
        assert!(
            banco.posizioni_di(UserId(1)).is_empty(),
            "una posizione rifiutata non deve essere persistita"
        );

        // Il limite di anticipo e' incluso.
        cliente
            .invia(ClientMessage::Position {
                coordinate: punto(0),
                recorded_at: istante_base() + TimeDelta::seconds(ANTICIPO_MASSIMO_SECS),
            })
            .await;
        assert_eq!(
            cliente.ricevi().await,
            ServerMessage::Ack {
                request: AckKind::Position
            }
        );
        assert_eq!(banco.posizioni_di(UserId(1)).len(), 1);
    }

    #[tokio::test]
    async fn oltre_il_limite_di_frequenza_arriva_rate_limited_finche_la_finestra_non_riparte() {
        let banco = avvia_banco().await;
        let mut cliente = Cliente::connetti(banco.indirizzo).await;
        cliente.registra("veicolo_01").await;

        // Sempre la stessa coordinata, per non avere cambi di stato fra le risposte.
        for tentativo in 0..15 {
            cliente
                .invia(ClientMessage::Position {
                    coordinate: punto(0),
                    recorded_at: istante_base(),
                })
                .await;

            let risposta = cliente.ricevi().await;
            if tentativo < i64::from(MAX_POSIZIONI_PER_FINESTRA) {
                assert_eq!(
                    risposta,
                    ServerMessage::Ack {
                        request: AckKind::Position
                    },
                    "la posizione {tentativo} sta dentro il limite"
                );
            } else {
                assert!(
                    matches!(
                        risposta,
                        ServerMessage::Error {
                            code: ErrorCode::RateLimited,
                            ..
                        }
                    ),
                    "la posizione {tentativo} supera il limite, ottenuto {risposta:?}"
                );
            }
        }

        // Il limite non chiude la connessione: lo si dimostra usandola.
        cliente.invia(ClientMessage::Ping).await;
        assert_eq!(cliente.ricevi().await, ServerMessage::Pong);
        assert_eq!(
            banco.posizioni_di(UserId(1)).len(),
            usize::try_from(MAX_POSIZIONI_PER_FINESTRA).expect("il limite entra in usize"),
            "le posizioni oltre il limite non vanno persistite"
        );

        banco.orologio.advance(TimeDelta::seconds(FINESTRA_SECS));
        cliente
            .invia(ClientMessage::Position {
                coordinate: punto(0),
                recorded_at: istante_base(),
            })
            .await;
        assert_eq!(
            cliente.ricevi().await,
            ServerMessage::Ack {
                request: AckKind::Position
            },
            "passata la finestra il conteggio riparte"
        );
    }

    #[test]
    fn il_limitatore_conta_per_finestra_fissa() {
        let t0 = istante_base();
        let mut limitatore = Limitatore::nuovo(t0);

        for _ in 0..MAX_POSIZIONI_PER_FINESTRA {
            assert!(limitatore.consenti(t0));
        }
        assert!(!limitatore.consenti(t0));
        assert!(
            !limitatore.consenti(t0 + TimeDelta::seconds(FINESTRA_SECS - 1)),
            "dentro la finestra il conteggio non riparte"
        );
        assert!(limitatore.consenti(t0 + TimeDelta::seconds(FINESTRA_SECS)));
    }

    #[tokio::test]
    async fn il_cambio_di_stato_segue_l_ack_e_non_si_ripete() {
        let banco = avvia_banco().await;
        let mut cliente = Cliente::connetti(banco.indirizzo).await;
        cliente.registra("veicolo_01").await;

        // Prima posizione: solo Ack. Seconda, oltre la soglia: Ack e passaggio in
        // movimento.
        cliente.porta_in_movimento().await;
        assert_eq!(banco.stato_di(UserId(1)), Some(UserState::Moving));

        // Gia' in movimento: dopo l'Ack arriva direttamente il Pong.
        cliente.invia_percorso(&[2]).await;
        cliente.invia(ClientMessage::Ping).await;
        assert_eq!(cliente.ricevi().await, ServerMessage::Pong);
    }

    #[tokio::test]
    async fn il_task_periodico_riporta_a_fermo_chi_smette_di_inviare() {
        let banco = avvia_banco().await;
        let mut cliente = Cliente::connetti(banco.indirizzo).await;
        cliente.registra("veicolo_01").await;
        cliente.porta_in_movimento().await;

        // Senza nuove posizioni, solo il task periodico puo' produrre un messaggio.
        banco.orologio.advance(oltre_la_soglia_di_fermo());

        assert_eq!(
            cliente.ricevi().await,
            ServerMessage::StateChanged {
                state: UserState::Still
            },
            "il ritorno a fermo arriva senza che il client abbia inviato altro"
        );
        assert_eq!(banco.stato_di(UserId(1)), Some(UserState::Still));
    }

    #[tokio::test]
    async fn un_utente_disconnesso_non_riceve_altri_cambi_di_stato() {
        let banco = avvia_banco().await;

        let mut uscente = Cliente::connetti(banco.indirizzo).await;
        uscente.registra("veicolo_01").await;
        uscente.porta_in_movimento().await;

        // Il secondo utente connesso dimostra che il task continua a girare.
        let mut presente = Cliente::connetti(banco.indirizzo).await;
        presente.registra("veicolo_02").await;
        presente.porta_in_movimento().await;

        uscente.esci().await;
        banco.orologio.advance(oltre_la_soglia_di_fermo());

        assert_eq!(
            presente.ricevi().await,
            ServerMessage::StateChanged {
                state: UserState::Still
            }
        );
        assert_eq!(
            banco.stato_di(UserId(1)),
            Some(UserState::Disconnected),
            "chi si e' disconnesso non torna fermo, quindi non c'e' nulla da recapitargli"
        );

        presente.invia(ClientMessage::Ping).await;
        assert_eq!(presente.ricevi().await, ServerMessage::Pong);
    }

    #[tokio::test]
    async fn un_percorso_persistito_torna_come_analisi_del_giorno() {
        let banco = avvia_banco().await;
        let mut cliente = Cliente::connetti(banco.indirizzo).await;
        cliente.registra("veicolo_01").await;

        // Tre punti a dieci secondi l'uno dall'altro: due segmenti in movimento.
        cliente.invia_percorso(&[0, 10, 20]).await;

        // L'orologio sta a mezzanotte UTC: va spostato in avanti perche' Today
        // comprenda tutte le posizioni.
        banco.orologio.advance(TimeDelta::seconds(60));

        let riepilogo = cliente.chiedi_analisi(AnalysisRange::Today).await;

        let attesa = haversine_meters(punto(0), punto(10)) + haversine_meters(punto(10), punto(20));
        assert!(
            (riepilogo.total_distance_meters - attesa).abs() < 1.0,
            "distanza {} m, attesi {attesa} m",
            riepilogo.total_distance_meters
        );
        assert_eq!(riepilogo.moving_duration_secs, 20);
        assert_eq!(riepilogo.pause_duration_secs, 0);
        assert_eq!(
            riepilogo.route,
            banco.posizioni_di(UserId(1)),
            "la rotta deve riportare le posizioni davvero persistite"
        );
    }

    #[tokio::test]
    async fn l_analisi_guarda_solo_l_utente_della_sessione() {
        let banco = avvia_banco().await;
        let mut primo = Cliente::connetti(banco.indirizzo).await;
        primo.registra("veicolo_01").await;
        primo.invia_percorso(&[0, 10, 20]).await;

        let mut secondo = Cliente::connetti(banco.indirizzo).await;
        secondo.registra("veicolo_02").await;
        banco.orologio.advance(TimeDelta::seconds(60));

        // Nessuna posizione: riepilogo a zero.
        let riepilogo = secondo.chiedi_analisi(AnalysisRange::Today).await;

        assert_eq!(riepilogo.total_distance_meters, 0.0);
        assert_eq!(riepilogo.moving_duration_secs, 0);
        assert_eq!(riepilogo.average_speed_kmh, 0.0);
        assert!(riepilogo.route.is_empty());
    }

    #[tokio::test]
    async fn un_intervallo_custom_seleziona_le_posizioni_e_rifiuta_gli_estremi_rovesciati() {
        let banco = avvia_banco().await;
        let mut cliente = Cliente::connetti(banco.indirizzo).await;
        cliente.registra("veicolo_01").await;
        cliente.invia_percorso(&[0, 10, 20]).await;

        // Estremi inclusi: resta un solo segmento.
        let riepilogo = cliente
            .chiedi_analisi(AnalysisRange::Custom {
                from: istante_base(),
                to: istante_base() + TimeDelta::seconds(10),
            })
            .await;
        assert_eq!(riepilogo.route.len(), 2);
        assert_eq!(riepilogo.moving_duration_secs, 10);

        // Con posizioni presenti, l'errore dimostra che il controllo precede la lettura.
        cliente
            .invia(ClientMessage::RequestAnalysis {
                range: AnalysisRange::Custom {
                    from: istante_base() + TimeDelta::days(1),
                    to: istante_base() - TimeDelta::days(1),
                },
            })
            .await;
        assert!(matches!(
            cliente.ricevi().await,
            ServerMessage::Error {
                code: ErrorCode::MalformedMessage,
                ..
            }
        ));
    }

    #[test]
    fn il_troncamento_tiene_le_posizioni_piu_recenti() {
        let percorso = |quante: i64| {
            (0..quante)
                .map(|offset| PositionRecord {
                    user_id: UserId(1),
                    coordinate: punto(offset),
                    recorded_at: istante_base() + TimeDelta::seconds(offset),
                })
                .collect::<Vec<_>>()
        };

        // Sotto o al limite non si tronca nulla.
        for quante in [3, 5] {
            let posizioni = percorso(quante);
            let (analizzate, scartate) = tronca_alle_piu_recenti(posizioni.clone(), 5);
            assert_eq!(analizzate, posizioni);
            assert_eq!(scartate, None);
        }

        let posizioni = percorso(5);
        let (analizzate, scartate) = tronca_alle_piu_recenti(posizioni.clone(), 2);
        assert_eq!(
            analizzate,
            posizioni[3..],
            "vanno tenute le ultime in ordine di recorded_at"
        );
        assert_eq!(
            scartate,
            Some(5),
            "il conteggio da riportare nel log e' quello originale"
        );

        let (vuote, scartate) = tronca_alle_piu_recenti(Vec::new(), 0);
        assert!(vuote.is_empty());
        assert_eq!(scartate, None);
    }

    #[tokio::test]
    async fn text_to_server_riceve_ack_e_un_testo_troppo_lungo_e_malformato() {
        let banco = avvia_banco().await;
        let mut cliente = Cliente::connetti(banco.indirizzo).await;
        cliente.registra("veicolo_01").await;

        cliente
            .invia(ClientMessage::TextToServer {
                body: "in arrivo al deposito".to_owned(),
            })
            .await;
        assert_eq!(
            cliente.ricevi().await,
            ServerMessage::Ack {
                request: AckKind::TextToServer
            }
        );

        cliente
            .invia(ClientMessage::TextToServer {
                body: "a".repeat(MAX_CARATTERI + 1),
            })
            .await;
        assert!(matches!(
            cliente.ricevi().await,
            ServerMessage::Error {
                code: ErrorCode::MalformedMessage,
                ..
            }
        ));

        // La connessione resta utilizzabile.
        cliente.invia(ClientMessage::Ping).await;
        assert_eq!(cliente.ricevi().await, ServerMessage::Pong);
    }

    #[tokio::test]
    async fn al_login_arrivano_i_messaggi_rimasti_in_attesa() {
        let banco = avvia_banco().await;

        // L'utente si scollega: i messaggi restano in attesa.
        let mut prima = Cliente::connetti(banco.indirizzo).await;
        prima.registra("veicolo_01").await;
        prima.esci().await;

        let attesi = ["torna in sede", "poi passa dal deposito"];
        for corpo in attesi {
            assert!(
                !banco
                    .contesto
                    .messaging
                    .direct(UserId(1), corpo)
                    .await
                    .expect("l'invio diretto riesce"),
                "l'utente e' scollegato: la consegna non puo' essere immediata"
            );
        }

        let mut dopo = Cliente::connetti(banco.indirizzo).await;
        dopo.accedi("veicolo_01").await;

        for corpo in attesi {
            assert_eq!(
                dopo.ricevi().await,
                ServerMessage::Direct {
                    body: corpo.to_owned(),
                    sent_at: istante_base(),
                },
                "i messaggi in attesa arrivano tutti, nell'ordine in cui erano stati mandati"
            );
        }

        // Il Pong subito dopo l'ultimo Direct conferma che non c'e' altro in coda.
        dopo.invia(ClientMessage::Ping).await;
        assert_eq!(dopo.ricevi().await, ServerMessage::Pong);
        assert!(
            banco.pendenti_di(UserId(1)).is_empty(),
            "i messaggi consegnati vanno marcati, altrimenti tornerebbero al prossimo accesso"
        );
    }

    #[tokio::test]
    async fn un_direct_ricevuto_da_collegati_non_torna_al_prossimo_accesso() {
        let banco = avvia_banco().await;
        let mut cliente = Cliente::connetti(banco.indirizzo).await;
        cliente.registra("veicolo_01").await;

        assert!(
            banco
                .contesto
                .messaging
                .direct(UserId(1), "torna in sede")
                .await
                .expect("l'invio diretto riesce"),
            "l'utente e' collegato: la consegna e' immediata"
        );
        assert_eq!(
            cliente.ricevi().await,
            ServerMessage::Direct {
                body: "torna in sede".to_owned(),
                sent_at: istante_base(),
            }
        );
        cliente.esci().await;

        let mut di_nuovo = Cliente::connetti(banco.indirizzo).await;
        di_nuovo.accedi("veicolo_01").await;

        // Un messaggio ancora in attesa arriverebbe prima del Pong.
        di_nuovo.invia(ClientMessage::Ping).await;
        assert_eq!(di_nuovo.ricevi().await, ServerMessage::Pong);
        assert!(banco.pendenti_di(UserId(1)).is_empty());
    }

    #[tokio::test]
    async fn il_logout_sgancia_la_sessione_dal_registro() {
        let banco = avvia_banco().await;
        let mut cliente = Cliente::connetti(banco.indirizzo).await;
        cliente.registra("veicolo_01").await;
        assert_eq!(
            banco.contesto.registry.connected_users().await,
            vec![UserId(1)]
        );

        cliente.esci().await;

        assert!(
            banco.contesto.registry.connected_users().await.is_empty(),
            "l'utente non deve piu' risultare connesso"
        );
        assert_eq!(banco.stato_di(UserId(1)), Some(UserState::Disconnected));
    }

    #[tokio::test]
    async fn la_sessione_sostituita_riceve_l_avviso_prima_della_chiusura() {
        let banco = avvia_banco().await;

        let mut primo = Cliente::connetti(banco.indirizzo).await;
        primo.registra("veicolo_01").await;

        let mut secondo = Cliente::connetti(banco.indirizzo).await;
        secondo.accedi("veicolo_01").await;

        // L'avviso deve arrivare prima della chiusura, altrimenti il client lo
        // scambierebbe per una caduta di rete e si riconnetterebbe.
        match primo.ricevi().await {
            ServerMessage::Error { code, message } => {
                assert_eq!(
                    code,
                    ErrorCode::NotAuthenticated,
                    "il codice piu' vicino nel significato, in mancanza di uno dedicato"
                );
                assert_eq!(
                    message, SESSION_REPLACED_MESSAGE,
                    "il testo e' il discriminante su cui il client si regola"
                );
            }
            altro => panic!("atteso l'avviso di sessione sostituita, ottenuto {altro:?}"),
        }
        assert!(
            primo.e_chiusa().await,
            "dopo l'avviso la connessione precedente va comunque chiusa"
        );

        // La nuova connessione resta attiva e registrata.
        secondo.invia(ClientMessage::Ping).await;
        assert_eq!(secondo.ricevi().await, ServerMessage::Pong);
        assert_eq!(
            banco.contesto.registry.connected_users().await,
            vec![UserId(1)]
        );
    }

    #[tokio::test]
    async fn la_chiusura_della_sessione_sostituita_non_segna_l_utente_come_sconnesso() {
        let banco = avvia_banco().await;

        let mut primo = Cliente::connetti(banco.indirizzo).await;
        primo.registra("veicolo_01").await;
        let mut secondo = Cliente::connetti(banco.indirizzo).await;
        secondo.accedi("veicolo_01").await;

        primo.ricevi().await;
        assert!(primo.e_chiusa().await);

        // Si attende la pulizia del gestore della vecchia connessione.
        tokio::time::sleep(Duration::from_millis(200)).await;
        secondo.invia(ClientMessage::Ping).await;
        assert_eq!(secondo.ricevi().await, ServerMessage::Pong);

        assert_eq!(
            banco.stato_di(UserId(1)),
            Some(UserState::Still),
            "l'utente e' ancora connesso con la sessione nuova"
        );
    }

    #[tokio::test]
    async fn l_inattivita_chiude_la_connessione_e_libera_la_sessione() {
        let banco = avvia_banco_con(ConnectionTimeouts {
            idle: Duration::from_millis(200),
            ..tempi()
        })
        .await;
        let mut cliente = Cliente::connetti(banco.indirizzo).await;
        cliente.registra("veicolo_01").await;

        // Allo scadere dell'inattivita' il server chiude e libera la sessione.
        assert!(cliente.e_chiusa().await);
        assert!(banco.contesto.registry.connected_users().await.is_empty());
    }

    #[tokio::test]
    async fn lo_spegnimento_chiude_le_connessioni_e_ferma_il_task_periodico() {
        let banco = avvia_banco().await;
        let mut cliente = Cliente::connetti(banco.indirizzo).await;
        cliente.registra("veicolo_01").await;
        cliente.porta_in_movimento().await;

        // Verifica preliminare che il task periodico stia girando.
        banco.orologio.advance(oltre_la_soglia_di_fermo());
        assert_eq!(
            cliente.ricevi().await,
            ServerMessage::StateChanged {
                state: UserState::Still
            }
        );

        banco
            .spegnimento
            .send(())
            .expect("almeno un gestore e' in ascolto");

        assert!(cliente.e_chiusa().await);
        timeout(ATTESA, banco.battito)
            .await
            .expect("il task periodico deve terminare allo spegnimento")
            .expect("il task periodico non deve andare in panico");
    }

    #[tokio::test]
    async fn un_frame_malformato_chiude_la_connessione() {
        let banco = avvia_banco().await;
        let grezzo = TcpStream::connect(banco.indirizzo)
            .await
            .expect("connessione riuscita");
        let (lettura, scrittura) = grezzo.into_split();
        let mut scrittura = FramedWrite::new(scrittura, BytesCodec::new());
        let mut lettura = FramedRead::new(lettura, BytesCodec::new());

        // Un payload non valido disallinea il flusso: la connessione va chiusa.
        scrittura
            .send(Bytes::from_static(&[0, 0, 0, 4, 0xFF, 0xFF, 0xFF, 0xFF]))
            .await
            .expect("scrittura riuscita");

        assert!(
            timeout(ATTESA, lettura.next())
                .await
                .expect("il server deve reagire entro l'attesa")
                .is_none(),
            "atteso EOF dopo un frame malformato"
        );
    }
}
