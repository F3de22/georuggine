//! Livello di rete del server: ascolto, ciclo di vita di una connessione e sessioni.
//!
//! # Forma di una connessione
//!
//! `MessageCodec<T>` e' generico su un solo tipo di messaggio, quindi codifica e
//! decodifica lo stesso `T`; al server servono invece due versi diversi, perche' legge
//! [`ClientMessage`] e scrive [`ServerMessage`]. Lo stream viene percio' diviso con
//! `into_split()` e ogni meta' riceve il codec del proprio verso:
//!
//! ```text
//!            +-- FramedRead <ClientCodec> --> ciclo del gestore
//! TcpStream -+
//!            +-- FramedWrite<ServerCodec> <-- task di scrittura <-- mpsc
//! ```
//!
//! Sulla meta' di scrittura non scrive il gestore ma un task dedicato che consuma una
//! coda mpsc. Cosi' il canale di scrittura e' un valore spostabile, che si puo' mettere
//! nel [`ConnectionRegistry`] e usare da altre connessioni per recapitare messaggi, e
//! le scritture di piu' mittenti restano serializzate senza un lock.
//!
//! # Chiusura e sostituzione di una sessione
//!
//! Chi possiede il mittente della coda decide la vita della connessione: quando l'ultimo
//! mittente cade, il task di scrittura esce e fa un `shutdown` esplicito sul socket, e
//! il client vede la fine del flusso. Da questo dipende la sostituzione di una sessione:
//! se lo stesso utente apre una seconda connessione, la prima va chiusa.
//!
//! Perche' quel meccanismo funzioni il mittente deve avere **un solo proprietario**. Fino
//! all'autenticazione e' il gestore ([`Uscita::Propria`]); dopo passa al registro e il
//! gestore ne conserva solo un riferimento debole ([`Uscita::NelRegistro`]). Togliere la
//! voce dal registro, o sostituirla, chiude davvero la connessione; se il gestore
//! tenesse un mittente forte, il canale resterebbe vivo e la vecchia connessione non si
//! chiuderebbe mai.
//!
//! # Stato condiviso e stato di una connessione
//!
//! Le dipendenze che tutte le connessioni usano stanno in [`ConnectionContext`], dietro
//! un solo [`Arc`]. Cio' che riguarda una connessione sola sta invece in
//! [`StatoConnessione`], che vive nel suo gestore: la sessione aperta e il [`Limitatore`]
//! delle posizioni. Il limite di frequenza e' per connessione, quindi tenerlo li' evita
//! sia un lock sia una mappa per utente.

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
    AckKind, ClientMessage, ErrorCode, PROTOCOL_VERSION, ServerMessage,
};
use georuggine_common::types::{Coordinate, PositionRecord, UserId, Username};
use thiserror::Error;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{RwLock, broadcast, mpsc};
use tokio::task::{self, JoinHandle};
use tokio::time::timeout;
use tokio_util::codec::{FramedRead, FramedWrite};

use crate::auth::{self, AuthConfig, AuthError, SessionStore, SessionToken};
use crate::observer::PositionObserver;
use crate::storage::Repository;

/// Messaggi che possono attendere in coda verso un client prima che le scritture
/// rallentino il mittente.
///
/// Serve a non far crescere la memoria senza limite quando un client legge piu' piano
/// di quanto il server produca: raggiunta la capacita', chi invia attende invece di
/// accumulare.
const CAPACITA_CODA: usize = 32;

/// Posizioni che una connessione puo' far accettare in una finestra di
/// [`FINESTRA_SECS`] secondi.
///
/// Un veicolo invia la propria posizione ogni pochi secondi, quindi dieci al minuto
/// coprono il ritmo previsto con margine; oltre, o il client e' guasto o sta provando a
/// riempire il database, e in nessuno dei due casi il server ha motivo di scrivere.
const MAX_POSIZIONI_PER_FINESTRA: u32 = 10;

/// Durata in secondi della finestra su cui si conta [`MAX_POSIZIONI_PER_FINESTRA`].
const FINESTRA_SECS: i64 = 60;

/// Anticipo massimo, in secondi, che si tollera su `recorded_at`.
///
/// L'istante lo misura il client, il cui orologio non e' sincronizzato con quello del
/// server: qualche minuto di scarto e' normale e non va buttato via. Una rilevazione
/// datata molto piu' avanti non e' invece spiegabile con la deriva di un orologio, e
/// accettarla sposterebbe in avanti le analisi di tutto lo storico.
const ANTICIPO_MASSIMO_SECS: i64 = 300;

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
    /// Cartella in cui scrivere i log del tempo di CPU.
    ///
    /// Se relativa, si intende rispetto alla directory di lavoro del processo.
    pub log_dir: PathBuf,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            // I valori predefiniti sono costanti testuali valide per costruzione;
            // se non lo fossero il server partirebbe comunque, sull'indirizzo di
            // ripiego, invece di andare in panico all'avvio.
            bind_addr: BIND_PREDEFINITO
                .parse()
                .unwrap_or_else(|_| SocketAddr::from(([127, 0, 0, 1], 7878))),
            db_path: PathBuf::from(DB_PREDEFINITO),
            log_dir: PathBuf::from(LOG_DIR_PREDEFINITA),
        }
    }
}

/// Esito dell'analisi degli argomenti.
///
/// `--help` non stampa ne' esce da dentro la funzione di analisi: la decisione torna al
/// chiamante, cosi' l'analisi resta una funzione pura e verificabile con un test.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Avvio {
    /// Il server deve partire con questa configurazione.
    Configurato(ServerConfig),
    /// E' stato chiesto `--help`: va stampato [`USO`] e si esce senza errore.
    Uso,
}

impl ServerConfig {
    /// Analizza gli argomenti da riga di comando.
    ///
    /// Si aspetta i soli argomenti, senza il nome del programma: il chiamante passa
    /// tipicamente `std::env::args().skip(1)`.
    ///
    /// # Errori
    ///
    /// [`ArgsError`] se un'opzione e' sconosciuta, se manca il valore di un'opzione che
    /// lo richiede o se `--bind` non e' un indirizzo valido.
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

/// Scadenze che regolano il ciclo di vita di una connessione.
///
/// Sono un parametro e non tre costanti perche' i test devono poter verificare il
/// comportamento alla scadenza senza aspettare i valori di produzione.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConnectionTimeouts {
    /// Attesa massima del primo [`ClientMessage::Hello`].
    pub handshake: Duration,
    /// Silenzio massimo del client prima che la connessione venga chiusa.
    pub idle: Duration,
    /// Attesa massima della chiusura ordinata dopo il segnale di spegnimento.
    pub shutdown: Duration,
}

impl Default for ConnectionTimeouts {
    fn default() -> Self {
        Self {
            handshake: Duration::from_secs(5),
            idle: Duration::from_secs(90),
            shutdown: Duration::from_secs(5),
        }
    }
}

/// Dipendenze che ogni connessione condivide.
///
/// Stanno in una struttura sola perche' il gestore le riceve tutte insieme dietro un
/// unico [`Arc`]: una connessione in piu' costa un incremento di contatore.
pub struct ConnectionContext {
    /// Persistenza di utenti, posizioni e messaggi.
    pub repo: Arc<dyn Repository>,
    /// Registro dei token di sessione attivi.
    pub sessions: Arc<SessionStore>,
    /// Registro delle connessioni aperte, una per utente.
    pub registry: Arc<ConnectionRegistry>,
    /// Sorgente dell'ora: il gestore non chiama mai `Utc::now()` da se'.
    pub clock: Arc<dyn Clock>,
    /// Destinatario degli eventi di sessione e di posizione.
    pub observer: Arc<dyn PositionObserver>,
    /// Parametri di costo dell'hashing delle password.
    pub auth: AuthConfig,
    /// Scadenze del ciclo di vita.
    pub timeouts: ConnectionTimeouts,
}

/// Connessioni aperte, una per utente autenticato.
///
/// Il valore associato a un utente e' il mittente della coda di scrittura della sua
/// connessione: da qui una parte qualunque del server puo' recapitargli un messaggio
/// senza conoscere il socket. Il registro e' anche il **proprietario** di quel mittente,
/// e da questo dipende la chiusura delle connessioni: vedi la nota nel modulo.
///
/// Usa il [`RwLock`] di tokio perche' i metodi sono `async`; il guard non attraversa mai
/// un punto di sospensione, perche' il mittente viene clonato fuori dalla sezione
/// critica e solo dopo si attende l'invio.
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

    /// Registra la connessione di un utente.
    ///
    /// Se l'utente aveva gia' una connessione aperta, restituisce il mittente
    /// precedente: sta al chiamante lasciarlo cadere, cosa che chiude la connessione
    /// vecchia. Non viene chiusa qui perche' il chiamante puo' voler prima registrare
    /// l'evento o avvisare il client.
    pub async fn register(
        &self,
        user_id: UserId,
        sender: mpsc::Sender<ServerMessage>,
    ) -> Option<mpsc::Sender<ServerMessage>> {
        self.connessioni.write().await.insert(user_id, sender)
    }

    /// Toglie la connessione di un utente dal registro.
    ///
    /// Se il mittente tolto era l'ultimo, la connessione si chiude.
    pub async fn unregister(&self, user_id: UserId) {
        self.connessioni.write().await.remove(&user_id);
    }

    /// Recapita un messaggio a un utente, se ha una connessione aperta.
    ///
    /// Restituisce vero se il messaggio e' stato accodato. Falso significa che l'utente
    /// non e' connesso, oppure che la sua connessione si stava gia' chiudendo.
    pub async fn send_to(&self, user_id: UserId, message: ServerMessage) -> bool {
        // Il mittente si clona dentro la sezione critica e si usa fuori: mandare il
        // messaggio tenendo il lock lo terrebbe per tutta la durata dell'invio, che puo'
        // sospendersi se la coda del destinatario e' piena.
        let destinatario = self.connessioni.read().await.get(&user_id).cloned();

        match destinatario {
            Some(mittente) => mittente.send(message).await.is_ok(),
            None => false,
        }
    }

    /// Recapita lo stesso messaggio a tutte le connessioni registrate.
    ///
    /// Restituisce il numero di destinatari a cui il messaggio e' stato accodato. Il
    /// fallimento di uno non ferma gli altri: chi non e' raggiungibile viene saltato e
    /// semplicemente non conteggiato.
    ///
    /// A differenza di [`send_to`](Self::send_to) l'accodamento non attende: un
    /// destinatario che legge troppo piano perde il messaggio invece di sospendere il
    /// broadcast verso tutti gli altri, che con una coda piena resterebbero in attesa
    /// del piu' lento.
    pub async fn broadcast_all(&self, message: ServerMessage) -> usize {
        // Come in `send_to`, i mittenti si clonano dentro la sezione critica e si usano
        // fuori: il lock non deve restare preso per tutta la durata degli invii.
        let destinatari: Vec<mpsc::Sender<ServerMessage>> =
            self.connessioni.read().await.values().cloned().collect();

        destinatari
            .iter()
            .filter(|mittente| mittente.try_send(message.clone()).is_ok())
            .count()
    }

    /// Utenti con una connessione aperta, in ordine di id crescente.
    ///
    /// L'ordine e' imposto qui e non lasciato a quello della mappa, che varia da
    /// un'esecuzione all'altra: cosi' i log e i test sono riproducibili.
    pub async fn connected_users(&self) -> Vec<UserId> {
        let mut utenti: Vec<UserId> = self.connessioni.read().await.keys().copied().collect();
        utenti.sort_unstable();
        utenti
    }

    /// Toglie la voce dell'utente solo se e' ancora quella della connessione indicata.
    ///
    /// Serve alla pulizia finale di un gestore: se nel frattempo l'utente si e'
    /// ricollegato, la voce nel registro appartiene alla connessione nuova e toglierla
    /// scollegherebbe quella viva. Il confronto e la rimozione avvengono sotto lo stesso
    /// lock in scrittura, quindi non c'e' finestra fra i due.
    async fn unregister_se_propria(
        &self,
        user_id: UserId,
        proprio: &mpsc::WeakSender<ServerMessage>,
    ) {
        let mut connessioni = self.connessioni.write().await;

        let (Some(registrato), Some(nostro)) = (connessioni.get(&user_id), proprio.upgrade())
        else {
            return;
        };
        if registrato.same_channel(&nostro) {
            connessioni.remove(&user_id);
        }
    }
}

/// Estremo di scrittura di una connessione, dal punto di vista del gestore.
///
/// La distinzione fra i due casi non e' un dettaglio: e' cio' che rende chiudibile una
/// connessione dall'esterno. Vedi la nota nel modulo.
enum Uscita {
    /// Prima dell'autenticazione il gestore possiede il canale.
    Propria(mpsc::Sender<ServerMessage>),
    /// Dopo l'autenticazione il canale vive nel registro e qui resta un riferimento
    /// debole: se la voce nel registro sparisce, questa connessione si chiude.
    NelRegistro(mpsc::WeakSender<ServerMessage>),
}

impl Uscita {
    /// Accoda un messaggio verso il client.
    ///
    /// Restituisce falso se la connessione non e' piu' scrivibile, cioe' se il task di
    /// scrittura e' terminato o se questa sessione e' stata sostituita.
    async fn invia(&self, messaggio: ServerMessage) -> bool {
        // L'eventuale mittente ottenuto da `upgrade` vive solo per la durata dell'invio:
        // tenerlo piu' a lungo terrebbe in vita il canale e impedirebbe la chiusura.
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

/// Sessione aperta su una connessione.
struct Sessione {
    /// Utente autenticato.
    user_id: UserId,
    /// Token consegnato al client, da togliere dallo store alla chiusura.
    token: SessionToken,
}

/// Contatore a finestra fissa che limita le posizioni accettate da una connessione.
///
/// La finestra parte dal primo messaggio dopo l'apertura di quella precedente e dura
/// [`FINESTRA_SECS`] secondi; a scadenza il conteggio riparte da zero. E' la forma piu'
/// semplice di limitazione, e ha il difetto noto delle finestre fisse: a cavallo di due
/// finestre contigue si possono far passare fino al doppio delle richieste in poco
/// tempo. Non e' un problema qui, perche' il limite serve a fermare un client che invia
/// senza sosta, non a spianare il traffico al secondo.
///
/// Vive nello stato della singola connessione e non nel contesto condiviso: il limite e'
/// per connessione, quindi non serve ne' un lock ne' una mappa per utente.
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

    /// Conta una posizione e dice se sta dentro il limite.
    ///
    /// Il conteggio comprende anche le posizioni che verranno poi rifiutate perche'
    /// malformate: il limite riguarda i messaggi che il client invia, e non contarle
    /// lascerebbe a un client difettoso la possibilita' di chiamare il server quanto
    /// vuole purche' sbagli i dati.
    ///
    /// Un orologio che tornasse indietro non riaprirebbe la finestra: la differenza
    /// diventa negativa e il conteggio prosegue, che e' il comportamento prudente.
    fn consenti(&mut self, adesso: DateTime<Utc>) -> bool {
        if adesso.signed_duration_since(self.inizio).num_seconds() >= FINESTRA_SECS {
            self.inizio = adesso;
            self.conteggio = 0;
        }

        if self.conteggio < MAX_POSIZIONI_PER_FINESTRA {
            // Non puo' traboccare: il ramo si prende solo sotto il limite.
            self.conteggio += 1;
            true
        } else {
            false
        }
    }
}

/// Stato che vive quanto una connessione e non e' condiviso con le altre.
struct StatoConnessione {
    /// Sessione aperta, presente solo dopo un'autenticazione riuscita.
    sessione: Option<Sessione>,
    /// Limite di frequenza delle posizioni di questa connessione.
    limitatore: Limitatore,
}

impl StatoConnessione {
    /// Stato di una connessione appena aperta, senza sessione.
    fn nuovo(adesso: DateTime<Utc>) -> Self {
        Self {
            sessione: None,
            limitatore: Limitatore::nuovo(adesso),
        }
    }
}

/// Accetta connessioni finche' non arriva il segnale di spegnimento.
///
/// L'indirizzo effettivo viene registrato nel log: con la porta 0 non coincide con
/// quello richiesto, e senza saperlo non ci si potrebbe collegare.
///
/// # Errori
///
/// [`io::Error`] se non si riesce ad ascoltare su `config.bind_addr`. Gli errori di una
/// singola `accept` non fermano il server: vengono registrati e il ciclo prosegue,
/// perche' sono in genere transitori (descrittori esauriti, peer sparito).
pub async fn run(
    config: ServerConfig,
    repo: Arc<dyn Repository>,
    sessions: Arc<SessionStore>,
    registry: Arc<ConnectionRegistry>,
    clock: Arc<dyn Clock>,
    observer: Arc<dyn PositionObserver>,
    shutdown: broadcast::Sender<()>,
) -> io::Result<()> {
    let listener = TcpListener::bind(config.bind_addr).await?;
    let indirizzo = listener.local_addr()?;
    tracing::info!(%indirizzo, "server in ascolto");

    let contesto = Arc::new(ConnectionContext {
        repo,
        sessions,
        registry,
        clock,
        observer,
        auth: AuthConfig::default(),
        timeouts: ConnectionTimeouts::default(),
    });
    let mut spegnimento = shutdown.subscribe();

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

/// Segue una connessione dall'apertura alla chiusura.
///
/// Non restituisce un errore: una connessione che fallisce riguarda un solo client, e il
/// server non ha nulla da decidere al suo posto. Ogni motivo di chiusura viene invece
/// registrato nel log.
pub async fn handle_connection(
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

    if handshake(&mut ingresso, &uscita, &contesto, peer).await {
        dialogo(
            &mut ingresso,
            &mut uscita,
            &mut stato,
            &contesto,
            &mut shutdown,
            &mut scrittore,
            peer,
        )
        .await;
    }

    // La sessione va sganciata qualunque sia il motivo della chiusura: logout, silenzio,
    // spegnimento o errore. `chiudi_sessione` non fa nulla se e' gia' stata sganciata.
    chiudi_sessione(&mut stato.sessione, &uscita, &contesto).await;

    // Caduto anche l'ultimo mittente, il task di scrittura svuota la coda e chiude il
    // socket in modo ordinato. Si aspetta, ma non oltre la scadenza: un peer che non
    // legge piu' non deve tenere vivo un task.
    drop(uscita);
    if timeout(contesto.timeouts.shutdown, &mut scrittore)
        .await
        .is_err()
    {
        tracing::warn!(%peer, "chiusura non completata entro la scadenza");
        scrittore.abort();
    }

    tracing::info!(%peer, "connessione chiusa");
}

/// Scrive sulla connessione tutto cio' che arriva in coda, poi la chiude.
///
/// Concentrare le scritture in un task solo evita di dover proteggere la meta' di
/// scrittura con un lock quando piu' parti del server scrivono allo stesso client.
async fn task_di_scrittura(
    mut uscita: FramedWrite<OwnedWriteHalf, ServerCodec>,
    mut coda: mpsc::Receiver<ServerMessage>,
) {
    while let Some(messaggio) = coda.recv().await {
        if uscita.send(messaggio).await.is_err() {
            // Il peer non e' piu' raggiungibile: inutile provare i messaggi successivi.
            break;
        }
    }

    // Chiusura esplicita e non un semplice drop: `close` arriva fino a `poll_shutdown`
    // sulla meta' di scrittura, cioe' a uno shutdown del verso di scrittura del socket.
    // Il client vede la fine del flusso subito, invece di restare in attesa.
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
            // Un primo messaggio diverso da Hello e' una violazione della sequenza, non
            // una richiesta fuori posto da cui si possa tornare: la connessione non ha
            // ancora negoziato una versione, quindi non c'e' nulla su cui proseguire.
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

/// Ciclo principale: serve i messaggi del client finche' la connessione resta viva.
async fn dialogo(
    ingresso: &mut FramedRead<OwnedReadHalf, ClientCodec>,
    uscita: &mut Uscita,
    stato: &mut StatoConnessione,
    contesto: &ConnectionContext,
    shutdown: &mut broadcast::Receiver<()>,
    scrittore: &mut JoinHandle<()>,
    peer: SocketAddr,
) {
    loop {
        tokio::select! {
            _ = shutdown.recv() => {
                tracing::info!(%peer, "spegnimento: chiusura della connessione");
                return;
            }
            _ = &mut *scrittore => {
                // Il task di scrittura e' finito da se': o il peer non e' piu'
                // raggiungibile, o questa sessione e' stata sostituita da una nuova
                // connessione dello stesso utente.
                tracing::info!(%peer, "canale di scrittura chiuso");
                return;
            }
            ricevuto = timeout(contesto.timeouts.idle, ingresso.next()) => {
                let messaggio = match ricevuto {
                    Err(_) => {
                        tracing::info!(%peer, "nessun messaggio entro la scadenza di inattivita'");
                        return;
                    }
                    Ok(None) => return,
                    Ok(Some(Err(errore))) => {
                        // FrameTooLarge e Serialization lasciano il flusso disallineato
                        // rispetto ai confini dei frame: proseguire leggerebbe spazzatura.
                        tracing::warn!(%peer, %errore, "frame non valido: chiusura");
                        return;
                    }
                    Ok(Some(Ok(messaggio))) => messaggio,
                };

                if !servi(messaggio, uscita, stato, contesto, peer).await {
                    return;
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
    match (stato.sessione.is_some(), messaggio) {
        (false, ClientMessage::Register { username, password }) => {
            let esito = auth::register(&contesto.repo, &contesto.auth, &username, &password).await;
            concludi_autenticazione(
                esito,
                &username,
                uscita,
                &mut stato.sessione,
                contesto,
                peer,
            )
            .await
        }
        (false, ClientMessage::Login { username, password }) => {
            let esito = auth::login(&contesto.repo, &username, &password).await;
            concludi_autenticazione(
                esito,
                &username,
                uscita,
                &mut stato.sessione,
                contesto,
                peer,
            )
            .await
        }
        (false, _) => {
            uscita
                .invia(ServerMessage::Error {
                    code: ErrorCode::NotAuthenticated,
                    message: "serve una sessione aperta: usa Register o Login".to_owned(),
                })
                .await
        }
        (true, ClientMessage::Ping) => uscita.invia(ServerMessage::Pong).await,
        (true, ClientMessage::Logout) => {
            // La conferma parte prima dello sgancio: la coda conserva l'ordine, e il
            // task di scrittura la svuota prima di chiudere il socket.
            uscita
                .invia(ServerMessage::Ack {
                    request: AckKind::Logout,
                })
                .await;
            chiudi_sessione(&mut stato.sessione, uscita, contesto).await;
            tracing::info!(%peer, "logout richiesto dal client");
            false
        }
        (
            true,
            ClientMessage::Position {
                coordinate,
                recorded_at,
            },
        ) => match stato.sessione.as_ref().map(|attiva| attiva.user_id) {
            Some(user_id) => {
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
            // Non raggiungibile: il ramo si sceglie proprio sulla presenza della sessione.
            None => true,
        },
        (true, ClientMessage::TextToServer { .. } | ClientMessage::RequestAnalysis { .. }) => {
            uscita
                .invia(ServerMessage::Error {
                    code: ErrorCode::Internal,
                    message: "non ancora implementato".to_owned(),
                })
                .await
        }
        (true, _) => {
            // Hello, Register o Login a sessione gia' aperta: fuori sequenza.
            uscita
                .invia(ServerMessage::Error {
                    code: ErrorCode::NotAuthenticated,
                    message: "richiesta fuori sequenza".to_owned(),
                })
                .await
        }
    }
}

/// Persiste una posizione e conferma al client; vero se la connessione resta aperta.
///
/// Requisito R3 della traccia. L'ordine dei controlli e' quello del costo crescente: il
/// limite di frequenza non tocca nulla, la validazione dell'istante e' un confronto, e
/// solo cio' che passa entrambi arriva al database. Nessuno dei due rifiuti chiude la
/// connessione: sono errori della singola richiesta, non della sequenza, e un veicolo
/// che sbaglia una rilevazione non ha motivo di riconnettersi.
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

    // L'istante lo misura il client: va verificato prima di scriverlo, perche' una
    // rilevazione datata nel futuro falserebbe ogni analisi calcolata sullo storico.
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
    // Il repository e' sincrono: la scrittura va su un thread da lavoro, altrimenti
    // fermerebbe un thread dell'esecutore per tutta la durata dell'accesso a SQLite.
    let repo = Arc::clone(&contesto.repo);
    let esito = task::spawn_blocking(move || repo.insert_position(&record)).await;

    match esito {
        Ok(Ok(())) => {}
        Ok(Err(errore)) => {
            // Il messaggio originale parla di tabelle e vincoli: al client va il solo
            // codice, come per gli errori di autenticazione.
            tracing::error!(%peer, utente = user_id.0, %errore, "posizione non persistita");
            return uscita
                .invia(ServerMessage::Error {
                    code: ErrorCode::Internal,
                    message: ErrorCode::Internal.to_string(),
                })
                .await;
        }
        Err(errore) => {
            tracing::error!(%peer, %errore, "attivita' di scrittura non completata");
            return uscita
                .invia(ServerMessage::Error {
                    code: ErrorCode::Internal,
                    message: ErrorCode::Internal.to_string(),
                })
                .await;
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

    // L'osservatore vede solo le posizioni davvero persistite, e viene interpellato dopo
    // l'Ack: la conferma della richiesta non deve dipendere dallo stato di movimento.
    match contesto.observer.on_position(user_id, coordinate) {
        Some(state) => uscita.invia(ServerMessage::StateChanged { state }).await,
        None => true,
    }
}

/// Traduce l'esito dell'autenticazione in una risposta al client.
async fn concludi_autenticazione(
    esito: Result<UserId, AuthError>,
    username: &Username,
    uscita: &mut Uscita,
    sessione: &mut Option<Sessione>,
    contesto: &ConnectionContext,
    peer: SocketAddr,
) -> bool {
    let user_id = match esito {
        Ok(user_id) => user_id,
        Err(errore) => {
            let (code, message) = risposta_a(&errore);
            // Si registra lo username, mai la password ne' l'hash.
            tracing::warn!(%peer, utente = username.as_str(), %errore, "autenticazione fallita");
            return uscita.invia(ServerMessage::Error { code, message }).await;
        }
    };

    let token = match SessionToken::generate() {
        Ok(token) => token,
        Err(errore) => {
            tracing::error!(%peer, %errore, "generazione del token di sessione fallita");
            return uscita
                .invia(ServerMessage::Error {
                    code: ErrorCode::Internal,
                    message: ErrorCode::Internal.to_string(),
                })
                .await;
        }
    };

    contesto.sessions.insert(token.clone(), user_id).await;

    // Il canale passa al registro e il gestore ne tiene solo un riferimento debole: da
    // qui in poi e' il registro a decidere la vita della connessione.
    let (debole, per_il_registro) = match uscita {
        Uscita::Propria(mittente) => (mittente.downgrade(), mittente.clone()),
        // Non raggiungibile: si arriva qui solo senza sessione aperta.
        Uscita::NelRegistro(_) => return true,
    };
    let precedente = contesto.registry.register(user_id, per_il_registro).await;
    *uscita = Uscita::NelRegistro(debole);

    if precedente.is_some() {
        // Lasciando cadere il mittente precedente si chiude la connessione che l'utente
        // aveva aperto prima: una sola sessione per utente.
        tracing::info!(
            %peer,
            utente = username.as_str(),
            "sessione precedente dello stesso utente sostituita"
        );
        drop(precedente);
    }

    *sessione = Some(Sessione {
        user_id,
        token: token.clone(),
    });
    contesto.observer.on_connect(user_id);
    tracing::info!(%peer, utente = username.as_str(), "accesso riuscito");

    uscita
        .invia(ServerMessage::SessionToken {
            token: token.as_str().to_owned(),
        })
        .await
}

/// Codice ed eventuale spiegazione da mandare al client per un errore di autenticazione.
///
/// I guasti interni non riportano il messaggio originale: parlerebbe di vincoli, tabelle
/// o primitive crittografiche, dettagli che non riguardano il client e che descrivono
/// come e' fatto il server.
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

/// Sgancia la sessione dallo store dei token e dal registro delle connessioni.
///
/// Non fa nulla se la sessione e' gia' stata sganciata, cosi' il logout e la pulizia
/// finale possono chiamarla entrambi: e' anche cio' che rende `on_disconnect` una
/// notifica sola per connessione, qualunque sia il motivo della chiusura.
async fn chiudi_sessione(
    sessione: &mut Option<Sessione>,
    uscita: &Uscita,
    contesto: &ConnectionContext,
) {
    let Some(attiva) = sessione.take() else {
        return;
    };

    contesto.sessions.remove(&attiva.token).await;
    if let Some(debole) = uscita.debole() {
        contesto
            .registry
            .unregister_se_propria(attiva.user_id, debole)
            .await;
    }
    contesto.observer.on_disconnect(attiva.user_id);
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::atomic::{AtomicUsize, Ordering};

    use chrono::TimeDelta;
    use georuggine_common::clock::FakeClock;
    use georuggine_common::types::UserState;
    use tokio_util::bytes::Bytes;
    use tokio_util::codec::BytesCodec;

    use crate::observer::NoopObserver;
    use crate::storage::InMemoryRepository;

    /// Quanto un test aspetta una risposta prima di considerarla mancata.
    ///
    /// E' generoso perche' lo si paga solo quando il test sta gia' fallendo: nel caso
    /// normale la risposta arriva in pochi millisecondi.
    const ATTESA: Duration = Duration::from_secs(5);

    /// Password che rispetta la policy di [`crate::auth::valida_password`].
    const PASSWORD: &str = "scorta12";

    /// Timeout ridotti per i test.
    ///
    /// L'inattivita' resta invece lunga apposta: quasi tutti i test verificano che la
    /// connessione si chiuda per un motivo preciso (logout, sostituzione, spegnimento),
    /// e con una scadenza breve la chiusura arriverebbe comunque, facendoli passare
    /// anche se il meccanismo sotto esame non funzionasse. Chi vuole verificare la
    /// scadenza di inattivita' la accorcia da se'.
    fn tempi() -> ConnectionTimeouts {
        ConnectionTimeouts {
            handshake: Duration::from_millis(200),
            idle: Duration::from_secs(30),
            shutdown: Duration::from_millis(200),
        }
    }

    /// Parametri di Argon2 al minimo: l'hashing di produzione e' lento apposta.
    fn autenticazione() -> AuthConfig {
        AuthConfig {
            memory_kib: 32,
            iterations: 1,
            parallelism: 1,
        }
    }

    /// Username che il test da' per valido.
    fn utente(nome: &str) -> Username {
        Username::new(nome).expect("username di test valido")
    }

    /// Istante su cui e' fermo l'orologio finto dei test: 2024-01-01T00:00:00Z.
    fn istante_base() -> DateTime<Utc> {
        DateTime::from_timestamp(1_704_067_200, 0).expect("istante valido")
    }

    /// Coordinata associata all'offset in secondi di una posizione di prova.
    ///
    /// Insieme a `recorded_at` fa da impronta del record: se il server persistesse la
    /// posizione sbagliata, o la attribuisse a un altro utente, le coordinate rilette
    /// dal repository non tornerebbero.
    fn punto(offset: i64) -> Coordinate {
        Coordinate::new(45.0 + offset as f64 / 1_000.0, 7.6869).expect("coordinata valida")
    }

    /// Osservatore di prova: conta le chiamate e restituisce uno stato programmato.
    ///
    /// I contatori sono atomici perche' il gestore della connessione li incrementa dal
    /// proprio task mentre il test li legge dal suo. `Ordering::Relaxed` basta: i
    /// contatori sono indipendenti fra loro e non fanno da guardia ad altri dati, e il
    /// test li legge solo dopo aver visto una risposta del server, che e' gia' un punto
    /// di sincronizzazione.
    struct OsservatoreDiProva {
        /// Stato restituito alla prima chiamata a `on_position`; dalla seconda in poi la
        /// risposta e' sempre `None`.
        primo_stato: Option<UserState>,
        /// Quante volte e' stato segnalato l'ingresso di un utente.
        connessioni: AtomicUsize,
        /// Quante volte e' stata segnalata la chiusura di una sessione.
        disconnessioni: AtomicUsize,
        /// Quante posizioni sono arrivate all'osservatore.
        posizioni: AtomicUsize,
    }

    impl OsservatoreDiProva {
        /// Costruisce un osservatore che risponde `primo_stato` alla prima posizione.
        fn nuovo(primo_stato: Option<UserState>) -> Self {
            Self {
                primo_stato,
                connessioni: AtomicUsize::new(0),
                disconnessioni: AtomicUsize::new(0),
                posizioni: AtomicUsize::new(0),
            }
        }

        fn connessioni(&self) -> usize {
            self.connessioni.load(Ordering::Relaxed)
        }

        fn disconnessioni(&self) -> usize {
            self.disconnessioni.load(Ordering::Relaxed)
        }

        fn posizioni(&self) -> usize {
            self.posizioni.load(Ordering::Relaxed)
        }
    }

    impl PositionObserver for OsservatoreDiProva {
        fn on_position(&self, _user_id: UserId, _coordinate: Coordinate) -> Option<UserState> {
            let precedenti = self.posizioni.fetch_add(1, Ordering::Relaxed);
            if precedenti == 0 {
                self.primo_stato
            } else {
                None
            }
        }

        fn on_connect(&self, _user_id: UserId) {
            self.connessioni.fetch_add(1, Ordering::Relaxed);
        }

        fn on_disconnect(&self, _user_id: UserId) {
            self.disconnessioni.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Server di prova in ascolto su una porta effimera.
    ///
    /// Non usa [`run`] ma un ciclo di accept equivalente, per due motivi: la porta 0
    /// va risolta in quella davvero assegnata, e il contesto deve restare in mano al
    /// test per poter ispezionare sessioni e connessioni registrate.
    struct Banco {
        /// Indirizzo su cui i client si collegano.
        indirizzo: SocketAddr,
        /// Dipendenze condivise, ispezionabili dal test.
        contesto: Arc<ConnectionContext>,
        /// Orologio del server, che il test puo' spostare a mano.
        orologio: Arc<FakeClock>,
        /// Segnale di spegnimento propagato ai gestori.
        spegnimento: broadcast::Sender<()>,
    }

    async fn avvia_banco() -> Banco {
        avvia_banco_completo(tempi(), Arc::new(NoopObserver)).await
    }

    async fn avvia_banco_con(timeouts: ConnectionTimeouts) -> Banco {
        avvia_banco_completo(timeouts, Arc::new(NoopObserver)).await
    }

    async fn avvia_banco_osservato(observer: Arc<dyn PositionObserver>) -> Banco {
        avvia_banco_completo(tempi(), observer).await
    }

    async fn avvia_banco_completo(
        timeouts: ConnectionTimeouts,
        observer: Arc<dyn PositionObserver>,
    ) -> Banco {
        let orologio = Arc::new(FakeClock::new(istante_base()));
        let sorgente: Arc<dyn Clock> = orologio.clone();
        let repo: Arc<dyn Repository> = Arc::new(InMemoryRepository::new(Arc::clone(&sorgente)));

        let contesto = Arc::new(ConnectionContext {
            repo,
            sessions: Arc::new(SessionStore::new()),
            registry: Arc::new(ConnectionRegistry::new()),
            clock: sorgente,
            observer,
            auth: autenticazione(),
            timeouts,
        });
        let (spegnimento, _) = broadcast::channel(1);

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind sulla porta effimera riuscito");
        let indirizzo = listener.local_addr().expect("indirizzo locale disponibile");

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
        }
    }

    impl Banco {
        /// Posizioni persistite per l'utente, dall'inizio dei tempi del banco in poi.
        ///
        /// L'intervallo e' volutamente largo: un test che verifica cosa e' stato scritto
        /// non deve poter fallire perche' la finestra di lettura era troppo stretta.
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
    }

    /// Client di prova: parla il protocollo con i codec veri.
    struct Cliente {
        /// Verso client -> server.
        scrittura: FramedWrite<OwnedWriteHalf, ClientCodec>,
        /// Verso server -> client.
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

        /// Vero se la connessione risulta chiusa alla lettura successiva.
        ///
        /// Distingue la chiusura dal semplice silenzio: un timeout significa che la
        /// connessione e' ancora aperta e il server non ha nulla da dire.
        async fn e_chiusa(&mut self) -> bool {
            match timeout(ATTESA, self.lettura.next()).await {
                Ok(None) => true,
                Ok(Some(Err(_))) => true,
                Ok(Some(Ok(_))) => false,
                Err(_) => false,
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

        /// Handshake seguito da registrazione; restituisce il token di sessione.
        async fn registra(&mut self, nome: &str) -> SessionToken {
            assert!(matches!(self.saluta().await, ServerMessage::Welcome { .. }));
            self.invia(ClientMessage::Register {
                username: utente(nome),
                password: PASSWORD.to_owned(),
            })
            .await;
            match self.ricevi().await {
                ServerMessage::SessionToken { token } => {
                    SessionToken::parse(&token).expect("il token deve avere la forma attesa")
                }
                altro => panic!("atteso SessionToken, ottenuto {altro:?}"),
            }
        }

        /// Handshake seguito da accesso; restituisce il token di sessione.
        async fn accedi(&mut self, nome: &str) -> SessionToken {
            assert!(matches!(self.saluta().await, ServerMessage::Welcome { .. }));
            self.invia(ClientMessage::Login {
                username: utente(nome),
                password: PASSWORD.to_owned(),
            })
            .await;
            match self.ricevi().await {
                ServerMessage::SessionToken { token } => {
                    SessionToken::parse(&token).expect("il token deve avere la forma attesa")
                }
                altro => panic!("atteso SessionToken, ottenuto {altro:?}"),
            }
        }
    }

    // --- analisi degli argomenti ---

    /// Trasforma gli argomenti di un test nella forma che [`ServerConfig::from_args`]
    /// si aspetta.
    fn argomenti(elenco: &[&str]) -> Vec<String> {
        elenco.iter().map(|voce| (*voce).to_owned()).collect()
    }

    #[test]
    fn senza_argomenti_valgono_i_valori_predefiniti() {
        let avvio = ServerConfig::from_args(argomenti(&[])).expect("analisi riuscita");
        assert_eq!(avvio, Avvio::Configurato(ServerConfig::default()));
    }

    #[test]
    fn bind_e_db_sovrascrivono_i_predefiniti() {
        let avvio = ServerConfig::from_args(argomenti(&[
            "--bind",
            "0.0.0.0:9000",
            "--db",
            "flotta.sqlite3",
        ]))
        .expect("analisi riuscita");

        let atteso = ServerConfig {
            bind_addr: "0.0.0.0:9000".parse().expect("indirizzo di test valido"),
            db_path: PathBuf::from("flotta.sqlite3"),
            log_dir: PathBuf::from("logs"),
        };
        assert_eq!(avvio, Avvio::Configurato(atteso));
    }

    #[test]
    fn senza_log_dir_vale_la_cartella_predefinita() {
        let avvio = ServerConfig::from_args(argomenti(&[])).expect("analisi riuscita");
        assert_eq!(
            avvio,
            Avvio::Configurato(ServerConfig {
                log_dir: PathBuf::from("logs"),
                ..ServerConfig::default()
            })
        );
    }

    #[test]
    fn log_dir_sovrascrive_la_cartella_predefinita() {
        let avvio = ServerConfig::from_args(argomenti(&["--log-dir", "/var/log/georuggine"]))
            .expect("analisi riuscita");

        let atteso = ServerConfig {
            log_dir: PathBuf::from("/var/log/georuggine"),
            ..ServerConfig::default()
        };
        assert_eq!(avvio, Avvio::Configurato(atteso));
    }

    #[test]
    fn log_dir_senza_valore_e_un_errore() {
        assert!(matches!(
            ServerConfig::from_args(argomenti(&["--log-dir"])),
            Err(ArgsError::ValoreMancante("--log-dir"))
        ));
    }

    #[test]
    fn l_uso_documenta_log_dir() {
        assert!(
            USO.contains("--log-dir <PERCORSO>"),
            "il testo di aiuto deve elencare --log-dir"
        );
    }

    #[test]
    fn help_chiede_di_stampare_l_uso() {
        let avvio = ServerConfig::from_args(argomenti(&["--help"])).expect("analisi riuscita");
        assert_eq!(avvio, Avvio::Uso);
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
            ServerConfig::from_args(argomenti(&["--bind", "non-un-indirizzo"])),
            Err(ArgsError::IndirizzoNonValido { .. })
        ));
    }

    // --- registro delle connessioni ---

    #[tokio::test]
    async fn il_registro_segnala_la_connessione_sostituita() {
        let registro = ConnectionRegistry::new();
        let (primo, _primo_ricevitore) = mpsc::channel(4);
        let (secondo, _secondo_ricevitore) = mpsc::channel(4);

        assert!(registro.register(UserId(1), primo).await.is_none());
        assert!(
            registro.register(UserId(1), secondo).await.is_some(),
            "la seconda registrazione deve restituire il canale precedente"
        );
    }

    #[tokio::test]
    async fn send_to_dice_se_l_utente_era_connesso() {
        let registro = ConnectionRegistry::new();
        let (mittente, mut ricevitore) = mpsc::channel(4);
        registro.register(UserId(1), mittente).await;

        assert!(registro.send_to(UserId(1), ServerMessage::Pong).await);
        assert_eq!(ricevitore.recv().await, Some(ServerMessage::Pong));
        assert!(!registro.send_to(UserId(2), ServerMessage::Pong).await);
    }

    #[tokio::test]
    async fn connected_users_elenca_gli_utenti_registrati() {
        let registro = ConnectionRegistry::new();
        let (primo, _a) = mpsc::channel(4);
        let (secondo, _b) = mpsc::channel(4);
        registro.register(UserId(2), primo).await;
        registro.register(UserId(1), secondo).await;

        assert_eq!(registro.connected_users().await, vec![UserId(1), UserId(2)]);

        registro.unregister(UserId(1)).await;
        assert_eq!(registro.connected_users().await, vec![UserId(2)]);
    }

    #[tokio::test]
    async fn broadcast_all_raggiunge_tutte_le_connessioni() {
        let registro = ConnectionRegistry::new();
        let (primo, mut primo_ricevitore) = mpsc::channel(4);
        let (secondo, mut secondo_ricevitore) = mpsc::channel(4);
        let (terzo, mut terzo_ricevitore) = mpsc::channel(4);
        registro.register(UserId(1), primo).await;
        registro.register(UserId(2), secondo).await;
        registro.register(UserId(3), terzo).await;

        assert_eq!(registro.broadcast_all(ServerMessage::Pong).await, 3);

        assert_eq!(primo_ricevitore.recv().await, Some(ServerMessage::Pong));
        assert_eq!(secondo_ricevitore.recv().await, Some(ServerMessage::Pong));
        assert_eq!(terzo_ricevitore.recv().await, Some(ServerMessage::Pong));
    }

    #[tokio::test]
    async fn broadcast_all_senza_connessioni_non_raggiunge_nessuno() {
        let registro = ConnectionRegistry::new();

        assert_eq!(registro.broadcast_all(ServerMessage::Pong).await, 0);
    }

    #[tokio::test]
    async fn broadcast_all_prosegue_oltre_un_destinatario_irraggiungibile() {
        let registro = ConnectionRegistry::new();
        let (primo, mut primo_ricevitore) = mpsc::channel(4);
        // Capacita' uno e un messaggio gia' accodato: la coda e' piena senza doverne
        // inviare a migliaia. Il ricevitore resta vivo, cosi' il canale e' pieno e non
        // chiuso, che sono due motivi diversi di fallimento.
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

    // --- handshake ---

    #[tokio::test]
    async fn hello_con_la_versione_giusta_riceve_welcome() {
        let banco = avvia_banco().await;
        let mut cliente = Cliente::connetti(banco.indirizzo).await;

        assert_eq!(
            cliente.saluta().await,
            ServerMessage::Welcome {
                protocol_version: PROTOCOL_VERSION
            }
        );
    }

    #[tokio::test]
    async fn hello_con_una_versione_sbagliata_chiude_la_connessione() {
        let banco = avvia_banco().await;
        let mut cliente = Cliente::connetti(banco.indirizzo).await;

        cliente
            .invia(ClientMessage::Hello {
                protocol_version: PROTOCOL_VERSION + 41,
            })
            .await;

        assert!(matches!(
            cliente.ricevi().await,
            ServerMessage::Error {
                code: ErrorCode::UnsupportedVersion,
                ..
            }
        ));
        assert!(
            cliente.e_chiusa().await,
            "dopo una versione rifiutata la connessione deve chiudersi"
        );
    }

    #[tokio::test]
    async fn senza_hello_l_handshake_scade_e_la_connessione_si_chiude() {
        let banco = avvia_banco().await;
        let mut cliente = Cliente::connetti(banco.indirizzo).await;

        // Non si invia nulla: il server deve chiudere allo scadere del timeout.
        assert!(cliente.e_chiusa().await);
    }

    // --- registrazione e accesso ---

    #[tokio::test]
    async fn register_apre_la_sessione_e_un_login_successivo_riesce() {
        let banco = avvia_banco().await;

        let mut primo = Cliente::connetti(banco.indirizzo).await;
        let token = primo.registra("veicolo_01").await;
        assert_eq!(
            banco.contesto.sessions.resolve(&token).await,
            Some(UserId(1))
        );

        // Nuova connessione, stesse credenziali: l'accesso deve riuscire.
        let mut secondo = Cliente::connetti(banco.indirizzo).await;
        let altro_token = secondo.accedi("veicolo_01").await;
        assert_ne!(altro_token, token, "ogni sessione ha un token suo");
        assert_eq!(
            banco.contesto.sessions.resolve(&altro_token).await,
            Some(UserId(1))
        );
    }

    #[tokio::test]
    async fn username_gia_preso_lascia_la_connessione_utilizzabile() {
        let banco = avvia_banco().await;
        let mut primo = Cliente::connetti(banco.indirizzo).await;
        primo.registra("veicolo_01").await;

        let mut secondo = Cliente::connetti(banco.indirizzo).await;
        assert!(matches!(
            secondo.saluta().await,
            ServerMessage::Welcome { .. }
        ));
        secondo
            .invia(ClientMessage::Register {
                username: utente("veicolo_01"),
                password: "diversa123".to_owned(),
            })
            .await;
        assert!(matches!(
            secondo.ricevi().await,
            ServerMessage::Error {
                code: ErrorCode::UsernameTaken,
                ..
            }
        ));

        // La connessione resta aperta: un accesso valido sulla stessa deve riuscire.
        secondo
            .invia(ClientMessage::Login {
                username: utente("veicolo_01"),
                password: PASSWORD.to_owned(),
            })
            .await;
        assert!(matches!(
            secondo.ricevi().await,
            ServerMessage::SessionToken { .. }
        ));
    }

    #[tokio::test]
    async fn credenziali_sbagliate_lasciano_la_connessione_aperta() {
        let banco = avvia_banco().await;
        let mut primo = Cliente::connetti(banco.indirizzo).await;
        primo.registra("veicolo_01").await;

        let mut secondo = Cliente::connetti(banco.indirizzo).await;
        assert!(matches!(
            secondo.saluta().await,
            ServerMessage::Welcome { .. }
        ));
        secondo
            .invia(ClientMessage::Login {
                username: utente("veicolo_01"),
                password: "sbagliata9".to_owned(),
            })
            .await;
        assert!(matches!(
            secondo.ricevi().await,
            ServerMessage::Error {
                code: ErrorCode::InvalidCredentials,
                ..
            }
        ));

        // Il client puo' riprovare sulla stessa connessione.
        secondo
            .invia(ClientMessage::Login {
                username: utente("veicolo_01"),
                password: PASSWORD.to_owned(),
            })
            .await;
        assert!(matches!(
            secondo.ricevi().await,
            ServerMessage::SessionToken { .. }
        ));
    }

    #[tokio::test]
    async fn una_password_debole_non_registra_l_utente() {
        let banco = avvia_banco().await;
        let mut cliente = Cliente::connetti(banco.indirizzo).await;
        assert!(matches!(
            cliente.saluta().await,
            ServerMessage::Welcome { .. }
        ));

        cliente
            .invia(ClientMessage::Register {
                username: utente("veicolo_01"),
                password: "corta".to_owned(),
            })
            .await;

        assert!(matches!(
            cliente.ricevi().await,
            ServerMessage::Error {
                code: ErrorCode::InvalidCredentials,
                ..
            }
        ));
    }

    // --- messaggi fuori sequenza ---

    #[tokio::test]
    async fn un_messaggio_prima_dell_accesso_riceve_not_authenticated() {
        let banco = avvia_banco().await;
        let mut cliente = Cliente::connetti(banco.indirizzo).await;
        assert!(matches!(
            cliente.saluta().await,
            ServerMessage::Welcome { .. }
        ));

        cliente
            .invia(ClientMessage::Position {
                coordinate: punto(0),
                recorded_at: istante_base(),
            })
            .await;

        assert!(matches!(
            cliente.ricevi().await,
            ServerMessage::Error {
                code: ErrorCode::NotAuthenticated,
                ..
            }
        ));

        // La connessione deve restare aperta: lo si dimostra usandola, non aspettando
        // un silenzio, che proverebbe solo che il server non ha ancora detto nulla.
        cliente
            .invia(ClientMessage::Register {
                username: utente("veicolo_01"),
                password: PASSWORD.to_owned(),
            })
            .await;
        assert!(matches!(
            cliente.ricevi().await,
            ServerMessage::SessionToken { .. }
        ));
    }

    // --- sessione aperta ---

    #[tokio::test]
    async fn ping_dopo_l_accesso_riceve_pong() {
        let banco = avvia_banco().await;
        let mut cliente = Cliente::connetti(banco.indirizzo).await;
        cliente.registra("veicolo_01").await;

        cliente.invia(ClientMessage::Ping).await;

        assert_eq!(cliente.ricevi().await, ServerMessage::Pong);
    }

    // --- posizioni ---

    #[tokio::test]
    async fn tre_posizioni_valide_finiscono_nel_repository() {
        let banco = avvia_banco().await;
        let mut cliente = Cliente::connetti(banco.indirizzo).await;
        cliente.registra("veicolo_01").await;
        let t0 = istante_base();

        for offset in [0, 10, 20] {
            cliente
                .invia(ClientMessage::Position {
                    coordinate: punto(offset),
                    recorded_at: t0 + TimeDelta::seconds(offset),
                })
                .await;
            assert_eq!(
                cliente.ricevi().await,
                ServerMessage::Ack {
                    request: AckKind::Position
                }
            );
        }

        let salvate = banco.posizioni_di(UserId(1));
        assert_eq!(
            salvate.iter().map(|p| p.recorded_at).collect::<Vec<_>>(),
            vec![t0, t0 + TimeDelta::seconds(10), t0 + TimeDelta::seconds(20),]
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
    async fn una_posizione_troppo_nel_futuro_e_rifiutata_e_non_persistita() {
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
    }

    #[tokio::test]
    async fn una_posizione_appena_dentro_la_tolleranza_e_accettata() {
        let banco = avvia_banco().await;
        let mut cliente = Cliente::connetti(banco.indirizzo).await;
        cliente.registra("veicolo_01").await;

        // Il limite e' l'anticipo massimo esatto: l'orologio del client puo' essere
        // avanti di poco senza che le sue rilevazioni vengano buttate via.
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
    async fn oltre_dieci_posizioni_al_minuto_arriva_rate_limited() {
        let banco = avvia_banco().await;
        let mut cliente = Cliente::connetti(banco.indirizzo).await;
        cliente.registra("veicolo_01").await;

        for tentativo in 0..15 {
            cliente
                .invia(ClientMessage::Position {
                    coordinate: punto(tentativo),
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
    }

    #[tokio::test]
    async fn la_finestra_del_limite_riparte_dopo_un_minuto() {
        let banco = avvia_banco().await;
        let mut cliente = Cliente::connetti(banco.indirizzo).await;
        cliente.registra("veicolo_01").await;

        for _ in 0..=MAX_POSIZIONI_PER_FINESTRA {
            cliente
                .invia(ClientMessage::Position {
                    coordinate: punto(0),
                    recorded_at: istante_base(),
                })
                .await;
            cliente.ricevi().await;
        }

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

    #[tokio::test]
    async fn lo_stato_restituito_dall_osservatore_raggiunge_il_client() {
        let osservatore = Arc::new(OsservatoreDiProva::nuovo(Some(UserState::Moving)));
        let banco =
            avvia_banco_osservato(Arc::clone(&osservatore) as Arc<dyn PositionObserver>).await;
        let mut cliente = Cliente::connetti(banco.indirizzo).await;
        cliente.registra("veicolo_01").await;

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
            "l'Ack arriva comunque, prima dell'eventuale cambio di stato"
        );
        assert_eq!(
            cliente.ricevi().await,
            ServerMessage::StateChanged {
                state: UserState::Moving
            }
        );

        // Dalla seconda posizione l'osservatore non segnala piu' nulla: dopo l'Ack il
        // client deve ricevere il Pong, non un altro StateChanged.
        cliente
            .invia(ClientMessage::Position {
                coordinate: punto(1),
                recorded_at: istante_base(),
            })
            .await;
        assert_eq!(
            cliente.ricevi().await,
            ServerMessage::Ack {
                request: AckKind::Position
            }
        );
        cliente.invia(ClientMessage::Ping).await;
        assert_eq!(cliente.ricevi().await, ServerMessage::Pong);
    }

    #[tokio::test]
    async fn senza_cambio_di_stato_il_client_riceve_solo_ack() {
        let osservatore = Arc::new(OsservatoreDiProva::nuovo(None));
        let banco =
            avvia_banco_osservato(Arc::clone(&osservatore) as Arc<dyn PositionObserver>).await;
        let mut cliente = Cliente::connetti(banco.indirizzo).await;
        cliente.registra("veicolo_01").await;

        for offset in 0..3 {
            cliente
                .invia(ClientMessage::Position {
                    coordinate: punto(offset),
                    recorded_at: istante_base(),
                })
                .await;
            assert_eq!(
                cliente.ricevi().await,
                ServerMessage::Ack {
                    request: AckKind::Position
                }
            );
        }

        // Un Pong subito dopo l'ultimo Ack dimostra che in mezzo non e' passato nessuno
        // StateChanged: sarebbe arrivato prima, perche' la coda conserva l'ordine.
        cliente.invia(ClientMessage::Ping).await;
        assert_eq!(cliente.ricevi().await, ServerMessage::Pong);
        assert_eq!(
            osservatore.posizioni(),
            3,
            "l'osservatore va interpellato per ogni posizione persistita"
        );
    }

    #[tokio::test]
    async fn on_connect_e_on_disconnect_valgono_una_volta_per_connessione() {
        let osservatore = Arc::new(OsservatoreDiProva::nuovo(None));
        let banco =
            avvia_banco_osservato(Arc::clone(&osservatore) as Arc<dyn PositionObserver>).await;
        let mut cliente = Cliente::connetti(banco.indirizzo).await;
        cliente.registra("veicolo_01").await;

        assert_eq!(osservatore.connessioni(), 1);
        assert_eq!(osservatore.disconnessioni(), 0);

        cliente.invia(ClientMessage::Logout).await;
        assert!(matches!(
            cliente.ricevi().await,
            ServerMessage::Ack {
                request: AckKind::Logout
            }
        ));
        // La chiusura del flusso arriva dopo lo sgancio della sessione: quando il client
        // la vede, `on_disconnect` e' gia' stato chiamato.
        assert!(cliente.e_chiusa().await);

        assert_eq!(osservatore.connessioni(), 1);
        assert_eq!(osservatore.disconnessioni(), 1);
    }

    #[tokio::test]
    async fn una_connessione_mai_autenticata_non_muove_l_osservatore() {
        let osservatore = Arc::new(OsservatoreDiProva::nuovo(None));
        let banco =
            avvia_banco_osservato(Arc::clone(&osservatore) as Arc<dyn PositionObserver>).await;
        let mut cliente = Cliente::connetti(banco.indirizzo).await;
        assert!(matches!(
            cliente.saluta().await,
            ServerMessage::Welcome { .. }
        ));

        // Chiudendo il client il gestore esce dal ciclo e passa dalla pulizia finale,
        // che su una connessione senza sessione non deve segnalare nulla. L'attesa da'
        // al gestore il tempo di arrivarci: senza, il test passerebbe anche se la
        // pulizia sbagliasse, semplicemente perche' non e' ancora avvenuta.
        drop(cliente);
        tokio::time::sleep(Duration::from_millis(100)).await;

        assert_eq!(osservatore.connessioni(), 0);
        assert_eq!(osservatore.disconnessioni(), 0);
        assert!(banco.contesto.registry.connected_users().await.is_empty());
    }

    // --- limite di frequenza ---

    #[test]
    fn il_limitatore_ammette_dieci_posizioni_per_finestra() {
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
    }

    #[test]
    fn il_limitatore_riapre_la_finestra_a_scadenza() {
        let t0 = istante_base();
        let mut limitatore = Limitatore::nuovo(t0);
        for _ in 0..=MAX_POSIZIONI_PER_FINESTRA {
            limitatore.consenti(t0);
        }

        assert!(limitatore.consenti(t0 + TimeDelta::seconds(FINESTRA_SECS)));
    }

    #[tokio::test]
    async fn logout_sgancia_la_sessione_dai_due_registri() {
        let banco = avvia_banco().await;
        let mut cliente = Cliente::connetti(banco.indirizzo).await;
        let token = cliente.registra("veicolo_01").await;
        assert_eq!(
            banco.contesto.registry.connected_users().await,
            vec![UserId(1)]
        );

        cliente.invia(ClientMessage::Logout).await;

        assert!(matches!(
            cliente.ricevi().await,
            ServerMessage::Ack {
                request: AckKind::Logout
            }
        ));
        assert!(cliente.e_chiusa().await, "il logout chiude la connessione");
        assert_eq!(
            banco.contesto.sessions.resolve(&token).await,
            None,
            "il token non deve piu' risolvere"
        );
        assert!(
            banco.contesto.registry.connected_users().await.is_empty(),
            "l'utente non deve piu' risultare connesso"
        );
    }

    #[tokio::test]
    async fn una_seconda_sessione_dello_stesso_utente_chiude_la_prima() {
        let banco = avvia_banco().await;

        let mut primo = Cliente::connetti(banco.indirizzo).await;
        primo.registra("veicolo_01").await;

        let mut secondo = Cliente::connetti(banco.indirizzo).await;
        secondo.accedi("veicolo_01").await;

        assert!(
            primo.e_chiusa().await,
            "la connessione precedente dello stesso utente va chiusa"
        );
        // La seconda resta viva e utilizzabile.
        secondo.invia(ClientMessage::Ping).await;
        assert_eq!(secondo.ricevi().await, ServerMessage::Pong);
        assert_eq!(
            banco.contesto.registry.connected_users().await,
            vec![UserId(1)],
            "resta registrata una sola connessione per utente"
        );
    }

    #[tokio::test]
    async fn l_inattivita_chiude_la_connessione() {
        let banco = avvia_banco_con(ConnectionTimeouts {
            idle: Duration::from_millis(200),
            ..tempi()
        })
        .await;
        let mut cliente = Cliente::connetti(banco.indirizzo).await;
        let token = cliente.registra("veicolo_01").await;

        // Non si invia piu' nulla: allo scadere del timeout di inattivita' il server
        // chiude e libera la sessione, come farebbe un logout.
        assert!(cliente.e_chiusa().await);
        assert_eq!(banco.contesto.sessions.resolve(&token).await, None);
    }

    #[tokio::test]
    async fn lo_spegnimento_chiude_le_connessioni_aperte() {
        let banco = avvia_banco().await;
        let mut cliente = Cliente::connetti(banco.indirizzo).await;
        cliente.registra("veicolo_01").await;

        banco
            .spegnimento
            .send(())
            .expect("almeno un gestore e' in ascolto");

        assert!(cliente.e_chiusa().await);
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

        // Header che dichiara 4 byte seguito da byte che non compongono un
        // ClientMessage: il codec produce un errore di serializzazione, che lascia il
        // flusso disallineato e quindi non e' recuperabile.
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
