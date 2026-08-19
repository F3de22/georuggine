//! Impalcatura condivisa dai sei scenari end-to-end.
//!
//! # Che cosa monta
//!
//! [`TestServer`] avvia il server vero, cioe' [`net::run_con`] con le stesse dipendenze
//! che costruisce il binario, su una porta effimera del loopback; [`TestClient`] avvia il
//! client vero, cioe' [`ClientHandle::spawn_con`], e lo pilota dai suoi due canali di
//! comandi ed eventi. Fra i due passa un socket TCP e il protocollo di
//! `georuggine-common`: nessuno dei due sa di essere dentro un test. Dove serve parlare
//! al server fuori dalla sequenza che il client rispetta — una versione di protocollo
//! sbagliata, una richiesta prima dell'accesso — c'e' [`ClientGrezzo`], che scrive i
//! messaggi a mano sugli stessi codec.
//!
//! # I due tempi
//!
//! Nei test convivono due tempi diversi, e confonderli porta a scenari lenti o instabili.
//!
//! Il **tempo di dominio** e' quello che server e client leggono dal [`Clock`]: ci
//! finiscono il `recorded_at` di ogni posizione, la scadenza dei tre minuti della
//! macchina a stati, gli estremi delle analisi e la finestra del limitatore di frequenza.
//! Qui l'orologio e' un [`FakeClock`] **condiviso fra i due programmi**, che lo scenario
//! sposta con [`TestServer::avanza`]: cosi' l'istante che il client scrive nella posizione
//! e quello con cui il server la giudica sono lo stesso, e tre minuti di requisito costano
//! microsecondi invece di tre minuti di attesa.
//!
//! Il **tempo vero** e' quello dei timer di tokio: l'intervallo del ticker delle
//! posizioni, il backoff di riconnessione, le scadenze di una connessione e il battito
//! del task periodico. Nessun orologio finto lo puo' saltare, quindi lo si accorcia:
//! [`ConnectionTimeouts`] e [`Tempi`] sono parametri proprio per questo.
//!
//! # Perche' la registrazione sta fuori dal client
//!
//! [`TestClient::accedi`] apre sempre la sessione con `Login`, mai con `Register`. Il
//! motivo e' la riconnessione: il task di rete ripete la stessa richiesta di
//! autenticazione a ogni tentativo, e un client nato con `register: true` che perde la
//! connessione si ritroverebbe con [`ErrorCode::UsernameTaken`], che e' un fallimento
//! terminale, e smetterebbe di riprovare invece di rientrare. L'utente si crea percio'
//! prima, con [`TestServer::registra`]. Fa eccezione [`TestClient::registrandosi`], che
//! verifica la registrazione dal filo e va usata solo dove nessuna caduta e' prevista.
//!
//! [`ErrorCode::UsernameTaken`]: georuggine_common::protocol::ErrorCode::UsernameTaken

// Ogni scenario e' un binario di test a se' e compila questo modulo per intero, ma ne usa
// solo la parte che gli serve: senza questo permesso cio' che un singolo scenario non
// tocca diventerebbe un warning, e la build e' con `-D warnings`.
#![allow(dead_code)]

use std::net::SocketAddr;
use std::panic::PanicHookInfo;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError};
use std::thread::ThreadId;
use std::time::Duration;

use chrono::{DateTime, TimeDelta, Utc};
use futures_util::{SinkExt as _, StreamExt as _};
use georuggine_client::core::{ClientCommand, ClientConfig, ClientEvent, ClientHandle, Tempi};
use georuggine_client::sources::SourceKind;
use georuggine_common::clock::{Clock, FakeClock};
use georuggine_common::codec::{ClientCodec, ServerCodec};
use georuggine_common::protocol::{
    ClientMessage, MovementSummary, PROTOCOL_VERSION, ServerMessage,
};
use georuggine_common::types::{Coordinate, PositionRecord, UserId, UserState, Username};
use georuggine_server::auth::{self, AuthConfig, SessionStore};
use georuggine_server::messaging::MessageHub;
use georuggine_server::net::{
    self, ConnectionRegistry, ConnectionTimeouts, Dipendenze, Osservazione, Regolazioni,
    ServerConfig,
};
use georuggine_server::state::StateTracker;
use georuggine_server::storage::{
    MessageDirection, MessageId, PendingMessage, Repository, SqliteRepository, StorageError,
    StoredUser,
};
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use tokio::time::{Instant, sleep, timeout};
use tokio_util::codec::{FramedRead, FramedWrite};

/// Quanto uno scenario aspetta cio' che si attende prima di dichiararlo mancato.
///
/// E' generoso perche' lo si paga solo quando il test sta gia' fallendo: nel caso normale
/// l'evento arriva in pochi millisecondi. Su una macchina carica, o in una build di debug,
/// qualche secondo di margine evita i fallimenti che non dipendono dal codice in esame.
pub const ATTESA: Duration = Duration::from_secs(10);

/// Margine che [`TestServer::spegni`] concede alle connessioni dopo che il ciclo di
/// accettazione e' finito.
///
/// Il ciclo termina appena riceve il segnale, ma le connessioni che ha spawnato sono
/// ancora dentro la propria chiusura: senza questa finestra un panic che avviene proprio
/// li' non sarebbe ancora stato registrato dalla spia. Basta poco, perche' sotto un
/// runtime `current_thread` questa attesa cede lo scheduler e i task pronti arrivano in
/// fondo; non e' un'attesa attiva su una scadenza lunga.
const ATTESA_CHIUSURA_CONNESSIONI: Duration = Duration::from_millis(50);

/// Intervallo fra due letture quando si aspetta una condizione che non ha un evento.
///
/// Riguarda lo stato che vive dentro il server — il registro delle connessioni, la
/// macchina a stati — e che dall'esterno si puo' solo rileggere finche' non cambia.
const PASSO_DI_ATTESA: Duration = Duration::from_millis(10);

/// Password che rispetta la policy di `georuggine_server::auth::valida_password`.
pub const PASSWORD: &str = "veicolo12";

/// Password che la policy rifiuta: troppo corta e senza cifre.
pub const PASSWORD_DEBOLE: &str = "corta";

/// Intervallo del battito della macchina a stati nei test.
///
/// E' tempo vero, quindi si accorcia: e' solo la frequenza con cui il task periodico si
/// sveglia a chiedere chi e' tornato fermo. La soglia dei tre minuti resta invece quella
/// di produzione, perche' e' tempo di dominio e la si supera muovendo il [`FakeClock`].
const BATTITO: Duration = Duration::from_millis(20);

/// Attesa concessa a un cambio di stato che puo' esserci oppure no.
///
/// La usa [`ClientGrezzo::scarta_ritorno_a_fermo`] dove il battito della macchina a stati
/// e' nel suo diritto di accodare un [`ServerMessage::StateChanged`] ma non e' detto che
/// lo abbia gia' fatto: l'assenza di un messaggio entro questa finestra e' un esito
/// normale e non un fallimento, quindi la scadenza e' breve invece che generosa come
/// [`ATTESA`], che si paga solo quando uno scenario sta gia' fallendo.
///
/// Sono tre periodi di [`BATTITO`] e non uno solo perche' il battito e' tempo vero: su una
/// macchina carica uno scatto puo' arrivare in ritardo sul proprio periodo, e una finestra
/// larga quanto il periodo lo mancherebbe. Restano poche decine di millisecondi, pagate
/// una volta per scenario e solo nel caso in cui il cambio non arrivi.
const ATTESA_DEL_BATTITO: Duration = BATTITO.saturating_mul(3);

/// Istante su cui parte l'orologio finto: 2024-01-01T12:00:00Z.
///
/// E' mezzogiorno e non mezzanotte perche' gli intervalli di calendario delle analisi si
/// risolvono nel fuso locale della macchina: partendo da meta' giornata, i pochi minuti di
/// tempo di dominio che uno scenario attraversa non scavalcano mai un cambio di data.
pub fn istante_base() -> DateTime<Utc> {
    DateTime::from_timestamp(1_704_110_400, 0).expect("istante di test valido")
}

/// Username che il test da' per valido.
pub fn utente(nome: &str) -> Username {
    Username::new(nome).expect("username di test valido")
}

/// Coordinata che il test da' per valida.
pub fn coordinata(lat: f64, lon: f64) -> Coordinate {
    Coordinate::new(lat, lon).expect("coordinata di test valida")
}

/// Punto di partenza dei percorsi di prova: piazza Castello, Torino.
pub fn partenza() -> Coordinate {
    coordinata(45.0703, 7.6869)
}

/// Punto circa un metro a nord di [`partenza`], quindi sotto la soglia di movimento.
pub fn a_un_metro() -> Coordinate {
    coordinata(45.0703 + 0.000_009, 7.6869)
}

/// Punto circa cinquanta metri a nord di [`partenza`]: dieci volte la soglia di movimento,
/// abbastanza perche' il test non dipenda dalla precisione della formula.
pub fn a_cinquanta_metri() -> Coordinate {
    coordinata(45.0703 + 0.000_45, 7.6869)
}

/// Punto circa cento metri a nord di [`partenza`], dove serve un terzo punto distante da
/// entrambi gli altri.
pub fn a_cento_metri() -> Coordinate {
    coordinata(45.0703 + 0.000_9, 7.6869)
}

/// Tempi con cui il ticker delle posizioni non scatta mai da solo.
///
/// L'intervallo e' un'ora di tempo vero, cioe' molto oltre la durata di qualunque
/// scenario: il client resta muto e le sole cose che il server vede sono quelle che il
/// test gli fa fare. Serve a tutti gli scenari che non parlano di posizioni, dove un invio
/// periodico sarebbe rumore — e finirebbe per esaurire il limite di frequenza, dato che la
/// finestra del limitatore non si riapre mentre l'orologio di dominio sta fermo.
///
/// Il backoff e' invece cortissimo, perche' una riconnessione non deve costare secondi.
pub fn tempi_fermi() -> Tempi {
    Tempi {
        intervallo_posizione: Duration::from_secs(3_600),
        backoff: [
            Duration::from_millis(20),
            Duration::from_millis(40),
            Duration::from_millis(60),
            Duration::from_millis(80),
        ],
        backoff_massimo: Duration::from_millis(100),
    }
}

/// Tempi con cui il ticker delle posizioni scatta di continuo.
///
/// Serve agli scenari che verificano l'invio periodico e la coda delle posizioni prodotte
/// mentre la connessione e' caduta. Il ticker ripete l'ultima coordinata nota, quindi fra
/// un invio voluto e il successivo il server riceve dei duplicati: sono innocui per la
/// macchina a stati e per le analisi, perche' non spostano nulla, ma contano nel limite di
/// frequenza, e per questo gli scenari che li usano restano brevi oppure spostano
/// l'orologio di dominio, cosa che riapre la finestra del limitatore.
pub fn tempi_con_invii() -> Tempi {
    Tempi {
        intervallo_posizione: Duration::from_millis(50),
        ..tempi_fermi()
    }
}

/// Scadenze del server nei test.
///
/// L'inattivita' resta lunga apposta: quasi tutti gli scenari verificano che la connessione
/// si chiuda per un motivo preciso, e una scadenza breve la chiuderebbe comunque, facendoli
/// passare anche se il meccanismo in esame non funzionasse. Chi vuole verificare proprio
/// quella scadenza la accorcia da se'.
pub fn timeouts_di_prova() -> ConnectionTimeouts {
    ConnectionTimeouts {
        handshake: Duration::from_secs(2),
        idle: Duration::from_secs(30),
        shutdown: Duration::from_millis(200),
        tick_interval: BATTITO,
    }
}

/// Parametri di Argon2 al minimo utile.
///
/// L'hashing di produzione e' lento apposta, e decine di millisecondi per ogni accesso,
/// moltiplicate per i client di sei scenari, si sentono. Il costo dell'hashing non fa
/// parte di cio' che questi test verificano.
pub fn autenticazione_rapida() -> AuthConfig {
    AuthConfig {
        memory_kib: 32,
        iterations: 1,
        parallelism: 1,
    }
}

/// Messaggio come la spia lo ha visto passare verso la persistenza.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessaggioSpiato {
    /// Destinatario, assente per gli avvisi alla flotta.
    pub destinatario: Option<UserId>,
    /// Verso rispetto al server.
    pub verso: MessageDirection,
    /// Corpo del messaggio.
    pub corpo: String,
}

/// Persistenza vera con un registratore davanti.
///
/// Il [`Repository`] espone in lettura le posizioni e i messaggi ancora da consegnare, ma
/// non quelli che un veicolo ha mandato al server: sono scritti e basta, e dall'esterno non
/// si rileggono. Uno scenario end-to-end del requisito R13 non avrebbe percio' modo di dire
/// se il testo sia arrivato davvero fino al database. La spia riempie quel buco senza
/// toccare il server: il livello di rete continua a vedere un `Arc<dyn Repository>`
/// qualunque, e ogni chiamata prosegue verso lo SQLite sottostante.
pub struct RepositorySpia {
    /// Persistenza vera: uno SQLite in memoria, che vive quanto il test.
    interno: SqliteRepository,
    /// Messaggi passati di qui, nell'ordine in cui sono stati scritti.
    messaggi: Mutex<Vec<MessaggioSpiato>>,
}

impl RepositorySpia {
    /// Apre un database in memoria e vi mette davanti il registratore.
    fn nuova(orologio: Arc<dyn Clock>) -> Self {
        Self {
            interno: SqliteRepository::new(":memory:", orologio)
                .expect("database in memoria apribile"),
            messaggi: Mutex::new(Vec::new()),
        }
    }

    /// Tutti i messaggi passati dalla persistenza, in ordine di scrittura.
    pub fn messaggi(&self) -> Vec<MessaggioSpiato> {
        self.blocca().clone()
    }

    /// Corpi dei soli messaggi di un verso, in ordine di scrittura.
    pub fn corpi_con_verso(&self, verso: MessageDirection) -> Vec<String> {
        self.blocca()
            .iter()
            .filter(|messaggio| messaggio.verso == verso)
            .map(|messaggio| messaggio.corpo.clone())
            .collect()
    }

    /// Prende il lock sul registro, riprendendolo anche se e' avvelenato.
    ///
    /// Come altrove nel progetto: la sezione critica e' un `push` o una copia, quindi non
    /// puo' lasciare lo stato a meta', e si continua invece di andare in panico.
    fn blocca(&self) -> MutexGuard<'_, Vec<MessaggioSpiato>> {
        self.messaggi.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl Repository for RepositorySpia {
    fn create_user(
        &self,
        username: &Username,
        password_hash: &str,
    ) -> Result<UserId, StorageError> {
        self.interno.create_user(username, password_hash)
    }

    fn find_user_by_username(
        &self,
        username: &Username,
    ) -> Result<Option<StoredUser>, StorageError> {
        self.interno.find_user_by_username(username)
    }

    fn insert_position(&self, record: &PositionRecord) -> Result<(), StorageError> {
        self.interno.insert_position(record)
    }

    fn positions_in_range(
        &self,
        user_id: UserId,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<Vec<PositionRecord>, StorageError> {
        self.interno.positions_in_range(user_id, from, to)
    }

    fn insert_message(
        &self,
        user_id: Option<UserId>,
        direction: MessageDirection,
        body: &str,
        at: DateTime<Utc>,
    ) -> Result<MessageId, StorageError> {
        // Si registra prima di scrivere e non dopo: il test vuole sapere che cosa il
        // server ha chiesto di persistere, anche quando la scrittura fallisce.
        self.blocca().push(MessaggioSpiato {
            destinatario: user_id,
            verso: direction,
            corpo: body.to_owned(),
        });
        self.interno.insert_message(user_id, direction, body, at)
    }

    fn pending_messages(&self, user_id: UserId) -> Result<Vec<PendingMessage>, StorageError> {
        self.interno.pending_messages(user_id)
    }

    fn mark_delivered(&self, id: MessageId) -> Result<(), StorageError> {
        self.interno.mark_delivered(id)
    }

    fn clear_pending_messages(&self, user_id: UserId) -> Result<(), StorageError> {
        self.interno.clear_pending_messages(user_id)
    }

    fn list_users(&self) -> Result<Vec<StoredUser>, StorageError> {
        self.interno.list_users()
    }
}

// --- spia sui panic dei task del server ---
//
// Il server vive in task `tokio::spawn`ati di cui nessuno tiene la `JoinHandle`: le
// connessioni, il loro task di scrittura, il battito della macchina a stati. Un panic
// dentro uno di quei task non fa fallire niente — tokio lo cattura, il task muore e lo
// scenario prosegue — e in piu' libtest cattura lo stderr per test, quindi senza
// `--nocapture` non se ne vede nemmeno la traccia. E' cosi' che il doppio poll della
// `JoinHandle` del task di scrittura in `handle_connection` e' rimasto invisibile pur
// scattando a ogni riconnessione.
//
// Il gancio qui sotto lo rende un fallimento vero. Non serve raggiungere le handle dei
// task — cosa che non si puo' fare da fuori senza allargare l'API di `net` — perche' ogni
// `#[tokio::test]` gira su un runtime `current_thread` sul proprio thread: i task del
// server di quello scenario sono eseguiti su quel thread, e un panic al loro interno
// porta l'identita' di quel thread. Registrandola si attribuisce ogni panic allo scenario
// che lo ha provocato, anche con i test in parallelo.
//
// Restano fuori i soli `spawn_blocking`, che girano sul pool dei thread bloccanti e non
// hanno l'identita' dello scenario: la' dentro c'e' pero' solo l'accesso allo SQLite, che
// gia' propaga i suoi errori come `Result`.

/// Un panic osservato dal gancio globale.
struct PanicSpiato {
    /// Thread su cui e' avvenuto, cioe' lo scenario a cui appartiene.
    thread: ThreadId,
    /// Messaggio e posizione, gia' formattati per il rapporto.
    descrizione: String,
}

/// Panic osservati e non ancora rivendicati da nessuno scenario.
static PANIC_SPIATI: OnceLock<Mutex<Vec<PanicSpiato>>> = OnceLock::new();

/// Ricordo che il gancio e' gia' stato installato: va messo una volta per binario.
static SPIA_ARMATA: OnceLock<()> = OnceLock::new();

/// Accesso alla lista dei panic osservati.
///
/// Il lock avvelenato non e' un caso da trattare, per lo stesso motivo di
/// [`RepositorySpia::blocca`]: dentro c'e' una lista, e una lista non si corrompe perche'
/// chi la teneva e' andato in panico.
fn panic_spiati() -> MutexGuard<'static, Vec<PanicSpiato>> {
    PANIC_SPIATI
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
}

/// Installa il gancio sui panic, una volta sola per binario di test.
///
/// Il gancio precedente viene richiamato in coda, cosi' il messaggio e la traccia
/// continuano ad arrivare su stderr come sempre: qui si aggiunge la registrazione, non si
/// sostituisce il comportamento normale.
fn arma_la_spia_sui_panic() {
    SPIA_ARMATA.get_or_init(|| {
        let precedente = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |informazioni| {
            panic_spiati().push(PanicSpiato {
                thread: std::thread::current().id(),
                descrizione: descrivi_panic(informazioni),
            });
            precedente(informazioni);
        }));
    });
}

/// Riduce un panic a una riga con messaggio e posizione.
fn descrivi_panic(informazioni: &PanicHookInfo<'_>) -> String {
    let carico = informazioni.payload();
    let messaggio = if let Some(testo) = carico.downcast_ref::<&str>() {
        (*testo).to_owned()
    } else if let Some(testo) = carico.downcast_ref::<String>() {
        testo.clone()
    } else {
        "panic senza messaggio testuale".to_owned()
    };

    match informazioni.location() {
        Some(dove) => format!("{messaggio} (a {dove})"),
        None => messaggio,
    }
}

/// Toglie dalla lista i panic avvenuti su questo scenario e li restituisce.
///
/// Si tolgono invece di limitarsi a leggerli perche' un panic va riportato una volta
/// sola: [`TestServer::spegni`] lo rivendica, e il `Drop` che viene subito dopo non lo
/// ritrova piu'.
fn panic_di_questo_scenario() -> Vec<String> {
    let corrente = std::thread::current().id();
    let mut spiati = panic_spiati();

    let (miei, altrui): (Vec<PanicSpiato>, Vec<PanicSpiato>) = std::mem::take(&mut *spiati)
        .into_iter()
        .partition(|spiato| spiato.thread == corrente);
    *spiati = altrui;

    miei.into_iter().map(|spiato| spiato.descrizione).collect()
}

/// Fa fallire lo scenario se un task del server e' andato in panico senza dirlo.
fn pretendi_nessun_panic_nei_task() {
    let panici = panic_di_questo_scenario();
    assert!(
        panici.is_empty(),
        "un task del server e' andato in panico senza far fallire lo scenario da se':\n  - {}",
        panici.join("\n  - ")
    );
}

/// Il server vero, in ascolto su una porta effimera del loopback.
///
/// Tiene un riferimento a tutto cio' che ha passato a [`net::run_con`], cosi' lo scenario
/// puo' guardare il server da dentro — chi e' connesso, in che stato e' un veicolo, che
/// cosa e' finito a storico — mentre lo interroga da fuori attraverso il protocollo.
pub struct TestServer {
    /// Indirizzo a cui i client si collegano.
    pub indirizzo: SocketAddr,
    /// Orologio di dominio, condiviso con i client dello scenario.
    pub orologio: Arc<FakeClock>,
    /// Persistenza, con la spia sui messaggi.
    pub archivio: Arc<RepositorySpia>,
    /// Token di sessione attivi.
    pub sessioni: Arc<SessionStore>,
    /// Connessioni aperte, una per utente.
    pub registro: Arc<ConnectionRegistry>,
    /// Messaggistica: e' da qui che lo scenario fa parlare il server.
    pub messaggistica: Arc<MessageHub>,
    /// Macchina a stati fermo/movimento.
    pub stati: Arc<StateTracker>,
    /// Segnale di spegnimento, condiviso con le connessioni.
    spegnimento: broadcast::Sender<()>,
    /// Ciclo di accettazione, atteso da [`TestServer::spegni`].
    ///
    /// E' un'opzione perche' `spegni` prende `self` per valore ma la struttura ha un
    /// `Drop`: da `self` non si puo' spostare via un campo, quindi la handle si prende
    /// con `take` invece che per movimento.
    ciclo: Option<JoinHandle<std::io::Result<()>>>,
}

impl TestServer {
    /// Avvia il server con le scadenze di [`timeouts_di_prova`].
    pub async fn avvia() -> Self {
        Self::con_timeouts(timeouts_di_prova()).await
    }

    /// Avvia il server con le scadenze indicate.
    ///
    /// Il ritorno avviene solo quando la porta accetta davvero connessioni: senza questa
    /// attesa il primo client potrebbe presentarsi prima che [`net::run_con`] abbia aperto
    /// l'ascolto e vedersi rifiutare la connessione.
    pub async fn con_timeouts(timeouts: ConnectionTimeouts) -> Self {
        // Prima di far partire qualunque task del server: da qui in poi un panic al loro
        // interno non e' piu' silenzioso.
        arma_la_spia_sui_panic();

        let orologio = Arc::new(FakeClock::new(istante_base()));
        let sorgente_ora: Arc<dyn Clock> = Arc::clone(&orologio) as Arc<dyn Clock>;

        let archivio = Arc::new(RepositorySpia::nuova(Arc::clone(&sorgente_ora)));
        let repo: Arc<dyn Repository> = Arc::clone(&archivio) as Arc<dyn Repository>;
        let sessioni = Arc::new(SessionStore::new());
        let registro = Arc::new(ConnectionRegistry::new());
        // La messaggistica riceve lo stesso registro del livello di rete, come nel
        // binario: un secondo registro non vedrebbe nessuno dei veicoli collegati.
        let messaggistica = Arc::new(MessageHub::new(
            Arc::clone(&repo),
            Arc::clone(&registro),
            Arc::clone(&sorgente_ora),
        ));
        let stati = Arc::new(StateTracker::new(Arc::clone(&sorgente_ora)));
        let (spegnimento, _) = broadcast::channel(1);

        let indirizzo = porta_libera();
        let configurazione = ServerConfig {
            bind_addr: indirizzo,
            // Nessuno dei due percorsi viene aperto da `run_con`: il database e' gia'
            // aperto qui sopra, e il log del tempo di CPU vive nel binario.
            db_path: PathBuf::from(":memory:"),
            log_dir: PathBuf::from("logs"),
        };
        let ciclo = tokio::spawn(net::run_con(
            configurazione,
            Dipendenze {
                repo,
                sessions: Arc::clone(&sessioni),
                registry: Arc::clone(&registro),
                messaging: Arc::clone(&messaggistica),
                clock: Arc::clone(&sorgente_ora),
            },
            Osservazione::con_stati(Arc::clone(&stati)),
            Regolazioni {
                auth: autenticazione_rapida(),
                timeouts,
            },
            spegnimento.clone(),
        ));

        attendi_ascolto(indirizzo).await;

        Self {
            indirizzo,
            orologio,
            archivio,
            sessioni,
            registro,
            messaggistica,
            stati,
            spegnimento,
            ciclo: Some(ciclo),
        }
    }

    /// Registra un utente senza passare dalla rete e ne restituisce l'id.
    ///
    /// E' il preparativo di quasi tutti gli scenari: il client di prova accede e basta,
    /// per il motivo spiegato in cima al modulo. Usa la stessa funzione del livello di
    /// rete, quindi l'hash della password e' quello vero.
    pub async fn registra(&self, nome: &str) -> UserId {
        let repo: Arc<dyn Repository> = Arc::clone(&self.archivio) as Arc<dyn Repository>;

        auth::register(&repo, &autenticazione_rapida(), &utente(nome), PASSWORD)
            .await
            .expect("registrazione di preparazione riuscita")
    }

    /// Id di un utente gia' registrato.
    pub fn id_utente(&self, nome: &str) -> UserId {
        self.archivio
            .find_user_by_username(&utente(nome))
            .expect("lettura dell'utente riuscita")
            .expect("l'utente e' registrato")
            .id
    }

    /// Posizioni che il server ha persistito per un utente.
    ///
    /// L'intervallo copre un anno attorno a [`istante_base`], cioe' molto oltre quello che
    /// uno scenario attraversa: qui non si sta verificando il filtro temporale, che ha i
    /// suoi controlli, ma che cosa e' arrivato a storico.
    ///
    /// La lettura e' sincrona e non passa da `spawn_blocking`: e' un accesso a uno SQLite
    /// in memoria, dell'ordine dei microsecondi, e il test non ha altro da fare intanto.
    pub fn posizioni(&self, utente: UserId) -> Vec<PositionRecord> {
        self.archivio
            .positions_in_range(
                utente,
                istante_base() - TimeDelta::days(365),
                istante_base() + TimeDelta::days(365),
            )
            .expect("lettura delle posizioni riuscita")
    }

    /// Sposta in avanti il tempo di dominio di client e server insieme.
    ///
    /// E' cosi' che i tre minuti del requisito R5, o i minuti fra una rilevazione e la
    /// successiva dentro un'analisi, costano microsecondi invece di minuti veri.
    pub fn avanza(&self, delta: TimeDelta) {
        self.orologio.advance(delta);
    }

    /// Chiede lo spegnimento ordinato e attende che il ciclo di accettazione finisca.
    ///
    /// E' anche il punto in cui si rivendicano i panic dei task del server: se uno di
    /// essi e' morto per un panic, lo scenario fallisce qui invece di chiudersi come se
    /// niente fosse. Vedi la nota sulla spia sui panic in cima a questa sezione.
    pub async fn spegni(mut self) {
        // L'errore significa solo che nessuno e' piu' in ascolto, cioe' che il server sta
        // gia' terminando.
        let _ = self.spegnimento.send(());

        let ciclo = self
            .ciclo
            .take()
            .expect("il ciclo di accettazione si attende una volta sola");
        timeout(ATTESA, ciclo)
            .await
            .expect("il server termina entro la scadenza")
            .expect("il task del server non va in panico")
            .expect("il server termina senza errori di I/O");

        // Il ciclo di accettazione finisce prima delle connessioni che ha spawnato: si
        // lascia loro il tempo di arrivare in fondo alla propria chiusura, altrimenti un
        // panic che avviene proprio li' non sarebbe ancora stato registrato. La finestra
        // copre la scadenza di chiusura di `timeouts_di_prova`.
        sleep(ATTESA_CHIUSURA_CONNESSIONI).await;

        pretendi_nessun_panic_nei_task();
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        // Rete di sicurezza per lo scenario che non chiama `spegni`: senza questo, i suoi
        // panic tornerebbero silenziosi. Non si controlla nulla mentre lo scenario sta
        // gia' srotolando per un panic suo, perche' un secondo panic durante l'unwind
        // aborte il processo e nasconderebbe il fallimento vero.
        if std::thread::panicking() {
            return;
        }
        pretendi_nessun_panic_nei_task();
    }
}

/// Indirizzo di loopback su una porta che il sistema dichiara libera.
///
/// [`net::run_con`] non restituisce l'indirizzo a cui si e' legato, quindi con la porta 0
/// non sarebbe piu' ricostruibile dall'esterno: la si fa assegnare qui da un ascolto
/// temporaneo, chiuso subito dopo. Fra la chiusura e il legame del server resta una
/// finestra in cui un altro processo potrebbe prendersi quella porta; sul loopback di una
/// macchina che sta eseguendo dei test e' un'eventualita' remota, e in cambio nessuno
/// scenario deve fissare un numero di porta, che sarebbe invece un conflitto certo fra due
/// scenari eseguiti insieme.
fn porta_libera() -> SocketAddr {
    let ascolto =
        std::net::TcpListener::bind(("127.0.0.1", 0)).expect("una porta effimera e' disponibile");

    ascolto
        .local_addr()
        .expect("l'ascolto temporaneo ha un indirizzo")
}

/// Attende che l'indirizzo accetti connessioni.
///
/// La connessione di prova viene chiusa subito: il server la vede come un client che si e'
/// presentato e se n'e' andato prima dell'handshake, e la lascia cadere.
async fn attendi_ascolto(indirizzo: SocketAddr) {
    let scadenza = Instant::now() + ATTESA;

    while Instant::now() < scadenza {
        if let Ok(prova) = TcpStream::connect(indirizzo).await {
            drop(prova);
            return;
        }
        sleep(PASSO_DI_ATTESA).await;
    }

    panic!("il server non ha aperto l'ascolto su {indirizzo} entro {ATTESA:?}");
}

/// Attende che il registro delle connessioni contenga esattamente gli utenti indicati.
///
/// Serve dove la chiusura di una sessione non produce nessun evento osservabile dal client
/// — un logout, una sostituzione — e la sola prova che sia avvenuta sta dentro il server.
pub async fn attendi_connessi(server: &TestServer, attesi: &[UserId]) {
    let scadenza = Instant::now() + ATTESA;
    let mut ultimo = Vec::new();

    while Instant::now() < scadenza {
        ultimo = server.registro.connected_users().await;
        if ultimo == attesi {
            return;
        }
        sleep(PASSO_DI_ATTESA).await;
    }

    panic!("connessi {ultimo:?} invece di {attesi:?} dopo {ATTESA:?}");
}

/// Attende che la macchina a stati porti l'utente nello stato indicato.
pub async fn attendi_stato(server: &TestServer, utente: UserId, atteso: UserState) {
    let scadenza = Instant::now() + ATTESA;
    let mut ultimo = None;

    while Instant::now() < scadenza {
        ultimo = server.stati.current_state(utente);
        if ultimo == Some(atteso) {
            return;
        }
        sleep(PASSO_DI_ATTESA).await;
    }

    panic!("stato {ultimo:?} invece di {atteso:?} dopo {ATTESA:?}");
}

/// Attende che un messaggio con quel verso e quel corpo sia arrivato alla persistenza.
///
/// Cio' che un veicolo manda al server non torna indietro in nessuna risposta di
/// contenuto — il livello di rete replica con un `Ack` — quindi l'unico posto in cui si
/// vede se sia stato preso in carico e' la scrittura, che la spia registra.
pub async fn attendi_messaggio(server: &TestServer, verso: MessageDirection, corpo: &str) {
    let scadenza = Instant::now() + ATTESA;
    let mut ultimi = Vec::new();

    while Instant::now() < scadenza {
        ultimi = server.archivio.corpi_con_verso(verso);
        if ultimi.iter().any(|scritto| scritto == corpo) {
            return;
        }
        sleep(PASSO_DI_ATTESA).await;
    }

    panic!("fra i messaggi {verso:?} {ultimi:?} non c'e' {corpo:?} dopo {ATTESA:?}");
}

/// Il client vero, pilotato dai suoi due canali.
///
/// Ogni attesa ha una scadenza e fallisce con un messaggio che dice che cosa si aspettava:
/// un test che si blocca per sempre non direbbe nulla a chi lo legge.
pub struct TestClient {
    /// Nucleo di rete del client, con i canali di comandi ed eventi.
    handle: ClientHandle,
    /// Nome dell'utente, riportato nei messaggi di fallimento.
    nome: String,
}

impl TestClient {
    /// Avvia il client senza attendere l'esito dell'accesso.
    ///
    /// E' la forma piu' generale, quella che serve dove l'accesso deve fallire: gli
    /// scenari normali usano [`TestClient::accedi`].
    pub fn avvia(
        server: &TestServer,
        nome: &str,
        password: &str,
        registra: bool,
        tempi: Tempi,
    ) -> Self {
        let strategia = SourceKind::Manual {
            iniziale: partenza(),
        };
        let sorgente = strategia
            .build()
            .expect("la sorgente manuale non puo' fallire");
        // L'orologio e' lo stesso del server: il `recorded_at` che il client scrive nelle
        // posizioni e l'ora con cui il server le giudica sono cosi' la stessa cosa.
        let orologio: Arc<dyn Clock> = Arc::clone(&server.orologio) as Arc<dyn Clock>;

        Self {
            handle: ClientHandle::spawn_con(
                ClientConfig {
                    server_addr: server.indirizzo,
                    username: utente(nome),
                    password: password.to_owned(),
                    register: registra,
                    source: strategia,
                },
                sorgente,
                orologio,
                tempi,
            ),
            nome: nome.to_owned(),
        }
    }

    /// Accede con un utente gia' registrato e attende la sessione aperta.
    pub async fn accedi(server: &TestServer, nome: &str) -> Self {
        let mut cliente = Self::avvia(server, nome, PASSWORD, false, tempi_fermi());
        cliente.attendi_connessione().await;
        cliente
    }

    /// Come [`TestClient::accedi`], ma con il ticker delle posizioni che scatta.
    pub async fn accedi_e_invia(server: &TestServer, nome: &str) -> Self {
        let mut cliente = Self::avvia(server, nome, PASSWORD, false, tempi_con_invii());
        cliente.attendi_connessione().await;
        cliente
    }

    /// Si registra dal filo e attende la sessione aperta.
    ///
    /// Va usata solo dove nessuna caduta di connessione e' prevista: vedi la nota in cima
    /// al modulo.
    pub async fn registrandosi(server: &TestServer, nome: &str) -> Self {
        let mut cliente = Self::avvia(server, nome, PASSWORD, true, tempi_fermi());
        cliente.attendi_connessione().await;
        cliente
    }

    /// Restituisce il primo evento che soddisfa il predicato, scartando quelli prima.
    ///
    /// Gli eventi scartati non sono un problema: il ticker delle posizioni e i messaggi del
    /// server arrivano quando capita, e uno scenario che aspetta un cambio di stato non
    /// deve inciampare in una conferma di invio.
    pub async fn attendi(
        &mut self,
        atteso: &str,
        predicato: impl Fn(&ClientEvent) -> bool,
    ) -> ClientEvent {
        let scadenza = Instant::now() + ATTESA;

        loop {
            let residuo = scadenza.saturating_duration_since(Instant::now());
            match timeout(residuo, self.handle.event_rx.recv()).await {
                Ok(Some(evento)) if predicato(&evento) => return evento,
                Ok(Some(evento)) => tracing::debug!(?evento, atteso, "evento scartato"),
                Ok(None) => panic!(
                    "il task di rete di {} e' terminato mentre si attendeva {atteso}",
                    self.nome
                ),
                Err(_) => panic!("{atteso} non e' arrivato a {} entro {ATTESA:?}", self.nome),
            }
        }
    }

    /// Attende l'apertura della sessione.
    pub async fn attendi_connessione(&mut self) {
        self.attendi("la connessione", |evento| {
            matches!(evento, ClientEvent::Connected)
        })
        .await;
    }

    /// Attende la caduta della connessione e ne restituisce il motivo.
    pub async fn attendi_caduta(&mut self) -> String {
        match self
            .attendi("la caduta della connessione", |evento| {
                matches!(evento, ClientEvent::Disconnected(_))
            })
            .await
        {
            ClientEvent::Disconnected(motivo) => motivo,
            altro => panic!("evento inatteso al posto di una caduta: {altro:?}"),
        }
    }

    /// Attende un testo dal server, diretto o rivolto alla flotta, e ne restituisce il
    /// corpo.
    pub async fn attendi_testo(&mut self, diretto: bool) -> String {
        let atteso = if diretto {
            "un messaggio diretto"
        } else {
            "un avviso alla flotta"
        };

        match self
            .attendi(atteso, |evento| {
                matches!(evento, ClientEvent::ServerText { direct, .. } if *direct == diretto)
            })
            .await
        {
            ClientEvent::ServerText { body, .. } => body,
            altro => panic!("evento inatteso al posto di un testo: {altro:?}"),
        }
    }

    /// Attende un cambio di stato di movimento.
    pub async fn attendi_cambio_di_stato(&mut self) -> UserState {
        match self
            .attendi("un cambio di stato", |evento| {
                matches!(evento, ClientEvent::StateChanged(_))
            })
            .await
        {
            ClientEvent::StateChanged(stato) => stato,
            altro => panic!("evento inatteso al posto di un cambio di stato: {altro:?}"),
        }
    }

    /// Attende il riepilogo chiesto con [`ClientCommand::RequestAnalysis`].
    pub async fn attendi_analisi(&mut self) -> MovementSummary {
        match self
            .attendi("il riepilogo dei movimenti", |evento| {
                matches!(evento, ClientEvent::AnalysisReceived(_))
            })
            .await
        {
            ClientEvent::AnalysisReceived(riepilogo) => riepilogo,
            altro => panic!("evento inatteso al posto di un riepilogo: {altro:?}"),
        }
    }

    /// Attende un errore non fatale e ne restituisce il testo.
    pub async fn attendi_errore(&mut self) -> String {
        match self
            .attendi("un errore", |evento| {
                matches!(evento, ClientEvent::Error(_))
            })
            .await
        {
            ClientEvent::Error(messaggio) => messaggio,
            altro => panic!("evento inatteso al posto di un errore: {altro:?}"),
        }
    }

    /// Attende che il task di rete termini, cioe' che il canale degli eventi si chiuda.
    ///
    /// E' la prova che il client **non** sta riprovando: dopo una sessione sostituita o un
    /// fallimento terminale dell'autenticazione il task esce, e nessun altro evento puo'
    /// arrivare.
    pub async fn attendi_fine(&mut self) {
        let scadenza = Instant::now() + ATTESA;

        loop {
            let residuo = scadenza.saturating_duration_since(Instant::now());
            match timeout(residuo, self.handle.event_rx.recv()).await {
                Ok(Some(evento)) => tracing::debug!(?evento, "evento prima della fine del task"),
                Ok(None) => return,
                Err(_) => panic!(
                    "il task di rete di {} e' ancora vivo dopo {ATTESA:?}",
                    self.nome
                ),
            }
        }
    }

    /// Manda un comando al task di rete.
    pub async fn comanda(&self, comando: ClientCommand) {
        self.handle
            .command_tx
            .send(comando)
            .await
            .expect("il task di rete accetta comandi");
    }

    /// Sposta la sorgente manuale su una coordinata e attende che sia partita.
    ///
    /// A inviare e' solo il ticker delle posizioni: il comando cambia la coordinata che la
    /// sorgente restituisce, e la conferma arriva al primo scatto successivo. Serve quindi
    /// un client costruito con [`tempi_con_invii`].
    pub async fn invia_posizione(&mut self, dove: Coordinate) {
        self.comanda(ClientCommand::SetManualPosition(dove)).await;
        self.attendi(
            "l'invio della posizione",
            |evento| matches!(evento, ClientEvent::PositionSent(coordinate) if *coordinate == dove),
        )
        .await;
    }

    /// Chiude la sessione con un `Logout` e attende la fine del task di rete.
    pub async fn chiudi(self) {
        let nome = self.nome.clone();

        assert!(
            self.handle.chiudi().await,
            "il client {nome} non si e' chiuso entro la scadenza"
        );
    }
}

/// Un client che parla il protocollo a mano, senza la sequenza che il client vero rispetta.
///
/// Serve agli scenari di sicurezza: una versione di protocollo che il server non accetta,
/// una richiesta prima dell'accesso, una posizione datata nel futuro sono tutte cose che
/// [`ClientHandle`] non fa e non deve fare. Usa gli stessi codec dei due programmi, quindi
/// sul filo passano gli stessi byte.
pub struct ClientGrezzo {
    /// Meta' in lettura, con il codec dei messaggi del server.
    ingresso: FramedRead<OwnedReadHalf, ServerCodec>,
    /// Meta' in scrittura, con il codec dei messaggi del client.
    uscita: FramedWrite<OwnedWriteHalf, ClientCodec>,
}

impl ClientGrezzo {
    /// Apre la connessione senza mandare nulla.
    pub async fn connetti(server: &TestServer) -> Self {
        let flusso = TcpStream::connect(server.indirizzo)
            .await
            .expect("il server accetta la connessione");
        let (lettura, scrittura) = flusso.into_split();

        Self {
            ingresso: FramedRead::new(lettura, ServerCodec::new()),
            uscita: FramedWrite::new(scrittura, ClientCodec::new()),
        }
    }

    /// Manda un messaggio al server.
    pub async fn invia(&mut self, messaggio: ClientMessage) {
        self.uscita
            .send(messaggio)
            .await
            .expect("la scrittura sul socket riesce");
    }

    /// Attende il prossimo messaggio del server.
    pub async fn ricevi(&mut self) -> ServerMessage {
        match self.ricevi_o_chiusura().await {
            Some(messaggio) => messaggio,
            None => panic!("il server ha chiuso il flusso invece di rispondere"),
        }
    }

    /// Attende il prossimo messaggio, oppure la fine del flusso.
    pub async fn ricevi_o_chiusura(&mut self) -> Option<ServerMessage> {
        match timeout(ATTESA, self.ingresso.next()).await {
            Ok(Some(Ok(messaggio))) => Some(messaggio),
            Ok(Some(Err(errore))) => panic!("frame non valido dal server: {errore}"),
            Ok(None) => None,
            Err(_) => panic!("nessuna risposta dal server entro {ATTESA:?}"),
        }
    }

    /// Attende che il server chiuda il flusso, scartando i messaggi che lo precedono.
    pub async fn attendi_chiusura(&mut self) {
        while let Some(messaggio) = self.ricevi_o_chiusura().await {
            tracing::debug!(?messaggio, "messaggio prima della chiusura");
        }
    }

    /// Esegue l'handshake con la versione di protocollo corrente.
    pub async fn saluta(&mut self) {
        self.invia(ClientMessage::Hello {
            protocol_version: PROTOCOL_VERSION,
        })
        .await;

        match self.ricevi().await {
            ServerMessage::Welcome { protocol_version } => {
                assert_eq!(protocol_version, PROTOCOL_VERSION);
            }
            altro => panic!("risposta inattesa all'handshake: {altro:?}"),
        }
    }

    /// Esegue l'handshake e apre la sessione di un utente gia' registrato.
    pub async fn saluta_e_accedi(&mut self, nome: &str) {
        self.saluta().await;
        self.invia(ClientMessage::Login {
            username: utente(nome),
            password: PASSWORD.to_owned(),
        })
        .await;

        match self.ricevi().await {
            ServerMessage::SessionToken { .. } => {}
            altro => panic!("risposta inattesa all'accesso: {altro:?}"),
        }
    }

    /// Scarta il ritorno a fermo che il battito della macchina a stati puo' avere gia'
    /// accodato, senza pretendere che ci sia.
    ///
    /// Serve dove uno scenario porta il veicolo oltre la soglia di [`STILL_AFTER_SECS`]
    /// lasciandolo in movimento e poi asserisce su un messaggio preciso. Da quel punto in
    /// avanti il task periodico del server e' nel suo diritto di segnalare
    /// [`UserState::Still`], e lo accoda sullo stesso canale delle risposte alle
    /// richieste: quale dei due messaggi arrivi per primo dipende da dove cade uno scatto
    /// di [`BATTITO`], che e' tempo vero e che nessun orologio finto governa. Consumandolo
    /// qui l'assert successivo torna a leggere una sequenza sola.
    ///
    /// Il cambio si scarta una volta sola perche' e' unico: il tracciatore segnala solo
    /// chi passa da [`UserState::Moving`] a fermo, quindi appena consumato non ne produce
    /// altri finche' il veicolo non torna a spostarsi.
    ///
    /// Va chiamata dove la coda non puo' contenere altro, cioe' prima di mandare la
    /// richiesta successiva: cosi' l'unico messaggio che puo' presentarsi e' il cambio di
    /// stato, e una lettura che scarta non rischia di mangiare una risposta attesa.
    ///
    /// [`STILL_AFTER_SECS`]: georuggine_common::types::STILL_AFTER_SECS
    pub async fn scarta_ritorno_a_fermo(&mut self) {
        match timeout(ATTESA_DEL_BATTITO, self.ingresso.next()).await {
            // Lo scatto e' gia' avvenuto: il cambio esce dalla coda e cio' che si attende
            // torna a essere il primo messaggio.
            Ok(Some(Ok(ServerMessage::StateChanged {
                state: UserState::Still,
            }))) => {}
            Ok(Some(Ok(altro))) => {
                panic!("in coda c'e' altro oltre al ritorno a fermo: {altro:?}")
            }
            Ok(Some(Err(errore))) => panic!("frame non valido dal server: {errore}"),
            Ok(None) => panic!("il server ha chiuso il flusso invece di restare in attesa"),
            // Nessun messaggio entro la finestra: o lo scatto era gia' stato consumato da
            // una lettura precedente, o non aveva nulla da segnalare. In nessuno dei due
            // casi e' un fallimento, ed e' per questo che la scadenza non e' generosa.
            Err(_) => {}
        }
    }
}
