//! Supporto condiviso dagli scenari end-to-end.
//!
//! [`TestServer`] avvia il server vero su una porta effimera; [`TestClient`] avvia il client
//! vero e lo pilota dai suoi canali; [`ClientGrezzo`] invia messaggi a mano per le prove
//! fuori sequenza.
//!
//! Il tempo di dominio viene da un [`FakeClock`] condiviso fra client e server e spostato
//! con [`TestServer::avanza`]; i timer di tokio usano tempo reale, quindi
//! [`ConnectionTimeouts`] e [`Tempi`] vengono accorciati.
//!
//! [`TestClient::accedi`] usa sempre Login: l'utente si crea prima con
//! [`TestServer::registra`].

// Ogni scenario usa solo una parte di questo modulo.
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
use georuggine_server::auth::{self, AuthConfig};
use georuggine_server::messaging::MessageHub;
use georuggine_server::net::{
    self, ConnectionRegistry, ConnectionTimeouts, Dipendenze, Regolazioni, ServerConfig,
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

/// Attesa massima di un evento atteso: ampia, perche' si paga solo quando il test fallisce.
pub const ATTESA: Duration = Duration::from_secs(10);

/// Margine concesso alle connessioni per chiudersi dopo lo spegnimento, prima di
/// controllare i panic.
const ATTESA_CHIUSURA_CONNESSIONI: Duration = Duration::from_millis(50);

/// Intervallo fra due controlli di uno stato interno del server.
const PASSO_DI_ATTESA: Duration = Duration::from_millis(10);

/// Password che rispetta la policy di `georuggine_server::auth::valida_password`.
pub const PASSWORD: &str = "veicolo12";

/// Password che la policy rifiuta: troppo corta e senza cifre.
pub const PASSWORD_DEBOLE: &str = "corta";

/// Intervallo del task periodico della macchina a stati nei test (tempo reale).
const BATTITO: Duration = Duration::from_millis(20);

/// Attesa breve di un cambio di stato che potrebbe non arrivare, pari a tre periodi di
/// [`BATTITO`].
const ATTESA_DEL_BATTITO: Duration = BATTITO.saturating_mul(3);

/// Istante iniziale dell'orologio finto: mezzogiorno, per non attraversare un cambio di
/// data nel fuso locale.
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

/// Punto circa cinquanta metri a nord di [`partenza`], ben oltre la soglia.
pub fn a_cinquanta_metri() -> Coordinate {
    coordinata(45.0703 + 0.000_45, 7.6869)
}

/// Punto circa cento metri a nord di [`partenza`].
pub fn a_cento_metri() -> Coordinate {
    coordinata(45.0703 + 0.000_9, 7.6869)
}

/// Tempi con cui il client non invia mai posizioni da solo e si riconnette subito.
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

/// Tempi con cui il client invia posizioni di continuo, per gli scenari sull'invio
/// periodico e sulla coda.
pub fn tempi_con_invii() -> Tempi {
    Tempi {
        intervallo_posizione: Duration::from_millis(50),
        ..tempi_fermi()
    }
}

/// Scadenze del server nei test; l'inattivita' resta lunga per non chiudere le connessioni
/// per il motivo sbagliato.
pub fn timeouts_di_prova() -> ConnectionTimeouts {
    ConnectionTimeouts {
        handshake: Duration::from_secs(2),
        idle: Duration::from_secs(30),
        shutdown: Duration::from_millis(200),
        tick_interval: BATTITO,
    }
}

/// Parametri di Argon2 minimi, per test veloci.
pub fn autenticazione_rapida() -> AuthConfig {
    AuthConfig {
        memory_kib: 32,
        iterations: 1,
        parallelism: 1,
    }
}

/// Messaggio registrato dalla spia.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessaggioSpiato {
    /// Destinatario, assente per gli avvisi alla flotta.
    pub destinatario: Option<UserId>,
    /// Verso rispetto al server.
    pub verso: MessageDirection,
    /// Corpo del messaggio.
    pub corpo: String,
}

/// Repository vero con un registratore davanti, per verificare anche i messaggi che il
/// trait non permette di rileggere.
pub struct RepositorySpia {
    /// Repository vero, SQLite in memoria.
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

    /// Prende il lock anche se avvelenato.
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
        // Si registra prima di scrivere, anche se la scrittura fallisce.
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

// --- panic dei task del server ---
//
// I task del server non hanno una `JoinHandle` attesa dal test, quindi un loro panic
// passerebbe inosservato. Il gancio registra i panic per thread, cioe' per scenario, e
// [`TestServer::spegni`] li trasforma in un fallimento.

/// Un panic osservato dal gancio globale.
struct PanicSpiato {
    /// Thread su cui e' avvenuto, cioe' lo scenario.
    thread: ThreadId,
    /// Messaggio e posizione.
    descrizione: String,
}

/// Panic osservati e non ancora riportati.
static PANIC_SPIATI: OnceLock<Mutex<Vec<PanicSpiato>>> = OnceLock::new();

/// Indica se il gancio e' gia' installato.
static SPIA_ARMATA: OnceLock<()> = OnceLock::new();

/// Lista dei panic osservati, anche con lock avvelenato.
fn panic_spiati() -> MutexGuard<'static, Vec<PanicSpiato>> {
    PANIC_SPIATI
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
}

/// Installa il gancio sui panic una volta per binario, mantenendo quello precedente.
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

/// Toglie dalla lista e restituisce i panic avvenuti in questo scenario.
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

/// Server vero su una porta effimera, con accesso alle sue dipendenze interne.
pub struct TestServer {
    /// Indirizzo a cui i client si collegano.
    pub indirizzo: SocketAddr,
    /// Orologio di dominio, condiviso con i client dello scenario.
    pub orologio: Arc<FakeClock>,
    /// Persistenza, con la spia sui messaggi.
    pub archivio: Arc<RepositorySpia>,
    /// Connessioni aperte, una per utente.
    pub registro: Arc<ConnectionRegistry>,
    /// Messaggistica, usata dagli scenari per far parlare il server.
    pub messaggistica: Arc<MessageHub>,
    /// Macchina a stati fermo/movimento.
    pub stati: Arc<StateTracker>,
    /// Segnale di spegnimento, condiviso con le connessioni.
    spegnimento: broadcast::Sender<()>,
    /// Ciclo di accettazione; un `Option` per poterlo prendere in `spegni` nonostante il
    /// `Drop`.
    ciclo: Option<JoinHandle<std::io::Result<()>>>,
}

impl TestServer {
    /// Avvia il server con le scadenze di [`timeouts_di_prova`].
    pub async fn avvia() -> Self {
        Self::con_timeouts(timeouts_di_prova()).await
    }

    /// Avvia il server con le scadenze indicate e ritorna quando accetta connessioni.
    pub async fn con_timeouts(timeouts: ConnectionTimeouts) -> Self {
        // Il gancio va installato prima di avviare i task del server.
        arma_la_spia_sui_panic();

        let orologio = Arc::new(FakeClock::new(istante_base()));
        let sorgente_ora: Arc<dyn Clock> = Arc::clone(&orologio) as Arc<dyn Clock>;

        let archivio = Arc::new(RepositorySpia::nuova(Arc::clone(&sorgente_ora)));
        let repo: Arc<dyn Repository> = Arc::clone(&archivio) as Arc<dyn Repository>;
        let registro = Arc::new(ConnectionRegistry::new());
        // Stesso registro del livello di rete, come nel binario.
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
            // Database gia' aperto qui; il log CPU lo gestisce il binario.
            db_path: PathBuf::from(":memory:"),
            log_dir: PathBuf::from("logs"),
        };
        let ciclo = tokio::spawn(net::run(
            configurazione,
            Dipendenze {
                repo,
                registry: Arc::clone(&registro),
                messaging: Arc::clone(&messaggistica),
                clock: Arc::clone(&sorgente_ora),
                stati: Arc::clone(&stati),
            },
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
            registro,
            messaggistica,
            stati,
            spegnimento,
            ciclo: Some(ciclo),
        }
    }

    /// Registra un utente senza passare dalla rete e ne restituisce l'id.
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

    /// Posizioni salvate per un utente, su un intervallo di un anno attorno a
    /// [`istante_base`].
    pub fn posizioni(&self, utente: UserId) -> Vec<PositionRecord> {
        self.archivio
            .positions_in_range(
                utente,
                istante_base() - TimeDelta::days(365),
                istante_base() + TimeDelta::days(365),
            )
            .expect("lettura delle posizioni riuscita")
    }

    /// Sposta in avanti il tempo di dominio di client e server.
    pub fn avanza(&self, delta: TimeDelta) {
        self.orologio.advance(delta);
    }

    /// Spegne il server, attende il ciclo di accettazione e fa fallire lo scenario se un
    /// task del server e' andato in panico.
    pub async fn spegni(mut self) {
        // Un errore indica solo che il server sta gia' terminando.
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

        // Si lascia alle connessioni il tempo di chiudersi prima di controllare i panic.
        sleep(ATTESA_CHIUSURA_CONNESSIONI).await;

        pretendi_nessun_panic_nei_task();
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        // Controllo anche per gli scenari che non chiamano `spegni`, ma non durante un
        // unwind: un secondo panic abortirebbe il processo.
        if std::thread::panicking() {
            return;
        }
        pretendi_nessun_panic_nei_task();
    }
}

/// Indirizzo di loopback su una porta libera, ottenuta da un ascolto temporaneo.
fn porta_libera() -> SocketAddr {
    let ascolto =
        std::net::TcpListener::bind(("127.0.0.1", 0)).expect("una porta effimera e' disponibile");

    ascolto
        .local_addr()
        .expect("l'ascolto temporaneo ha un indirizzo")
}

/// Attende che l'indirizzo accetti connessioni.
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

/// Attende che un messaggio con quel verso e quel corpo sia stato salvato.
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

/// Client vero, pilotato dai suoi canali; ogni attesa ha una scadenza.
pub struct TestClient {
    /// Nucleo di rete del client, con i canali di comandi ed eventi.
    handle: ClientHandle,
    /// Nome dell'utente, riportato nei messaggi di fallimento.
    nome: String,
}

impl TestClient {
    /// Avvia il client senza attendere l'esito dell'accesso.
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
        // Stesso orologio del server.
        let orologio: Arc<dyn Clock> = Arc::clone(&server.orologio) as Arc<dyn Clock>;

        Self {
            handle: ClientHandle::spawn(
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

    /// Si registra e attende la sessione aperta; da usare solo senza cadute di
    /// connessione.
    pub async fn registrandosi(server: &TestServer, nome: &str) -> Self {
        let mut cliente = Self::avvia(server, nome, PASSWORD, true, tempi_fermi());
        cliente.attendi_connessione().await;
        cliente
    }

    /// Restituisce il primo evento che soddisfa il predicato, scartando gli altri.
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

    /// Attende un testo dal server, diretto o broadcast, e ne restituisce il corpo.
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

    /// Attende la fine del task di rete, cioe' la chiusura del canale degli eventi.
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

    /// Sposta la sorgente manuale e attende l'invio della nuova posizione; richiede
    /// [`tempi_con_invii`].
    pub async fn invia_posizione(&mut self, dove: Coordinate) {
        self.comanda(ClientCommand::SetManualPosition(dove)).await;
        self.attendi(
            "l'invio della posizione",
            |evento| matches!(evento, ClientEvent::PositionSent(coordinate, _) if *coordinate == dove),
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

/// Client che scrive i messaggi a mano con gli stessi codec, per le prove fuori sequenza.
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
        // Subito dopo il token il server comunica lo stato iniziale.
        match self.ricevi().await {
            ServerMessage::StateChanged {
                state: UserState::Still,
            } => {}
            altro => panic!("atteso lo stato iniziale, ottenuto {altro:?}"),
        }
    }

    /// Scarta l'eventuale ritorno a fermo gia' accodato dal task periodico, se c'e'.
    pub async fn scarta_ritorno_a_fermo(&mut self) {
        match timeout(ATTESA_DEL_BATTITO, self.ingresso.next()).await {
            Ok(Some(Ok(ServerMessage::StateChanged {
                state: UserState::Still,
            }))) => {}
            Ok(Some(Ok(altro))) => {
                panic!("in coda c'e' altro oltre al ritorno a fermo: {altro:?}")
            }
            Ok(Some(Err(errore))) => panic!("frame non valido dal server: {errore}"),
            Ok(None) => panic!("il server ha chiuso il flusso invece di restare in attesa"),
            // Nessun cambio entro la finestra: non e' un errore.
            Err(_) => {}
        }
    }
}
