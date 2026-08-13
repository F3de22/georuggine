//! Persistenza del server: utenti, storico delle posizioni e messaggi in attesa.
//!
//! L'accesso ai dati passa dal trait [`Repository`], che ha due implementazioni con
//! comportamento identico: [`SqliteRepository`], usata in produzione, e
//! [`InMemoryRepository`], che evita di toccare il disco nei test della logica di
//! dominio. L'uguaglianza fra le due non e' affidata alla buona volonta': i test di
//! questo modulo fanno passare entrambe dalla stessa funzione di verifica del contratto.
//!
//! I metodi sono **sincroni**: SQLite e' una libreria bloccante, e nasconderla dietro
//! `async fn` farebbe fermare un thread dell'esecutore. Il chiamante li avvolge in
//! `tokio::task::spawn_blocking`, quindi qui non si importa tokio.
//!
//! Gli istanti sono salvati come testo RFC3339 in UTC con i nanosecondi sempre scritti
//! (`2024-01-01T00:00:00.000000000Z`). SQLite non ha un tipo per le date e confronta il
//! testo carattere per carattere: se la precisione fosse variabile, come quella che
//! chrono sceglie da solo, due istanti dello stesso secondo si distinguerebbero per la
//! presenza o l'assenza della parte decimale, e l'ordine dipenderebbe da dove cade nella
//! tabella ASCII il carattere che segue i secondi rispetto alle cifre: con il suffisso
//! `Z` (0x5A) `...:00Z` risulta maggiore di `...:00.5Z`, cioe' l'inverso dell'ordine
//! cronologico. A lunghezza fissa il confronto testuale coincide con quello sul tempo, e
//! `ORDER BY recorded_at` e' corretto senza dipendere da quell'accidente di codifica.

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use chrono::{DateTime, SecondsFormat, Utc};
use georuggine_common::clock::Clock;
use georuggine_common::types::{Coordinate, PositionRecord, UserId, Username};
use rusqlite::{Connection, OptionalExtension, params};
use thiserror::Error;

/// Schema del database, applicato a ogni apertura.
///
/// Ogni `CREATE` e' `IF NOT EXISTS`, quindi rieseguirlo su un file gia' popolato non
/// cambia nulla: l'apertura e' idempotente e non serve distinguere il primo avvio.
/// `journal_mode=WAL` permette letture concorrenti mentre una scrittura e' in corso;
/// `foreign_keys=ON` va ripetuto a ogni connessione perche' SQLite lo disattiva per
/// impostazione predefinita, ed e' cio' che rende osservabile il riferimento a un utente
/// inesistente.
const SCHEMA: &str = "\
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS users (
    id            INTEGER PRIMARY KEY,
    username      TEXT    UNIQUE NOT NULL,
    password_hash TEXT    NOT NULL,
    created_at    TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS positions (
    id          INTEGER PRIMARY KEY,
    user_id     INTEGER NOT NULL REFERENCES users(id),
    lat         REAL    NOT NULL,
    lon         REAL    NOT NULL,
    recorded_at TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS messages (
    id        INTEGER PRIMARY KEY,
    user_id   INTEGER,
    direction TEXT    NOT NULL,
    body      TEXT    NOT NULL,
    sent_at   TEXT    NOT NULL,
    delivered INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_positions_user_time ON positions(user_id, recorded_at);
";

/// Errore restituito dalle operazioni di persistenza.
#[derive(Debug, Error)]
pub enum StorageError {
    /// Errore proveniente da SQLite che non corrisponde a nessun caso di dominio.
    #[error("errore del database: {0}")]
    Sqlite(#[from] rusqlite::Error),
    /// Lo username e' gia' assegnato a un altro utente.
    #[error("username gia' registrato")]
    UsernameTaken,
    /// L'operazione fa riferimento a un utente che non esiste.
    #[error("utente inesistente")]
    UserNotFound,
    /// Una riga letta dal database non si converte nel tipo di dominio corrispondente.
    ///
    /// Segnala dati corrotti o scritti da una versione incompatibile: uno username che
    /// non supera piu' la validazione, una coordinata fuori intervallo, un istante non
    /// in RFC3339, un id che non entra in `u32`.
    #[error("dato illeggibile dal database: {0}")]
    Serialization(String),
}

/// Utente come conservato dal repository.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredUser {
    /// Identificatore assegnato alla creazione.
    pub id: UserId,
    /// Nome scelto dall'utente, unico fra tutti gli utenti.
    pub username: Username,
    /// Hash della password: in chiaro non viene mai salvata.
    pub password_hash: String,
    /// Istante di registrazione, letto dall'orologio del repository.
    pub created_at: DateTime<Utc>,
}

/// Verso di un messaggio rispetto al server.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MessageDirection {
    /// Dal server a tutti i veicoli connessi.
    Broadcast,
    /// Dal server a un singolo veicolo.
    Direct,
    /// Da un veicolo verso il server.
    Inbound,
}

impl MessageDirection {
    /// Etichetta salvata nella colonna `direction`.
    ///
    /// Si salva il nome e non un intero perche' il contenuto della tabella resti
    /// leggibile con un qualunque client SQL, e perche' aggiungere un verso in futuro
    /// non rimescoli il significato dei valori gia' scritti.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Broadcast => "Broadcast",
            Self::Direct => "Direct",
            Self::Inbound => "Inbound",
        }
    }
}

/// Identificatore di una riga di `messages`.
///
/// E' un `i64` e non un `u32` come [`UserId`]: quello viaggia nel protocollo e paga una
/// conversione verificata a ogni lettura, mentre questo resta dentro il server e
/// coincide con il `rowid` di SQLite, che e' gia' un `i64`. Tenere lo stesso tipo evita
/// una conversione che potrebbe fallire su un archivio molto grande, cioe' proprio quando
/// perdere la marcatura di un messaggio sarebbe piu' facile da non notare.
///
/// E' un newtype e non un alias perche' il compilatore lo distingua da un id di utente:
/// scambiarli marcherebbe consegnato il messaggio sbagliato, in silenzio.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct MessageId(pub i64);

/// Messaggio destinato a un utente e non ancora consegnato.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingMessage {
    /// Testo del messaggio.
    pub body: String,
    /// Istante in cui il messaggio e' stato prodotto.
    pub sent_at: DateTime<Utc>,
}

/// Accesso in lettura e scrittura ai dati persistiti del server.
///
/// I bound `Send + Sync + 'static` stanno sul trait perche' il repository viene
/// condiviso fra i task come `Arc<dyn Repository>` e le sue chiamate finiscono in
/// `spawn_blocking`, che richiede argomenti `'static`: chiederli qui una volta evita di
/// ripeterli a ogni struttura che tiene un repository.
///
/// Contratto comune alle implementazioni, verificato dai test:
///
/// - le letture per un utente inesistente restituiscono un risultato vuoto, non un
///   errore; solo le scritture che violano un riferimento danno
///   [`StorageError::UserNotFound`];
/// - gli id degli utenti partono da 1 e crescono di uno a ogni creazione;
/// - i risultati hanno un ordine definito, indicato su ciascun metodo.
pub trait Repository: Send + Sync + 'static {
    /// Registra un nuovo utente e ne restituisce l'id assegnato.
    ///
    /// L'istante di creazione viene letto dall'orologio del repository, non da
    /// `Utc::now()`, cosi' i test possono renderlo deterministico.
    ///
    /// # Errori
    ///
    /// [`StorageError::UsernameTaken`] se lo username e' gia' registrato.
    fn create_user(&self, username: &Username, password_hash: &str)
    -> Result<UserId, StorageError>;

    /// Cerca un utente per username.
    ///
    /// L'assenza e' un esito normale e vale `Ok(None)`, non un errore.
    ///
    /// # Errori
    ///
    /// [`StorageError::Serialization`] se la riga trovata non si converte nei tipi di
    /// dominio.
    fn find_user_by_username(
        &self,
        username: &Username,
    ) -> Result<Option<StoredUser>, StorageError>;

    /// Aggiunge una posizione allo storico.
    ///
    /// # Errori
    ///
    /// [`StorageError::UserNotFound`] se `record.user_id` non corrisponde a nessun
    /// utente.
    fn insert_position(&self, record: &PositionRecord) -> Result<(), StorageError>;

    /// Posizioni di un utente nell'intervallo indicato, estremi inclusi.
    ///
    /// Il risultato e' ordinato per `recorded_at` crescente e, a parita' di istante,
    /// per ordine di inserimento. Un intervallo senza dati, o un utente inesistente,
    /// danno un vettore vuoto.
    ///
    /// # Errori
    ///
    /// [`StorageError::Serialization`] se una riga non si converte nei tipi di dominio.
    fn positions_in_range(
        &self,
        user_id: UserId,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<Vec<PositionRecord>, StorageError>;

    /// Registra un messaggio, marcandolo come non ancora consegnato, e ne restituisce
    /// l'id.
    ///
    /// `user_id` e' `None` per i messaggi che non hanno un singolo destinatario: restano
    /// a storico e non compaiono in [`Repository::pending_messages`] di nessun utente.
    ///
    /// L'id serve a marcare consegnato **quel** messaggio con
    /// [`Repository::mark_delivered`], senza toccare gli altri che lo stesso utente
    /// potrebbe avere davvero in attesa: e' cio' che permette di scrivere prima e
    /// consegnare poi, che e' l'ordine con cui la messaggistica evita di perdere un
    /// messaggio se il destinatario non e' raggiungibile.
    ///
    /// # Errori
    ///
    /// [`StorageError::Sqlite`] se la scrittura fallisce.
    fn insert_message(
        &self,
        user_id: Option<UserId>,
        direction: MessageDirection,
        body: &str,
        at: DateTime<Utc>,
    ) -> Result<MessageId, StorageError>;

    /// Messaggi che il server deve ancora recapitare all'utente.
    ///
    /// Sono i soli [`MessageDirection::Direct`] non ancora consegnati. Cio' che l'utente
    /// ha mandato al server ([`MessageDirection::Inbound`]) e gli avvisi alla flotta
    /// ([`MessageDirection::Broadcast`]) stanno nella stessa tabella ma non sono in
    /// attesa per nessuno: i primi hanno gia' raggiunto il destinatario, che e' il server,
    /// e i secondi valgono per chi era collegato quando sono partiti.
    ///
    /// Il risultato e' ordinato per `sent_at` crescente e, a parita' di istante, per
    /// ordine di inserimento.
    ///
    /// # Errori
    ///
    /// [`StorageError::Serialization`] se una riga non si converte nei tipi di dominio.
    fn pending_messages(&self, user_id: UserId) -> Result<Vec<PendingMessage>, StorageError>;

    /// Segna come consegnato il singolo messaggio indicato.
    ///
    /// Serve alla consegna immediata: il messaggio e' stato scritto prima di provare
    /// l'invio, e una volta recapitato va tolto dall'attesa senza toccare quelli che lo
    /// precedono, che potrebbero non essere mai stati consegnati.
    ///
    /// E' idempotente, e su un id che non esiste non e' un errore: come per le letture,
    /// l'assenza e' un esito normale.
    ///
    /// # Errori
    ///
    /// [`StorageError::Sqlite`] se la scrittura fallisce.
    fn mark_delivered(&self, id: MessageId) -> Result<(), StorageError>;

    /// Segna come consegnati tutti i messaggi in attesa dell'utente.
    ///
    /// Riguarda esattamente cio' che [`Repository::pending_messages`] restituirebbe: i
    /// messaggi diretti non ancora consegnati, e nessuna delle altre righe dell'utente.
    ///
    /// Le righe non vengono cancellate ma marcate: lo storico resta interrogabile. E'
    /// idempotente, e su un utente senza messaggi non e' un errore.
    ///
    /// # Errori
    ///
    /// [`StorageError::Sqlite`] se la scrittura fallisce.
    fn clear_pending_messages(&self, user_id: UserId) -> Result<(), StorageError>;

    /// Tutti gli utenti registrati, ordinati per id crescente.
    ///
    /// # Errori
    ///
    /// [`StorageError::Serialization`] se una riga non si converte nei tipi di dominio.
    fn list_users(&self) -> Result<Vec<StoredUser>, StorageError>;
}

/// Rappresentazione testuale di un istante nel database.
fn formatta_istante(istante: DateTime<Utc>) -> String {
    istante.to_rfc3339_opts(SecondsFormat::Nanos, true)
}

/// Rilegge un istante scritto da [`formatta_istante`].
fn analizza_istante(testo: &str) -> Result<DateTime<Utc>, StorageError> {
    DateTime::parse_from_rfc3339(testo)
        .map(|istante| istante.with_timezone(&Utc))
        .map_err(|errore| {
            StorageError::Serialization(format!("istante {testo:?} non in RFC3339: {errore}"))
        })
}

/// Converte l'id intero di una riga nel tipo di dominio.
///
/// SQLite conserva le chiavi primarie in 64 bit con segno, mentre [`UserId`] ne usa 32
/// senza segno: la conversione e' verificata invece di essere troncata con `as`.
fn id_da_riga(grezzo: i64) -> Result<UserId, StorageError> {
    u32::try_from(grezzo).map(UserId).map_err(|_| {
        StorageError::Serialization(format!("id utente {grezzo} fuori dall'intervallo di u32"))
    })
}

/// Riconosce la violazione del vincolo `UNIQUE` su `users.username`.
///
/// Senza questa traduzione un nome gia' preso arriverebbe al chiamante come errore
/// generico di SQLite, e distinguerlo da un guasto del database vorrebbe dire leggere
/// il testo del messaggio. `users.username` e' l'unico vincolo `UNIQUE` dello schema.
fn errore_di_unicita(errore: rusqlite::Error) -> StorageError {
    match &errore {
        rusqlite::Error::SqliteFailure(fallimento, _)
            if fallimento.extended_code == rusqlite::ffi::SQLITE_CONSTRAINT_UNIQUE =>
        {
            StorageError::UsernameTaken
        }
        _ => StorageError::Sqlite(errore),
    }
}

/// Riconosce la violazione di una chiave esterna verso `users(id)`.
fn errore_di_riferimento(errore: rusqlite::Error) -> StorageError {
    match &errore {
        rusqlite::Error::SqliteFailure(fallimento, _)
            if fallimento.extended_code == rusqlite::ffi::SQLITE_CONSTRAINT_FOREIGNKEY =>
        {
            StorageError::UserNotFound
        }
        _ => StorageError::Sqlite(errore),
    }
}

/// Colonne di `users` come arrivano da SQLite, prima della validazione di dominio.
type ColonneUtente = (i64, String, String, String);

/// Costruisce uno [`StoredUser`] dalle colonne grezze.
///
/// La conversione sta fuori dalla chiusura passata a rusqlite perche' quella puo'
/// fallire solo con `rusqlite::Error`, mentre qui gli errori sono di dominio: tenerla
/// separata evita di impacchettare un errore di validazione dentro un errore di
/// database per poi doverlo riestrarre.
fn utente_da_colonne(colonne: ColonneUtente) -> Result<StoredUser, StorageError> {
    let (id, username, password_hash, created_at) = colonne;
    Ok(StoredUser {
        id: id_da_riga(id)?,
        username: Username::new(&username).map_err(|errore| {
            StorageError::Serialization(format!("username {username:?} non valido: {errore}"))
        })?,
        password_hash,
        created_at: analizza_istante(&created_at)?,
    })
}

/// Repository su file SQLite, usato in produzione.
///
/// La connessione sta dietro un [`Mutex`] perche' `rusqlite::Connection` e' `Send` ma
/// non `Sync`, mentre [`Repository`] va condiviso fra i task. Il lock non attraversa mai
/// un `.await`: i metodi sono sincroni e girano dentro `spawn_blocking`.
pub struct SqliteRepository {
    /// Connessione unica al database.
    connessione: Mutex<Connection>,
    /// Sorgente dell'ora usata per `created_at`.
    orologio: Arc<dyn Clock>,
}

impl SqliteRepository {
    /// Apre il database al percorso indicato e vi applica lo schema.
    ///
    /// Il percorso puo' essere `":memory:"` per un database temporaneo in RAM, comodo
    /// nei test. Se il file non esiste viene creato; se esiste, lo schema viene
    /// riapplicato senza toccare i dati.
    ///
    /// # Errori
    ///
    /// [`StorageError::Sqlite`] se il file non e' apribile o lo schema non e'
    /// applicabile.
    pub fn new(percorso: impl AsRef<Path>, orologio: Arc<dyn Clock>) -> Result<Self, StorageError> {
        let connessione = Connection::open(percorso)?;
        connessione.execute_batch(SCHEMA)?;

        Ok(Self {
            connessione: Mutex::new(connessione),
            orologio,
        })
    }

    /// Prende il lock sulla connessione, riprendendolo anche se e' avvelenato.
    ///
    /// L'avvelenamento segnala che un thread e' andato in panico tenendo il lock. La
    /// connessione resta comunque utilizzabile, perche' ogni metodo esegue statement
    /// completi e SQLite non lascia transazioni implicite a meta': si continua con
    /// [`PoisonError::into_inner`] invece dell'`unwrap` che il progetto vieta.
    fn blocca(&self) -> MutexGuard<'_, Connection> {
        self.connessione
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }
}

impl Repository for SqliteRepository {
    fn create_user(
        &self,
        username: &Username,
        password_hash: &str,
    ) -> Result<UserId, StorageError> {
        let creato_il = formatta_istante(self.orologio.now());

        let connessione = self.blocca();
        connessione
            .execute(
                "INSERT INTO users (username, password_hash, created_at) VALUES (?1, ?2, ?3)",
                params![username.as_str(), password_hash, creato_il],
            )
            .map_err(errore_di_unicita)?;

        id_da_riga(connessione.last_insert_rowid())
    }

    fn find_user_by_username(
        &self,
        username: &Username,
    ) -> Result<Option<StoredUser>, StorageError> {
        let connessione = self.blocca();
        let colonne = connessione
            .query_row(
                "SELECT id, username, password_hash, created_at FROM users WHERE username = ?1",
                params![username.as_str()],
                |riga| Ok((riga.get(0)?, riga.get(1)?, riga.get(2)?, riga.get(3)?)),
            )
            .optional()?;

        colonne.map(utente_da_colonne).transpose()
    }

    fn insert_position(&self, record: &PositionRecord) -> Result<(), StorageError> {
        let connessione = self.blocca();
        connessione
            .execute(
                "INSERT INTO positions (user_id, lat, lon, recorded_at) VALUES (?1, ?2, ?3, ?4)",
                params![
                    record.user_id.0,
                    record.coordinate.lat(),
                    record.coordinate.lon(),
                    formatta_istante(record.recorded_at),
                ],
            )
            .map_err(errore_di_riferimento)?;

        Ok(())
    }

    fn positions_in_range(
        &self,
        user_id: UserId,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<Vec<PositionRecord>, StorageError> {
        let connessione = self.blocca();
        let mut interrogazione = connessione.prepare(
            "SELECT lat, lon, recorded_at FROM positions \
             WHERE user_id = ?1 AND recorded_at >= ?2 AND recorded_at <= ?3 \
             ORDER BY recorded_at ASC, id ASC",
        )?;

        let righe = interrogazione.query_map(
            params![user_id.0, formatta_istante(from), formatta_istante(to)],
            |riga| {
                Ok((
                    riga.get::<_, f64>(0)?,
                    riga.get::<_, f64>(1)?,
                    riga.get::<_, String>(2)?,
                ))
            },
        )?;

        let mut posizioni = Vec::new();
        for riga in righe {
            let (lat, lon, registrata_il) = riga?;
            posizioni.push(PositionRecord {
                user_id,
                coordinate: Coordinate::new(lat, lon).map_err(|errore| {
                    StorageError::Serialization(format!(
                        "coordinata ({lat}, {lon}) non valida: {errore}"
                    ))
                })?,
                recorded_at: analizza_istante(&registrata_il)?,
            });
        }

        Ok(posizioni)
    }

    fn insert_message(
        &self,
        user_id: Option<UserId>,
        direction: MessageDirection,
        body: &str,
        at: DateTime<Utc>,
    ) -> Result<MessageId, StorageError> {
        let connessione = self.blocca();
        connessione.execute(
            "INSERT INTO messages (user_id, direction, body, sent_at) VALUES (?1, ?2, ?3, ?4)",
            params![
                user_id.map(|id| id.0),
                direction.as_str(),
                body,
                formatta_istante(at),
            ],
        )?;

        // `last_insert_rowid` e' relativo alla connessione, che qui e' una sola e sta
        // sotto lo stesso lock dell'inserimento: fra le due chiamate non puo' passare
        // un'altra scrittura.
        Ok(MessageId(connessione.last_insert_rowid()))
    }

    fn pending_messages(&self, user_id: UserId) -> Result<Vec<PendingMessage>, StorageError> {
        let connessione = self.blocca();
        let mut interrogazione = connessione.prepare(
            "SELECT body, sent_at FROM messages \
             WHERE user_id = ?1 AND direction = ?2 AND delivered = 0 \
             ORDER BY sent_at ASC, id ASC",
        )?;

        let righe = interrogazione.query_map(
            params![user_id.0, MessageDirection::Direct.as_str()],
            |riga| Ok((riga.get::<_, String>(0)?, riga.get::<_, String>(1)?)),
        )?;

        let mut messaggi = Vec::new();
        for riga in righe {
            let (body, inviato_il) = riga?;
            messaggi.push(PendingMessage {
                body,
                sent_at: analizza_istante(&inviato_il)?,
            });
        }

        Ok(messaggi)
    }

    fn mark_delivered(&self, id: MessageId) -> Result<(), StorageError> {
        let connessione = self.blocca();
        // Un id che non esiste aggiorna zero righe: `execute` non lo considera un
        // errore, ed e' il comportamento voluto.
        connessione.execute(
            "UPDATE messages SET delivered = 1 WHERE id = ?1",
            params![id.0],
        )?;

        Ok(())
    }

    fn clear_pending_messages(&self, user_id: UserId) -> Result<(), StorageError> {
        let connessione = self.blocca();
        // Il filtro sulla direzione ripete quello di `pending_messages`: le due
        // operazioni devono avere la stessa idea di cosa sia un messaggio in attesa,
        // altrimenti lo svuotamento marcherebbe righe che nessuno ha mai letto.
        connessione.execute(
            "UPDATE messages SET delivered = 1 \
             WHERE user_id = ?1 AND direction = ?2 AND delivered = 0",
            params![user_id.0, MessageDirection::Direct.as_str()],
        )?;

        Ok(())
    }

    fn list_users(&self) -> Result<Vec<StoredUser>, StorageError> {
        let connessione = self.blocca();
        let mut interrogazione = connessione
            .prepare("SELECT id, username, password_hash, created_at FROM users ORDER BY id ASC")?;

        let righe = interrogazione.query_map([], |riga| {
            Ok((riga.get(0)?, riga.get(1)?, riga.get(2)?, riga.get(3)?))
        })?;

        let mut utenti = Vec::new();
        for riga in righe {
            utenti.push(utente_da_colonne(riga?)?);
        }

        Ok(utenti)
    }
}

/// Messaggio come conservato da [`InMemoryRepository`].
///
/// Tiene la direzione anche se nessun metodo di [`Repository`] la restituisce: e' cio' su
/// cui [`Repository::pending_messages`] filtra, quindi senza di essa questo backend
/// tornerebbe anche i messaggi che l'utente ha mandato al server e divergerebbe da quello
/// su SQLite.
#[derive(Debug, Clone)]
struct MessaggioSalvato {
    /// Identificatore assegnato alla scrittura, come il `rowid` di SQLite.
    id: MessageId,
    /// Destinatario, `None` per i messaggi senza un singolo destinatario.
    user_id: Option<UserId>,
    /// Verso del messaggio rispetto al server.
    direction: MessageDirection,
    /// Testo del messaggio.
    body: String,
    /// Istante di produzione.
    sent_at: DateTime<Utc>,
    /// Vero quando il messaggio e' gia' stato consegnato.
    delivered: bool,
}

/// Vero se il messaggio e' fra quelli che il server deve ancora recapitare all'utente.
///
/// Sta in una funzione sola perche' [`Repository::pending_messages`] e
/// [`Repository::clear_pending_messages`] devono leggere e marcare esattamente le stesse
/// righe: due condizioni scritte a mano divergerebbero alla prima modifica. E' l'equivalente
/// della clausola `WHERE` che le due interrogazioni condividono nel backend su SQLite.
fn e_in_attesa(messaggio: &MessaggioSalvato, user_id: UserId) -> bool {
    messaggio.user_id == Some(user_id)
        && messaggio.direction == MessageDirection::Direct
        && !messaggio.delivered
}

/// Contenuto di [`InMemoryRepository`], protetto da un solo lock.
///
/// Le tre raccolte stanno sotto lo stesso [`Mutex`] e non uno ciascuna: le operazioni
/// ne toccano piu' di una per volta (inserire una posizione richiede di sapere se
/// l'utente esiste) e un lock unico rende quel controllo atomico senza ordini di
/// acquisizione da rispettare.
#[derive(Debug, Default)]
struct Stato {
    /// Utenti indicizzati per id.
    utenti: HashMap<UserId, StoredUser>,
    /// Posizioni in ordine di inserimento, come le righe di `positions`.
    posizioni: Vec<PositionRecord>,
    /// Messaggi in ordine di inserimento, come le righe di `messages`.
    messaggi: Vec<MessaggioSalvato>,
    /// Ultimo id assegnato: il prossimo utente prende `ultimo_id + 1`.
    ultimo_id: u32,
    /// Ultimo id di messaggio assegnato, come il `rowid` che SQLite fa crescere da se'.
    ultimo_id_messaggio: i64,
}

/// Repository in memoria, con la stessa semantica di [`SqliteRepository`].
///
/// Serve ai test della logica di dominio, che cosi' non aprono file ne' pagano il costo
/// di SQLite. Non persiste nulla: alla caduta della struttura i dati spariscono.
pub struct InMemoryRepository {
    /// Dati conservati.
    stato: Mutex<Stato>,
    /// Sorgente dell'ora usata per `created_at`.
    orologio: Arc<dyn Clock>,
}

impl InMemoryRepository {
    /// Costruisce un repository vuoto che legge l'ora dall'orologio indicato.
    pub fn new(orologio: Arc<dyn Clock>) -> Self {
        Self {
            stato: Mutex::new(Stato::default()),
            orologio,
        }
    }

    /// Prende il lock sullo stato, riprendendolo anche se e' avvelenato.
    ///
    /// Come in [`SqliteRepository::blocca`], le sezioni critiche sono singole
    /// operazioni che non lasciano lo stato a meta', quindi l'avvelenamento non rende
    /// i dati inconsistenti.
    fn blocca(&self) -> MutexGuard<'_, Stato> {
        self.stato.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl Repository for InMemoryRepository {
    fn create_user(
        &self,
        username: &Username,
        password_hash: &str,
    ) -> Result<UserId, StorageError> {
        let creato_il = self.orologio.now();

        let mut stato = self.blocca();
        if stato.utenti.values().any(|u| u.username == *username) {
            return Err(StorageError::UsernameTaken);
        }

        // Gli id partono da 1 e crescono di uno, come le chiavi primarie di SQLite.
        let id =
            UserId(stato.ultimo_id.checked_add(1).ok_or_else(|| {
                StorageError::Serialization("id degli utenti esauriti".to_owned())
            })?);
        stato.ultimo_id = id.0;

        stato.utenti.insert(
            id,
            StoredUser {
                id,
                username: username.clone(),
                password_hash: password_hash.to_owned(),
                created_at: creato_il,
            },
        );

        Ok(id)
    }

    fn find_user_by_username(
        &self,
        username: &Username,
    ) -> Result<Option<StoredUser>, StorageError> {
        let stato = self.blocca();
        Ok(stato
            .utenti
            .values()
            .find(|utente| utente.username == *username)
            .cloned())
    }

    fn insert_position(&self, record: &PositionRecord) -> Result<(), StorageError> {
        let mut stato = self.blocca();
        // Equivale al vincolo di chiave esterna su `positions.user_id`.
        if !stato.utenti.contains_key(&record.user_id) {
            return Err(StorageError::UserNotFound);
        }

        stato.posizioni.push(record.clone());
        Ok(())
    }

    fn positions_in_range(
        &self,
        user_id: UserId,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<Vec<PositionRecord>, StorageError> {
        let stato = self.blocca();
        let mut trovate: Vec<PositionRecord> = stato
            .posizioni
            .iter()
            .filter(|posizione| {
                posizione.user_id == user_id && (from..=to).contains(&posizione.recorded_at)
            })
            .cloned()
            .collect();

        // `sort_by_key` e' stabile: a parita' di istante resta l'ordine di inserimento,
        // che e' quello che SQLite ottiene con `ORDER BY recorded_at ASC, id ASC`.
        trovate.sort_by_key(|posizione| posizione.recorded_at);

        Ok(trovate)
    }

    fn insert_message(
        &self,
        user_id: Option<UserId>,
        direction: MessageDirection,
        body: &str,
        at: DateTime<Utc>,
    ) -> Result<MessageId, StorageError> {
        let mut stato = self.blocca();
        // Gli id partono da 1 e crescono di uno, come i `rowid` di SQLite.
        let id =
            MessageId(stato.ultimo_id_messaggio.checked_add(1).ok_or_else(|| {
                StorageError::Serialization("id dei messaggi esauriti".to_owned())
            })?);
        stato.ultimo_id_messaggio = id.0;

        stato.messaggi.push(MessaggioSalvato {
            id,
            user_id,
            direction,
            body: body.to_owned(),
            sent_at: at,
            delivered: false,
        });

        Ok(id)
    }

    fn pending_messages(&self, user_id: UserId) -> Result<Vec<PendingMessage>, StorageError> {
        let stato = self.blocca();
        let mut trovati: Vec<&MessaggioSalvato> = stato
            .messaggi
            .iter()
            .filter(|messaggio| e_in_attesa(messaggio, user_id))
            .collect();

        trovati.sort_by_key(|messaggio| messaggio.sent_at);

        Ok(trovati
            .into_iter()
            .map(|messaggio| PendingMessage {
                body: messaggio.body.clone(),
                sent_at: messaggio.sent_at,
            })
            .collect())
    }

    fn mark_delivered(&self, id: MessageId) -> Result<(), StorageError> {
        let mut stato = self.blocca();
        // Un id che non esiste non trova nulla da marcare: come l'UPDATE di SQLite che
        // aggiorna zero righe, non e' un errore.
        if let Some(messaggio) = stato.messaggi.iter_mut().find(|salvato| salvato.id == id) {
            messaggio.delivered = true;
        }

        Ok(())
    }

    fn clear_pending_messages(&self, user_id: UserId) -> Result<(), StorageError> {
        let mut stato = self.blocca();
        for messaggio in &mut stato.messaggi {
            if e_in_attesa(messaggio, user_id) {
                messaggio.delivered = true;
            }
        }

        Ok(())
    }

    fn list_users(&self) -> Result<Vec<StoredUser>, StorageError> {
        let stato = self.blocca();
        let mut utenti: Vec<StoredUser> = stato.utenti.values().cloned().collect();
        utenti.sort_by_key(|utente| utente.id);

        Ok(utenti)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    use chrono::TimeDelta;
    use georuggine_common::clock::FakeClock;

    /// Istante su cui e' fermo l'orologio finto dei test: 2024-01-01T00:00:00Z.
    fn istante_base() -> DateTime<Utc> {
        DateTime::from_timestamp(1_704_067_200, 0).expect("istante valido")
    }

    /// Costruisce uno username che il test da' per valido.
    fn utente(nome: &str) -> Username {
        Username::new(nome).expect("username di test valido")
    }

    /// Coordinata associata all'offset in secondi di una posizione di prova.
    ///
    /// Insieme a `recorded_at` fa da impronta del record: se una query restituisse le
    /// posizioni sbagliate, o le mescolasse fra utenti, le coordinate non tornerebbero.
    fn posizione_di(offset: i64) -> Coordinate {
        Coordinate::new(45.0 + offset as f64 / 1_000.0, 7.6869).expect("coordinata valida")
    }

    /// Percorso di un file di prova nella cartella temporanea, unico per esecuzione.
    fn percorso_temporaneo(nome: &str) -> PathBuf {
        let unico = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        std::env::temp_dir().join(format!(
            "georuggine-{nome}-{}-{unico}.sqlite3",
            std::process::id()
        ))
    }

    /// Cancella il file di prova e i file ausiliari che il modo WAL si porta dietro.
    fn rimuovi(percorso: &Path) {
        for sospeso in ["", "-wal", "-shm"] {
            let mut nome = percorso.as_os_str().to_owned();
            nome.push(sospeso);
            let _ = std::fs::remove_file(PathBuf::from(nome));
        }
    }

    /// Verifica il contratto che ogni [`Repository`] deve rispettare.
    ///
    /// E' generica sull'implementazione e viene chiamata una volta per backend: cosi'
    /// una differenza di comportamento fra SQLite e in memoria non e' possibile, perche'
    /// entrambi passano dagli stessi identici controlli. Comprende anche l'ordinamento
    /// dei risultati, che senza un test condiviso divergerebbe facilmente.
    ///
    /// Il repository passato deve essere costruito su un orologio fermo su
    /// [`istante_base`], perche' i controlli su `created_at` confrontano quel valore.
    fn verifica_contratto<R: Repository>(repo: R) {
        let t0 = istante_base();

        // --- creazione e ricerca di un utente ---

        let nome = utente("veicolo_01");
        let id = repo
            .create_user(&nome, "hash-argon2")
            .expect("creazione dell'utente riuscita");

        let trovato = repo
            .find_user_by_username(&nome)
            .expect("ricerca riuscita")
            .expect("l'utente appena creato deve esistere");
        assert_eq!(trovato.id, id);
        assert_eq!(trovato.username, nome);
        assert_eq!(trovato.password_hash, "hash-argon2");
        assert_eq!(trovato.created_at, t0);

        // --- username duplicato ---

        let esito = repo.create_user(&nome, "un-altro-hash");
        assert!(
            matches!(esito, Err(StorageError::UsernameTaken)),
            "atteso UsernameTaken, ottenuto {esito:?}"
        );

        // Il tentativo fallito non deve aver toccato la riga esistente.
        let dopo_il_duplicato = repo
            .find_user_by_username(&nome)
            .expect("ricerca riuscita")
            .expect("l'utente esiste ancora");
        assert_eq!(dopo_il_duplicato.password_hash, "hash-argon2");

        // --- username mai creato: assenza, non errore ---

        assert_eq!(
            repo.find_user_by_username(&utente("mai_visto"))
                .expect("la ricerca di uno username assente non e' un errore"),
            None
        );

        // --- elenco degli utenti, ordinato per id ---

        let secondo = repo
            .create_user(&utente("veicolo_02"), "hash-2")
            .expect("creazione del secondo utente riuscita");
        assert_ne!(secondo, id, "gli id assegnati devono essere distinti");

        let elenco = repo.list_users().expect("elenco riuscito");
        assert_eq!(
            elenco.iter().map(|u| u.id).collect::<Vec<_>>(),
            vec![id, secondo]
        );

        // --- posizioni ---

        // Inserite fuori ordine cronologico: se la query non ordinasse davvero per
        // `recorded_at`, tornerebbero nell'ordine di inserimento e il test fallirebbe.
        for offset in [30, 0, 40, 10, 20] {
            repo.insert_position(&PositionRecord {
                user_id: id,
                coordinate: posizione_di(offset),
                recorded_at: t0 + TimeDelta::seconds(offset),
            })
            .expect("inserimento della posizione riuscito");
        }

        let nel_range = repo
            .positions_in_range(id, t0 + TimeDelta::seconds(10), t0 + TimeDelta::seconds(30))
            .expect("query sull'intervallo riuscita");

        assert_eq!(
            nel_range.len(),
            3,
            "l'intervallo [10s, 30s] contiene 3 delle 5 posizioni: {nel_range:?}"
        );
        assert_eq!(
            nel_range.iter().map(|p| p.recorded_at).collect::<Vec<_>>(),
            vec![
                t0 + TimeDelta::seconds(10),
                t0 + TimeDelta::seconds(20),
                t0 + TimeDelta::seconds(30),
            ]
        );
        for (posizione, offset) in nel_range.iter().zip([10, 20, 30]) {
            assert_eq!(posizione.user_id, id);
            assert_eq!(posizione.coordinate, posizione_di(offset));
        }

        // --- intervallo senza posizioni: vettore vuoto, non errore ---

        let vuoto = repo
            .positions_in_range(
                id,
                t0 + TimeDelta::seconds(100),
                t0 + TimeDelta::seconds(200),
            )
            .expect("un intervallo vuoto non e' un errore");
        assert!(
            vuoto.is_empty(),
            "atteso nessun risultato, ottenuto {vuoto:?}"
        );

        // Le posizioni di un utente non compaiono fra quelle di un altro.
        let altrui = repo
            .positions_in_range(secondo, t0, t0 + TimeDelta::seconds(100))
            .expect("query riuscita");
        assert!(
            altrui.is_empty(),
            "posizioni attribuite all'utente sbagliato"
        );

        // --- scritture riferite a un utente inesistente ---

        let esito = repo.insert_position(&PositionRecord {
            user_id: UserId(9_999),
            coordinate: posizione_di(0),
            recorded_at: t0,
        });
        assert!(
            matches!(esito, Err(StorageError::UserNotFound)),
            "atteso UserNotFound, ottenuto {esito:?}"
        );

        // --- letture riferite a un utente inesistente: vuoto, non errore ---

        assert!(
            repo.positions_in_range(UserId(9_999), t0, t0 + TimeDelta::seconds(100))
                .expect("la lettura per un utente assente non e' un errore")
                .is_empty()
        );
        assert!(
            repo.pending_messages(UserId(9_999))
                .expect("la lettura per un utente assente non e' un errore")
                .is_empty()
        );

        // --- messaggi in attesa ---
        //
        // In attesa e' solo cio' che il server deve ancora recapitare, cioe' i messaggi
        // diretti non consegnati. Nella stessa tabella stanno anche il testo che il
        // veicolo ha mandato al server (Inbound) e gli avvisi alla flotta (Broadcast):
        // senza il filtro sulla direzione un veicolo, al ritorno, si vedrebbe recapitare
        // i propri stessi messaggi.

        let piu_recente = repo
            .insert_message(
                Some(id),
                MessageDirection::Direct,
                "torna in sede",
                t0 + TimeDelta::seconds(5),
            )
            .expect("inserimento del messaggio riuscito");
        let piu_vecchio = repo
            .insert_message(
                Some(id),
                MessageDirection::Direct,
                "passa dal deposito",
                t0 + TimeDelta::seconds(1),
            )
            .expect("inserimento del messaggio riuscito");
        assert_ne!(
            piu_recente, piu_vecchio,
            "ogni messaggio scritto ha un id suo"
        );

        // Messaggio che l'utente ha mandato al server: e' suo, non e' ancora marcato, e
        // non deve comparire fra quelli che gli tornano indietro.
        repo.insert_message(
            Some(id),
            MessageDirection::Inbound,
            "sono in arrivo al deposito",
            t0 + TimeDelta::seconds(2),
        )
        .expect("inserimento del messaggio riuscito");
        // Un avviso alla flotta registrato con il destinatario: nemmeno questo e' un
        // messaggio in attesa, perche' vale per chi era collegato quando e' partito.
        repo.insert_message(
            Some(id),
            MessageDirection::Broadcast,
            "chiusura turno",
            t0 + TimeDelta::seconds(3),
        )
        .expect("inserimento del messaggio riuscito");
        repo.insert_message(
            Some(secondo),
            MessageDirection::Direct,
            "non per te",
            t0 + TimeDelta::seconds(2),
        )
        .expect("inserimento del messaggio riuscito");
        repo.insert_message(
            None,
            MessageDirection::Broadcast,
            "avviso senza destinatario",
            t0,
        )
        .expect("un messaggio senza destinatario e' ammesso");

        // La tabella `messages` non dichiara una chiave esterna su `user_id`, quindi un
        // messaggio per un utente inesistente viene accettato invece di dare
        // UserNotFound come fa `insert_position`. E' un'asimmetria dello schema, non
        // una svista: il controllo la fissa perche' i due backend non divergano.
        repo.insert_message(
            Some(UserId(9_999)),
            MessageDirection::Direct,
            "destinatario mai registrato",
            t0,
        )
        .expect("messages non ha un vincolo di chiave esterna");
        assert_eq!(
            repo.pending_messages(UserId(9_999))
                .expect("lettura riuscita")
                .len(),
            1
        );

        let pendenti = repo.pending_messages(id).expect("lettura riuscita");
        assert_eq!(
            pendenti.iter().map(|m| m.body.as_str()).collect::<Vec<_>>(),
            vec!["passa dal deposito", "torna in sede"],
            "attesi i soli messaggi diretti dell'utente, ordinati per sent_at crescente"
        );
        assert_eq!(pendenti[0].sent_at, t0 + TimeDelta::seconds(1));
        assert_eq!(pendenti[1].sent_at, t0 + TimeDelta::seconds(5));

        // --- marcatura di un singolo messaggio ---

        repo.mark_delivered(piu_vecchio)
            .expect("marcatura riuscita");
        assert_eq!(
            repo.pending_messages(id)
                .expect("lettura riuscita")
                .iter()
                .map(|m| m.body.as_str())
                .collect::<Vec<_>>(),
            vec!["torna in sede"],
            "la marcatura riguarda il solo messaggio indicato, non tutti quelli dell'utente"
        );
        assert_eq!(
            repo.pending_messages(secondo)
                .expect("lettura riuscita")
                .len(),
            1,
            "la marcatura non tocca gli altri utenti"
        );

        // Marcare due volte, o marcare un id che non esiste, non e' un errore.
        repo.mark_delivered(piu_vecchio)
            .expect("marcatura idempotente");
        repo.mark_delivered(MessageId(999_999))
            .expect("marcare un id sconosciuto non e' un errore");
        assert_eq!(
            repo.pending_messages(id).expect("lettura riuscita").len(),
            1
        );

        // --- svuotamento ---

        repo.clear_pending_messages(id)
            .expect("svuotamento riuscito");
        assert!(
            repo.pending_messages(id)
                .expect("lettura riuscita")
                .is_empty(),
            "dopo lo svuotamento non deve restare nulla in attesa"
        );

        // Lo svuotamento tocca solo l'utente indicato.
        assert_eq!(
            repo.pending_messages(secondo)
                .expect("lettura riuscita")
                .len(),
            1,
            "lo svuotamento ha cancellato anche i messaggi di un altro utente"
        );

        // Svuotare due volte, o svuotare quando non c'e' nulla, non e' un errore.
        repo.clear_pending_messages(id)
            .expect("svuotamento idempotente");
        repo.clear_pending_messages(UserId(9_999))
            .expect("svuotare per un utente assente non e' un errore");
    }

    #[test]
    fn sqlite_rispetta_il_contratto() {
        let orologio = Arc::new(FakeClock::new(istante_base()));
        let repo =
            SqliteRepository::new(":memory:", orologio).expect("apertura in memoria riuscita");
        verifica_contratto(repo);
    }

    #[test]
    fn in_memory_rispetta_il_contratto() {
        let orologio = Arc::new(FakeClock::new(istante_base()));
        verifica_contratto(InMemoryRepository::new(orologio));
    }

    #[test]
    fn sqlite_ordina_gli_istanti_con_frazioni_di_secondo() {
        let orologio = Arc::new(FakeClock::new(istante_base()));
        let repo =
            SqliteRepository::new(":memory:", orologio).expect("apertura in memoria riuscita");
        let id = repo
            .create_user(&utente("veicolo_01"), "hash")
            .expect("creazione riuscita");
        let t0 = istante_base();

        // Istanti dentro lo stesso secondo, inseriti fuori ordine: e' il caso in cui un
        // formato a precisione variabile farebbe ordinare a SQLite il testo in modo
        // diverso dal tempo. Serve anche a verificare che i nanosecondi sopravvivano al
        // giro di andata e ritorno.
        let sparsi = [
            t0 + TimeDelta::milliseconds(500),
            t0,
            t0 + TimeDelta::nanoseconds(1),
            t0 + TimeDelta::milliseconds(50),
        ];
        for istante in sparsi {
            repo.insert_position(&PositionRecord {
                user_id: id,
                coordinate: posizione_di(0),
                recorded_at: istante,
            })
            .expect("inserimento riuscito");
        }

        let lette = repo
            .positions_in_range(id, t0, t0 + TimeDelta::seconds(1))
            .expect("query riuscita");
        assert_eq!(
            lette.iter().map(|p| p.recorded_at).collect::<Vec<_>>(),
            vec![
                t0,
                t0 + TimeDelta::nanoseconds(1),
                t0 + TimeDelta::milliseconds(50),
                t0 + TimeDelta::milliseconds(500),
            ]
        );
    }

    #[test]
    fn sqlite_riapre_un_file_esistente_senza_perdere_i_dati() {
        let percorso = percorso_temporaneo("riapertura");
        rimuovi(&percorso);
        let nome = utente("veicolo_01");

        let id = {
            let repo = SqliteRepository::new(&percorso, Arc::new(FakeClock::new(istante_base())))
                .expect("prima apertura riuscita");
            repo.create_user(&nome, "hash").expect("creazione riuscita")
        };

        // La seconda apertura ricrea lo schema con CREATE TABLE IF NOT EXISTS: deve
        // essere idempotente e lasciare intatte le righe gia' presenti.
        let repo = SqliteRepository::new(&percorso, Arc::new(FakeClock::new(istante_base())))
            .expect("riapertura sul file esistente riuscita");
        let trovato = repo
            .find_user_by_username(&nome)
            .expect("ricerca riuscita")
            .expect("l'utente scritto dalla sessione precedente deve esserci");
        assert_eq!(trovato.id, id);

        drop(repo);
        rimuovi(&percorso);
    }

    #[test]
    fn storage_error_si_mostra_in_italiano() {
        assert_eq!(
            StorageError::UsernameTaken.to_string(),
            "username gia' registrato"
        );
        assert_eq!(StorageError::UserNotFound.to_string(), "utente inesistente");
        assert_eq!(
            StorageError::Serialization("data non valida".to_owned()).to_string(),
            "dato illeggibile dal database: data non valida"
        );
    }
}
