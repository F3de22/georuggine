//! Persistenza del server su SQLite: utenti, posizioni e messaggi.
//!
//! I metodi di [`Repository`] sono sincroni e il chiamante li esegue in `spawn_blocking`.
//! Gli istanti sono salvati come testo RFC 3339 a nanosecondi fissi, cosi' l'ordinamento
//! del testo coincide con quello cronologico.

use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use chrono::{DateTime, SecondsFormat, Utc};
use georuggine_common::clock::Clock;
use georuggine_common::types::{Coordinate, PositionRecord, UserId, Username};
use rusqlite::{Connection, OptionalExtension, params};
use thiserror::Error;

/// Schema del database, applicato a ogni apertura; ogni `CREATE` e' idempotente.
///
/// `foreign_keys` va attivato a ogni connessione perche' SQLite lo tiene spento.
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
    /// Errore di SQLite che non corrisponde a nessun caso di dominio.
    #[error("errore del database: {0}")]
    Sqlite(#[from] rusqlite::Error),
    /// Lo username e' gia' assegnato a un altro utente.
    #[error("username gia' registrato")]
    UsernameTaken,
    /// L'operazione fa riferimento a un utente che non esiste.
    #[error("utente inesistente")]
    UserNotFound,
    /// Una riga letta non si converte nel tipo di dominio.
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
    /// Istante di registrazione.
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
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Broadcast => "Broadcast",
            Self::Direct => "Direct",
            Self::Inbound => "Inbound",
        }
    }
}

/// Identificatore di una riga di `messages`, distinto da [`UserId`] a livello di tipo.
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

/// Accesso ai dati persistiti del server, condiviso fra i task come `Arc<dyn Repository>`.
///
/// Le letture per un utente inesistente restituiscono un risultato vuoto, non un errore.
pub trait Repository: Send + Sync + 'static {
    /// Registra un nuovo utente e ne restituisce l'id assegnato.
    ///
    /// # Errori
    ///
    /// [`StorageError::UsernameTaken`] se lo username e' gia' registrato.
    fn create_user(&self, username: &Username, password_hash: &str)
    -> Result<UserId, StorageError>;

    /// Cerca un utente per username; `Ok(None)` se non esiste.
    ///
    /// # Errori
    ///
    /// [`StorageError::Serialization`] se la riga non e' valida.
    fn find_user_by_username(
        &self,
        username: &Username,
    ) -> Result<Option<StoredUser>, StorageError>;

    /// Aggiunge una posizione allo storico.
    ///
    /// # Errori
    ///
    /// [`StorageError::UserNotFound`] se `record.user_id` non esiste.
    fn insert_position(&self, record: &PositionRecord) -> Result<(), StorageError>;

    /// Posizioni di un utente nell'intervallo, estremi inclusi, in ordine cronologico.
    ///
    /// # Errori
    ///
    /// [`StorageError::Serialization`] se una riga non e' valida.
    fn positions_in_range(
        &self,
        user_id: UserId,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<Vec<PositionRecord>, StorageError>;

    /// Salva un messaggio non ancora consegnato e ne restituisce l'id; `user_id` e'
    /// `None` per i broadcast.
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

    /// Messaggi diretti non ancora consegnati all'utente, in ordine di invio.
    ///
    /// # Errori
    ///
    /// [`StorageError::Serialization`] se una riga non e' valida.
    fn pending_messages(&self, user_id: UserId) -> Result<Vec<PendingMessage>, StorageError>;

    /// Segna come consegnato un messaggio; un id inesistente non e' un errore.
    ///
    /// # Errori
    ///
    /// [`StorageError::Sqlite`] se la scrittura fallisce.
    fn mark_delivered(&self, id: MessageId) -> Result<(), StorageError>;

    /// Segna come consegnati tutti i messaggi in attesa dell'utente.
    ///
    /// # Errori
    ///
    /// [`StorageError::Sqlite`] se la scrittura fallisce.
    fn clear_pending_messages(&self, user_id: UserId) -> Result<(), StorageError>;

    /// Utenti registrati, in ordine di id.
    ///
    /// # Errori
    ///
    /// [`StorageError::Serialization`] se una riga non e' valida.
    fn list_users(&self) -> Result<Vec<StoredUser>, StorageError>;
}

/// Rappresentazione testuale di un istante nel database, a precisione fissa.
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

/// Converte l'id di una riga in [`UserId`] senza troncare.
fn id_da_riga(grezzo: i64) -> Result<UserId, StorageError> {
    u32::try_from(grezzo).map(UserId).map_err(|_| {
        StorageError::Serialization(format!("id utente {grezzo} fuori dall'intervallo di u32"))
    })
}

/// Riconosce la violazione del vincolo `UNIQUE` su `users.username`.
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

/// Riconosce la violazione della chiave esterna verso `users(id)`.
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

/// Costruisce uno [`StoredUser`] dalle colonne lette.
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

/// Repository su file SQLite; la connessione sta dietro un [`Mutex`] perche' non e'
/// `Sync`.
pub struct SqliteRepository {
    connessione: Mutex<Connection>,
    /// Sorgente dell'ora usata per `created_at`.
    orologio: Arc<dyn Clock>,
}

impl SqliteRepository {
    /// Apre il database (anche `":memory:"`) e applica lo schema.
    ///
    /// # Errori
    ///
    /// [`StorageError::Sqlite`] se il file non si apre o lo schema non si applica.
    pub fn new(percorso: impl AsRef<Path>, orologio: Arc<dyn Clock>) -> Result<Self, StorageError> {
        let connessione = Connection::open(percorso)?;
        connessione.execute_batch(SCHEMA)?;

        Ok(Self {
            connessione: Mutex::new(connessione),
            orologio,
        })
    }

    /// Prende il lock anche se avvelenato: ogni metodo esegue statement completi.
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

        // Stesso lock dell'inserimento: nessun'altra scrittura in mezzo.
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
        connessione.execute(
            "UPDATE messages SET delivered = 1 WHERE id = ?1",
            params![id.0],
        )?;

        Ok(())
    }

    fn clear_pending_messages(&self, user_id: UserId) -> Result<(), StorageError> {
        let connessione = self.blocca();
        // Stesso filtro di `pending_messages`.
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

#[cfg(test)]
mod tests {
    use super::*;

    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    use chrono::TimeDelta;
    use georuggine_common::clock::FakeClock;

    fn istante_base() -> DateTime<Utc> {
        DateTime::from_timestamp(1_704_067_200, 0).expect("istante valido")
    }

    fn utente(nome: &str) -> Username {
        Username::new(nome).expect("username di test valido")
    }

    /// Coordinata associata all'offset di una posizione di prova, per riconoscerla.
    fn posizione_di(offset: i64) -> Coordinate {
        Coordinate::new(45.0 + offset as f64 / 1_000.0, 7.6869).expect("coordinata valida")
    }

    /// Repository su un database in RAM, con l'orologio fermo su [`istante_base`].
    fn repo_in_memoria() -> SqliteRepository {
        SqliteRepository::new(":memory:", Arc::new(FakeClock::new(istante_base())))
            .expect("apertura in memoria riuscita")
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

    /// Cancella il file di prova e i file ausiliari del modo WAL.
    fn rimuovi(percorso: &Path) {
        for sospeso in ["", "-wal", "-shm"] {
            let mut nome = percorso.as_os_str().to_owned();
            nome.push(sospeso);
            let _ = std::fs::remove_file(PathBuf::from(nome));
        }
    }

    #[test]
    fn gli_utenti_si_creano_si_cercano_e_si_elencano_per_id() {
        let repo = repo_in_memoria();
        let nome = utente("veicolo_01");

        let id = repo
            .create_user(&nome, "hash-argon2")
            .expect("creazione riuscita");
        let trovato = repo
            .find_user_by_username(&nome)
            .expect("ricerca riuscita")
            .expect("l'utente appena creato deve esistere");
        assert_eq!(trovato.id, id);
        assert_eq!(trovato.username, nome);
        assert_eq!(trovato.password_hash, "hash-argon2");
        assert_eq!(trovato.created_at, istante_base());

        let esito = repo.create_user(&nome, "un-altro-hash");
        assert!(
            matches!(esito, Err(StorageError::UsernameTaken)),
            "atteso UsernameTaken, ottenuto {esito:?}"
        );
        assert_eq!(
            repo.find_user_by_username(&nome)
                .expect("ricerca riuscita")
                .expect("l'utente esiste ancora")
                .password_hash,
            "hash-argon2",
            "il duplicato rifiutato non deve toccare la riga esistente"
        );

        assert_eq!(
            repo.find_user_by_username(&utente("mai_visto"))
                .expect("la ricerca di uno username assente non e' un errore"),
            None
        );

        let secondo = repo
            .create_user(&utente("veicolo_02"), "hash-2")
            .expect("creazione riuscita");
        assert_eq!(
            repo.list_users()
                .expect("elenco riuscito")
                .iter()
                .map(|u| u.id)
                .collect::<Vec<_>>(),
            vec![id, secondo]
        );
    }

    #[test]
    fn le_posizioni_si_leggono_per_utente_e_intervallo_in_ordine_di_tempo() {
        let repo = repo_in_memoria();
        let t0 = istante_base();
        let id = repo
            .create_user(&utente("veicolo_01"), "hash")
            .expect("creazione riuscita");
        let secondo = repo
            .create_user(&utente("veicolo_02"), "hash")
            .expect("creazione riuscita");

        // Inserite fuori ordine: la query deve ordinarle per `recorded_at`.
        for offset in [30, 0, 40, 10, 20] {
            repo.insert_position(&PositionRecord {
                user_id: id,
                coordinate: posizione_di(offset),
                recorded_at: t0 + TimeDelta::seconds(offset),
            })
            .expect("inserimento riuscito");
        }

        let nel_range = repo
            .positions_in_range(id, t0 + TimeDelta::seconds(10), t0 + TimeDelta::seconds(30))
            .expect("query riuscita");
        assert_eq!(
            nel_range.iter().map(|p| p.recorded_at).collect::<Vec<_>>(),
            vec![
                t0 + TimeDelta::seconds(10),
                t0 + TimeDelta::seconds(20),
                t0 + TimeDelta::seconds(30),
            ],
            "gli estremi sono inclusi e l'ordine e' cronologico"
        );
        for (posizione, offset) in nel_range.iter().zip([10, 20, 30]) {
            assert_eq!(posizione.coordinate, posizione_di(offset));
        }

        for (utente_cercato, da, a) in [(id, 100, 200), (secondo, 0, 100), (UserId(9_999), 0, 100)]
        {
            assert!(
                repo.positions_in_range(
                    utente_cercato,
                    t0 + TimeDelta::seconds(da),
                    t0 + TimeDelta::seconds(a)
                )
                .expect("una lettura senza risultati non e' un errore")
                .is_empty()
            );
        }

        let esito = repo.insert_position(&PositionRecord {
            user_id: UserId(9_999),
            coordinate: posizione_di(0),
            recorded_at: t0,
        });
        assert!(
            matches!(esito, Err(StorageError::UserNotFound)),
            "atteso UserNotFound, ottenuto {esito:?}"
        );
    }

    #[test]
    fn in_attesa_sono_i_soli_messaggi_diretti_non_consegnati() {
        let repo = repo_in_memoria();
        let t0 = istante_base();
        let id = repo
            .create_user(&utente("veicolo_01"), "hash")
            .expect("creazione riuscita");
        let secondo = repo
            .create_user(&utente("veicolo_02"), "hash")
            .expect("creazione riuscita");

        let piu_recente = repo
            .insert_message(
                Some(id),
                MessageDirection::Direct,
                "torna in sede",
                t0 + TimeDelta::seconds(5),
            )
            .expect("inserimento riuscito");
        let piu_vecchio = repo
            .insert_message(
                Some(id),
                MessageDirection::Direct,
                "passa dal deposito",
                t0 + TimeDelta::seconds(1),
            )
            .expect("inserimento riuscito");
        assert_ne!(piu_recente, piu_vecchio, "ogni messaggio ha un id suo");

        // Solo i diretti non consegnati sono in attesa.
        for (destinatario, direzione, corpo) in [
            (Some(id), MessageDirection::Inbound, "sono in arrivo"),
            (Some(id), MessageDirection::Broadcast, "chiusura turno"),
            (Some(secondo), MessageDirection::Direct, "non per te"),
        ] {
            repo.insert_message(destinatario, direzione, corpo, t0 + TimeDelta::seconds(2))
                .expect("inserimento riuscito");
        }
        repo.insert_message(None, MessageDirection::Broadcast, "senza destinatario", t0)
            .expect("un messaggio senza destinatario e' ammesso");

        let pendenti = repo.pending_messages(id).expect("lettura riuscita");
        assert_eq!(
            pendenti.iter().map(|m| m.body.as_str()).collect::<Vec<_>>(),
            vec!["passa dal deposito", "torna in sede"],
            "attesi i soli diretti dell'utente, per sent_at crescente"
        );
        assert_eq!(pendenti[0].sent_at, t0 + TimeDelta::seconds(1));
    }

    #[test]
    fn la_marcatura_riguarda_un_messaggio_solo_lo_svuotamento_un_utente_solo() {
        let repo = repo_in_memoria();
        let t0 = istante_base();
        let id = repo
            .create_user(&utente("veicolo_01"), "hash")
            .expect("creazione riuscita");
        let secondo = repo
            .create_user(&utente("veicolo_02"), "hash")
            .expect("creazione riuscita");

        let primo = repo
            .insert_message(Some(id), MessageDirection::Direct, "primo", t0)
            .expect("inserimento riuscito");
        repo.insert_message(
            Some(id),
            MessageDirection::Direct,
            "secondo",
            t0 + TimeDelta::seconds(1),
        )
        .expect("inserimento riuscito");
        repo.insert_message(Some(secondo), MessageDirection::Direct, "altrui", t0)
            .expect("inserimento riuscito");

        repo.mark_delivered(primo).expect("marcatura riuscita");
        assert_eq!(
            repo.pending_messages(id)
                .expect("lettura riuscita")
                .iter()
                .map(|m| m.body.as_str())
                .collect::<Vec<_>>(),
            vec!["secondo"],
            "la marcatura riguarda il solo messaggio indicato"
        );

        // Marcare due volte, o marcare un id che non esiste, non e' un errore.
        repo.mark_delivered(primo).expect("marcatura idempotente");
        repo.mark_delivered(MessageId(999_999))
            .expect("un id sconosciuto non e' un errore");

        repo.clear_pending_messages(id)
            .expect("svuotamento riuscito");
        assert!(
            repo.pending_messages(id)
                .expect("lettura riuscita")
                .is_empty()
        );
        assert_eq!(
            repo.pending_messages(secondo)
                .expect("lettura riuscita")
                .len(),
            1,
            "lo svuotamento tocca solo l'utente indicato"
        );
        repo.clear_pending_messages(UserId(9_999))
            .expect("svuotare per un utente assente non e' un errore");
    }

    #[test]
    fn messages_non_ha_il_vincolo_di_chiave_esterna_di_positions() {
        // Lo schema accetta messaggi per utenti inesistenti, a differenza delle posizioni.
        let repo = repo_in_memoria();

        repo.insert_message(
            Some(UserId(9_999)),
            MessageDirection::Direct,
            "destinatario mai registrato",
            istante_base(),
        )
        .expect("messages non ha un vincolo di chiave esterna");

        assert_eq!(
            repo.pending_messages(UserId(9_999))
                .expect("lettura riuscita")
                .len(),
            1
        );
    }

    #[test]
    fn gli_istanti_si_ordinano_anche_dentro_lo_stesso_secondo() {
        let repo = repo_in_memoria();
        let t0 = istante_base();
        let id = repo
            .create_user(&utente("veicolo_01"), "hash")
            .expect("creazione riuscita");

        // Precisione variabile: verifica ordinamento e conservazione dei nanosecondi.
        for istante in [
            t0 + TimeDelta::milliseconds(500),
            t0,
            t0 + TimeDelta::nanoseconds(1),
            t0 + TimeDelta::milliseconds(50),
        ] {
            repo.insert_position(&PositionRecord {
                user_id: id,
                coordinate: posizione_di(0),
                recorded_at: istante,
            })
            .expect("inserimento riuscito");
        }

        assert_eq!(
            repo.positions_in_range(id, t0, t0 + TimeDelta::seconds(1))
                .expect("query riuscita")
                .iter()
                .map(|p| p.recorded_at)
                .collect::<Vec<_>>(),
            vec![
                t0,
                t0 + TimeDelta::nanoseconds(1),
                t0 + TimeDelta::milliseconds(50),
                t0 + TimeDelta::milliseconds(500),
            ]
        );
    }

    #[test]
    fn riaprire_un_file_esistente_non_perde_i_dati() {
        let percorso = percorso_temporaneo("riapertura");
        rimuovi(&percorso);
        let nome = utente("veicolo_01");

        let id = {
            let repo = SqliteRepository::new(&percorso, Arc::new(FakeClock::new(istante_base())))
                .expect("prima apertura riuscita");
            repo.create_user(&nome, "hash").expect("creazione riuscita")
        };

        // La seconda apertura riapplica lo schema senza toccare i dati.
        let repo = SqliteRepository::new(&percorso, Arc::new(FakeClock::new(istante_base())))
            .expect("riapertura riuscita");
        assert_eq!(
            repo.find_user_by_username(&nome)
                .expect("ricerca riuscita")
                .expect("l'utente scritto prima deve esserci")
                .id,
            id
        );

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
