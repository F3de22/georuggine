//! Registrazione e accesso degli utenti: hashing delle password, policy e sessioni.
//!
//! Le password non vengono mai salvate in chiaro: si conserva l'hash Argon2id nel
//! formato PHC, una stringa autocontenuta che porta con se' algoritmo, versione,
//! parametri e salt. La verifica rilegge i parametri dalla stringa, quindi cambiare i
//! valori di [`AuthConfig`] non invalida gli hash gia' scritti: le password vecchie
//! continuano a essere verificabili con i parametri con cui erano state calcolate.
//!
//! La password in chiaro non compare mai in un log, nemmeno a livello `trace`, e non
//! finisce in nessun messaggio d'errore: [`AuthError::WeakPassword`] descrive cosa
//! manca senza citare cio' che l'utente ha scritto.
//!
//! Argon2 e' lento per costruzione, ed e' li' che sta la sua sicurezza: farlo girare
//! direttamente in un task async fermerebbe un thread dell'esecutore per tutta la
//! durata. [`register`] e [`login`] lo eseguono percio' dentro
//! `tokio::task::spawn_blocking`, insieme alle chiamate al [`Repository`], che sono
//! sincrone.

use std::collections::HashMap;
use std::fmt::Write as _;
use std::sync::Arc;

use argon2::password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString};
use argon2::{Algorithm, Argon2, Params, Version};
use georuggine_common::types::{UserId, Username};
use rand::TryRng as _;
use rand::rngs::SysRng;
use thiserror::Error;
use tokio::sync::RwLock;
use tokio::task;

use crate::storage::{Repository, StorageError};

/// Numero minimo di caratteri di una password.
const PASSWORD_MIN_LEN: usize = 8;

/// Byte casuali del salt: e' la lunghezza consigliata dalla specifica PHC.
const SALT_BYTES: usize = 16;

/// Byte casuali di un token di sessione.
///
/// Con 256 bit di entropia indovinare un token e' fuori portata, e il registro delle
/// sessioni non ha bisogno di difendersi dai tentativi a forza bruta.
const TOKEN_BYTES: usize = 32;

/// Hash Argon2id usato da [`login`] quando lo username non esiste.
///
/// Serve a rendere indistinguibili i due modi di fallire un accesso. Senza, il caso
/// "utente inesistente" tornerebbe subito, mentre "password sbagliata" pagherebbe la
/// verifica Argon2: la differenza di tempo, misurabile da fuori, direbbe a chi prova
/// quali username esistono. Verificando comunque contro questa costante i due percorsi
/// fanno lo stesso lavoro.
///
/// E' una costante e non un valore calcolato all'avvio perche' cosi' non costa nulla e
/// non puo' fallire. Corrisponde a una password che nessuno conosce, con i parametri di
/// [`AuthConfig::default`]; un test verifica che sia una stringa PHC valida.
///
/// I parametri qui dentro sono fissi, mentre quelli delle password vere vengono dalla
/// [`AuthConfig`] passata a [`register`]: i due percorsi hanno lo stesso costo solo
/// finche' il server registra gli utenti con i parametri predefiniti. Chi cambia la
/// configurazione in produzione deve rigenerare anche questa costante, altrimenti la
/// differenza di tempo torna misurabile. Nei test la discrepanza c'e' ed e' voluta,
/// perche' li' i parametri sono abbassati apposta.
const HASH_FITTIZIO: &str = "$argon2id$v=19$m=19456,t=2,p=1$+VRtQ67z7l20ciN8AoheKQ$b6yROYVyGAc2PwIkF0UFTBTtyFdx2c2Rm94YEkCj3vU";

/// Errore restituito dalle operazioni di autenticazione.
#[derive(Debug, Error)]
pub enum AuthError {
    /// La password non rispetta la policy; il messaggio elenca cosa manca.
    #[error("password troppo debole: {0}")]
    WeakPassword(String),
    /// Username o password non validi.
    ///
    /// Il messaggio e' volutamente unico per i due casi: distinguerli direbbe a chi
    /// tenta l'accesso quali username sono registrati.
    #[error("credenziali non valide")]
    InvalidCredentials,
    /// Lo username e' gia' assegnato a un altro utente.
    #[error("username gia' registrato")]
    UsernameTaken,
    /// Guasto di una primitiva crittografica o dell'attivita' che la esegue.
    ///
    /// Copre i parametri di Argon2 non validi, un hash illeggibile, il generatore di
    /// casualita' non disponibile e l'interruzione del task di `spawn_blocking`: sono
    /// tutti guasti interni, che non dipendono da cosa ha scritto l'utente.
    #[error("errore interno di autenticazione: {0}")]
    Hashing(String),
    /// Errore proveniente dalla persistenza.
    #[error("errore di persistenza: {0}")]
    Storage(#[from] StorageError),
}

/// Parametri di costo di Argon2id.
///
/// Sono configurabili e non fissi nel codice per due motivi: i test possono abbassarli
/// e restare veloci, e in produzione si possono alzare senza toccare il codice man mano
/// che l'hardware migliora.
///
/// Alzarli rende piu' caro l'attacco a dizionario su un archivio rubato, ma ogni login
/// e ogni registrazione costano di piu' in tempo di CPU, che e' proprio la grandezza
/// riportata nel log periodico richiesto da R15: se quel valore cresce oltre le attese,
/// questi tre numeri sono il primo posto in cui guardare.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AuthConfig {
    /// Memoria occupata dal calcolo, in KiB (`m`).
    pub memory_kib: u32,
    /// Numero di passate sulla memoria (`t`).
    pub iterations: u32,
    /// Corsie calcolate in parallelo (`p`).
    pub parallelism: u32,
}

impl Default for AuthConfig {
    /// Parametri consigliati da OWASP per Argon2id: 19 MiB, 2 passate, 1 corsia.
    fn default() -> Self {
        Self {
            memory_kib: 19_456,
            iterations: 2,
            parallelism: 1,
        }
    }
}

impl AuthConfig {
    /// Traduce la configurazione nei parametri della crate `argon2`.
    ///
    /// # Errori
    ///
    /// [`AuthError::Hashing`] se la combinazione non e' ammessa, per esempio con una
    /// memoria minore di `8 * parallelism`.
    fn parametri(&self) -> Result<Params, AuthError> {
        Params::new(self.memory_kib, self.iterations, self.parallelism, None).map_err(|errore| {
            AuthError::Hashing(format!("parametri di Argon2 non validi: {errore}"))
        })
    }
}

/// Traduce un errore della crate `argon2` in [`AuthError::Hashing`].
fn errore_di_hashing(errore: argon2::password_hash::Error) -> AuthError {
    AuthError::Hashing(errore.to_string())
}

/// Traduce l'interruzione di un task di `spawn_blocking` in [`AuthError::Hashing`].
///
/// Si verifica solo se la chiusura va in panico o se il runtime si sta spegnendo:
/// nessuno dei due dipende dai dati dell'utente.
fn errore_di_task(errore: task::JoinError) -> AuthError {
    AuthError::Hashing(format!("attivita' in background non completata: {errore}"))
}

/// Riempie `destinazione` con byte presi dal generatore casuale del sistema operativo.
///
/// `SysRng` e' il nome che `rand` 0.10 da' all'`OsRng` delle versioni precedenti: legge
/// dalla stessa sorgente del sistema (`getrandom`) ed e' adatto all'uso crittografico.
/// Si usa la variante fallibile `try_fill_bytes` e non `fill_bytes`, che andrebbe in
/// panico se il sistema non avesse entropia disponibile.
///
/// # Errori
///
/// [`AuthError::Hashing`] se il generatore del sistema non e' disponibile.
fn byte_casuali(destinazione: &mut [u8]) -> Result<(), AuthError> {
    SysRng.try_fill_bytes(destinazione).map_err(|errore| {
        AuthError::Hashing(format!(
            "generatore di casualita' non disponibile: {errore}"
        ))
    })
}

/// Calcola l'hash Argon2id di una password, con salt casuale.
///
/// Il risultato e' una stringa nel formato PHC, per esempio
/// `$argon2id$v=19$m=19456,t=2,p=1$<salt>$<hash>`: contiene tutto cio' che serve a
/// verificarla, quindi salt e parametri non vanno conservati a parte.
///
/// # Errori
///
/// [`AuthError::Hashing`] se i parametri non sono validi o se il generatore casuale del
/// sistema non e' disponibile.
pub fn hash_password(password: &str, config: &AuthConfig) -> Result<String, AuthError> {
    let mut salt_grezzo = [0_u8; SALT_BYTES];
    byte_casuali(&mut salt_grezzo)?;
    let salt = SaltString::encode_b64(&salt_grezzo).map_err(errore_di_hashing)?;

    let argon2 = Argon2::new(Algorithm::Argon2id, Version::V0x13, config.parametri()?);

    Ok(argon2
        .hash_password(password.as_bytes(), &salt)
        .map_err(errore_di_hashing)?
        .to_string())
}

/// Verifica una password contro un hash in formato PHC.
///
/// Il confronto lo fa la crate `argon2`, che usa `subtle` per un'uguaglianza a tempo
/// costante: paragonare le due stringhe con `==` uscirebbe al primo byte diverso e il
/// tempo di risposta rivelerebbe quanti byte iniziali sono stati indovinati.
///
/// I parametri di costo non vengono da [`AuthConfig`] ma dalla stringa `hash` stessa,
/// quindi restano validi anche gli hash calcolati con una configurazione precedente.
///
/// # Errori
///
/// [`AuthError::Hashing`] se `hash` non e' una stringa PHC leggibile. Una password
/// semplicemente sbagliata non e' un errore: vale `Ok(false)`.
pub fn verify_password(hash: &str, password: &str) -> Result<bool, AuthError> {
    let analizzato = PasswordHash::new(hash).map_err(errore_di_hashing)?;

    match Argon2::default().verify_password(password.as_bytes(), &analizzato) {
        Ok(()) => Ok(true),
        Err(argon2::password_hash::Error::Password) => Ok(false),
        Err(errore) => Err(errore_di_hashing(errore)),
    }
}

/// Verifica che la password rispetti la policy: almeno 8 caratteri, una cifra e una
/// lettera.
///
/// # Errori
///
/// [`AuthError::WeakPassword`] con l'elenco di tutti i requisiti non rispettati, non
/// solo del primo: chi sta scegliendo una password corregge tutto in una volta invece
/// di scoprire un requisito alla volta. Il messaggio non riporta mai la password.
pub fn valida_password(password: &str) -> Result<(), AuthError> {
    let lunghezza = password.chars().count();
    let mut mancanze = Vec::new();

    if lunghezza < PASSWORD_MIN_LEN {
        mancanze.push(format!(
            "almeno {PASSWORD_MIN_LEN} caratteri (ne ha {lunghezza})"
        ));
    }
    if !password.chars().any(char::is_numeric) {
        mancanze.push("almeno una cifra".to_owned());
    }
    if !password.chars().any(char::is_alphabetic) {
        mancanze.push("almeno una lettera".to_owned());
    }

    if mancanze.is_empty() {
        Ok(())
    } else {
        Err(AuthError::WeakPassword(format!(
            "deve avere {}",
            mancanze.join(", ")
        )))
    }
}

/// Token che identifica una sessione aperta.
///
/// E' un newtype e non un alias di `String` perche' il compilatore possa distinguerlo
/// da uno username, da un hash o da un qualunque altro testo che gira nel server:
/// scambiarli sarebbe un errore silenzioso.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SessionToken(String);

impl SessionToken {
    /// Genera un token nuovo da [`TOKEN_BYTES`] byte casuali.
    ///
    /// I byte sono resi in esadecimale minuscolo, quindi il token e' lungo il doppio
    /// (64 caratteri) ed e' fatto di soli caratteri stampabili: puo' viaggiare in un
    /// messaggio di testo senza codifiche aggiuntive.
    ///
    /// # Errori
    ///
    /// [`AuthError::Hashing`] se il generatore casuale del sistema non e' disponibile.
    pub fn generate() -> Result<Self, AuthError> {
        let mut byte = [0_u8; TOKEN_BYTES];
        byte_casuali(&mut byte)?;

        let esadecimale = byte.iter().fold(
            String::with_capacity(TOKEN_BYTES * 2),
            |mut accumulatore, valore| {
                // `write!` su una `String` non puo' fallire: l'implementazione di
                // `fmt::Write` per `String` restituisce sempre `Ok`. Si ignora l'esito
                // invece di usare l'`expect` che il progetto vieta.
                let _ = write!(accumulatore, "{valore:02x}");
                accumulatore
            },
        );

        Ok(Self(esadecimale))
    }

    /// Ricostruisce un token ricevuto dalla rete, verificandone la forma.
    ///
    /// Accetta soltanto cio' che [`SessionToken::generate`] produce: esattamente 64
    /// caratteri esadecimali minuscoli. Senza questo controllo il token sarebbe un
    /// campo di testo libero scelto dal client, e la mappa delle sessioni finirebbe per
    /// contenere chiavi arbitrarie, anche molto grandi.
    ///
    /// # Errori
    ///
    /// [`AuthError::InvalidCredentials`] per qualunque stringa di forma diversa. E' lo
    /// stesso errore che riceve chi presenta un token sconosciuto: distinguere
    /// "malformato" da "non riconosciuto" non serve a un client corretto e direbbe a
    /// chi prova quali forme il server considera plausibili.
    pub fn parse(testo: &str) -> Result<Self, AuthError> {
        // Il confronto sulla lunghezza in byte viene per primo e cortocircuita, quindi
        // un input enorme costa un confronto invece di una scansione. Su una stringa di
        // soli byte ASCII lunghezza in byte e in caratteri coincidono, e il controllo
        // successivo garantisce che siano tutti ASCII: un input multibyte lungo 64 byte
        // viene rifiutato dal secondo controllo, non accettato per errore dal primo.
        let forma_valida = testo.len() == TOKEN_BYTES * 2
            && testo
                .bytes()
                .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'));

        if forma_valida {
            Ok(Self(testo.to_owned()))
        } else {
            Err(AuthError::InvalidCredentials)
        }
    }

    /// Il token come stringa esadecimale di 64 caratteri.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Registro delle sessioni aperte, condiviso fra i task del server.
///
/// Usa il [`RwLock`] di tokio e non quello della libreria standard perche' i metodi
/// sono `async` e il guard puo' attraversare un punto di sospensione senza bloccare il
/// thread dell'esecutore. Qui in realta' nessun metodo sospende mentre tiene il lock:
/// ogni sezione critica e' una singola operazione sulla mappa.
///
/// Le letture sono molte piu' delle scritture (ogni messaggio in arrivo risolve un
/// token, mentre insert e remove avvengono a inizio e fine sessione): un `RwLock` le
/// lascia procedere in parallelo, mentre un `Mutex` le serializzerebbe.
#[derive(Debug, Default)]
pub struct SessionStore {
    /// Token attivi e utente a cui appartengono.
    sessioni: RwLock<HashMap<SessionToken, UserId>>,
}

impl SessionStore {
    /// Costruisce un registro vuoto.
    pub fn new() -> Self {
        Self::default()
    }

    /// Registra un token come sessione attiva dell'utente indicato.
    ///
    /// Se il token era gia' presente, la voce viene sostituita.
    pub async fn insert(&self, token: SessionToken, user_id: UserId) {
        self.sessioni.write().await.insert(token, user_id);
    }

    /// Utente a cui appartiene il token, se la sessione e' ancora attiva.
    pub async fn resolve(&self, token: &SessionToken) -> Option<UserId> {
        self.sessioni.read().await.get(token).copied()
    }

    /// Chiude la sessione identificata dal token.
    ///
    /// Su un token sconosciuto non fa nulla e non e' un errore.
    pub async fn remove(&self, token: &SessionToken) {
        self.sessioni.write().await.remove(token);
    }

    /// Chiude tutte le sessioni di un utente.
    ///
    /// Serve quando una seconda connessione dello stesso veicolo deve invalidare la
    /// precedente: le sessioni degli altri utenti restano intatte.
    pub async fn remove_by_user(&self, user_id: UserId) {
        self.sessioni.write().await.retain(|_, id| *id != user_id);
    }
}

/// Registra un nuovo utente e ne restituisce l'id assegnato.
///
/// La password viene validata prima di qualunque altra cosa, cosi' una password debole
/// non fa pagare ne' l'hashing ne' un accesso al database.
///
/// # Errori
///
/// - [`AuthError::WeakPassword`] se la password non rispetta la policy;
/// - [`AuthError::UsernameTaken`] se lo username e' gia' registrato: l'errore del
///   database non viene propagato cosi' com'e', perche' il suo messaggio parla di
///   vincoli e tabelle, dettagli che non riguardano chi si registra;
/// - [`AuthError::Hashing`] se il calcolo dell'hash fallisce;
/// - [`AuthError::Storage`] per ogni altro guasto della persistenza.
pub async fn register(
    repo: &Arc<dyn Repository>,
    config: &AuthConfig,
    username: &Username,
    password: &str,
) -> Result<UserId, AuthError> {
    valida_password(password)?;

    // I dati vengono duplicati perche' `spawn_blocking` sposta la chiusura su un altro
    // thread e richiede quindi che non contenga prestiti.
    let password_posseduta = password.to_owned();
    let config_posseduta = *config;
    let hash = task::spawn_blocking(move || hash_password(&password_posseduta, &config_posseduta))
        .await
        .map_err(errore_di_task)??;

    let repo_posseduto = Arc::clone(repo);
    let username_posseduto = username.clone();
    let esito =
        task::spawn_blocking(move || repo_posseduto.create_user(&username_posseduto, &hash))
            .await
            .map_err(errore_di_task)?;

    match esito {
        Ok(id) => Ok(id),
        Err(StorageError::UsernameTaken) => Err(AuthError::UsernameTaken),
        Err(errore) => Err(AuthError::Storage(errore)),
    }
}

/// Verifica le credenziali di un utente e ne restituisce l'id.
///
/// Se lo username non esiste, la password viene comunque verificata contro
/// [`HASH_FITTIZIO`]: senza quel confronto il caso "utente inesistente" tornerebbe
/// molto prima di "password sbagliata", e la differenza di tempo direbbe a chi prova
/// quali username sono registrati.
///
/// # Errori
///
/// - [`AuthError::InvalidCredentials`] se lo username non esiste **o** la password e'
///   sbagliata: i due casi sono indistinguibili, per messaggio e per tempo di risposta;
/// - [`AuthError::Hashing`] se la verifica non e' eseguibile;
/// - [`AuthError::Storage`] se la lettura dalla persistenza fallisce.
pub async fn login(
    repo: &Arc<dyn Repository>,
    username: &Username,
    password: &str,
) -> Result<UserId, AuthError> {
    let repo_posseduto = Arc::clone(repo);
    let username_posseduto = username.clone();
    let utente =
        task::spawn_blocking(move || repo_posseduto.find_user_by_username(&username_posseduto))
            .await
            .map_err(errore_di_task)??;

    let hash = match &utente {
        Some(trovato) => trovato.password_hash.clone(),
        None => HASH_FITTIZIO.to_owned(),
    };

    let password_posseduta = password.to_owned();
    let corretta = task::spawn_blocking(move || verify_password(&hash, &password_posseduta))
        .await
        .map_err(errore_di_task)??;

    match utente {
        Some(trovato) if corretta => Ok(trovato.id),
        _ => Err(AuthError::InvalidCredentials),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::HashSet;

    use chrono::DateTime;
    use georuggine_common::clock::FakeClock;

    use crate::storage::InMemoryRepository;

    /// Parametri di Argon2 ridotti al minimo utile.
    ///
    /// In produzione l'hashing e' lento apposta: usare i valori predefiniti qui
    /// significherebbe pagare decine di millisecondi per ogni `register` e ogni `login`
    /// dei test, senza verificare nulla in piu'. La correttezza non dipende dal costo.
    fn config_di_test() -> AuthConfig {
        AuthConfig {
            memory_kib: 32,
            iterations: 1,
            parallelism: 1,
        }
    }

    /// Repository vuoto, dietro il puntatore condiviso che [`register`] e [`login`]
    /// si aspettano.
    fn repository() -> Arc<dyn Repository> {
        let istante = DateTime::from_timestamp(1_704_067_200, 0).expect("istante valido");
        Arc::new(InMemoryRepository::new(Arc::new(FakeClock::new(istante))))
    }

    /// Costruisce uno username che il test da' per valido.
    fn utente(nome: &str) -> Username {
        Username::new(nome).expect("username di test valido")
    }

    // --- hashing e verifica ---

    #[test]
    fn una_password_corretta_verifica_a_vero() {
        let hash = hash_password("scorta12", &config_di_test()).expect("hashing riuscito");
        assert!(verify_password(&hash, "scorta12").expect("verifica riuscita"));
    }

    #[test]
    fn una_password_sbagliata_verifica_a_falso_senza_errore() {
        let hash = hash_password("scorta12", &config_di_test()).expect("hashing riuscito");
        let esito = verify_password(&hash, "scorta13");
        assert!(!esito.expect("una password sbagliata non e' un errore"));
    }

    #[test]
    fn due_hash_della_stessa_password_sono_diversi() {
        let config = config_di_test();
        let primo = hash_password("scorta12", &config).expect("hashing riuscito");
        let secondo = hash_password("scorta12", &config).expect("hashing riuscito");

        assert_ne!(
            primo, secondo,
            "il salt casuale deve rendere diversi due hash della stessa password"
        );
        // Entrambi restano validi: il salt viaggia dentro la stringa PHC.
        assert!(verify_password(&primo, "scorta12").expect("verifica riuscita"));
        assert!(verify_password(&secondo, "scorta12").expect("verifica riuscita"));
    }

    #[test]
    fn l_hash_e_nel_formato_phc_argon2id() {
        let hash = hash_password("scorta12", &config_di_test()).expect("hashing riuscito");
        assert!(
            hash.starts_with("$argon2id$"),
            "atteso formato PHC di Argon2id, ottenuto {hash}"
        );
    }

    // --- policy sulla password ---

    #[test]
    fn una_password_valida_e_accettata() {
        assert!(valida_password("scorta12").is_ok());
    }

    #[test]
    fn una_password_troppo_corta_e_rifiutata() {
        let esito = valida_password("scort1");
        let messaggio = match esito {
            Err(AuthError::WeakPassword(messaggio)) => messaggio,
            altro => panic!("atteso WeakPassword, ottenuto {altro:?}"),
        };
        assert!(
            messaggio.contains("caratteri"),
            "il messaggio deve dire che manca la lunghezza: {messaggio}"
        );
    }

    #[test]
    fn una_password_senza_cifre_e_rifiutata() {
        let esito = valida_password("scortasenzanumeri");
        let messaggio = match esito {
            Err(AuthError::WeakPassword(messaggio)) => messaggio,
            altro => panic!("atteso WeakPassword, ottenuto {altro:?}"),
        };
        assert!(
            messaggio.contains("cifra"),
            "il messaggio deve dire che manca una cifra: {messaggio}"
        );
    }

    #[test]
    fn una_password_senza_lettere_e_rifiutata() {
        let esito = valida_password("12345678");
        let messaggio = match esito {
            Err(AuthError::WeakPassword(messaggio)) => messaggio,
            altro => panic!("atteso WeakPassword, ottenuto {altro:?}"),
        };
        assert!(
            messaggio.contains("lettera"),
            "il messaggio deve dire che manca una lettera: {messaggio}"
        );
    }

    #[test]
    fn il_messaggio_elenca_tutte_le_carenze() {
        let esito = valida_password("abc");
        let messaggio = match esito {
            Err(AuthError::WeakPassword(messaggio)) => messaggio,
            altro => panic!("atteso WeakPassword, ottenuto {altro:?}"),
        };
        assert!(messaggio.contains("caratteri"), "{messaggio}");
        assert!(messaggio.contains("cifra"), "{messaggio}");
    }

    // --- registrazione ---

    #[tokio::test]
    async fn register_assegna_un_id_e_permette_il_login() {
        let repo = repository();
        let config = config_di_test();
        let nome = utente("veicolo_01");

        let id = register(&repo, &config, &nome, "scorta12")
            .await
            .expect("registrazione riuscita");
        let entrato = login(&repo, &nome, "scorta12")
            .await
            .expect("accesso riuscito");

        assert_eq!(
            entrato, id,
            "il login deve tornare l'id della registrazione"
        );
    }

    #[tokio::test]
    async fn register_con_password_debole_non_tocca_il_repository() {
        let repo = repository();
        let nome = utente("veicolo_01");

        let esito = register(&repo, &config_di_test(), &nome, "corta").await;
        assert!(
            matches!(esito, Err(AuthError::WeakPassword(_))),
            "atteso WeakPassword, ottenuto {esito:?}"
        );
        assert!(
            repo.list_users().expect("elenco riuscito").is_empty(),
            "una password rifiutata non deve creare l'utente"
        );
    }

    #[tokio::test]
    async fn register_con_username_gia_esistente_da_username_taken() {
        let repo = repository();
        let config = config_di_test();
        let nome = utente("veicolo_01");

        register(&repo, &config, &nome, "scorta12")
            .await
            .expect("prima registrazione riuscita");
        let esito = register(&repo, &config, &nome, "altra123").await;

        assert!(
            matches!(esito, Err(AuthError::UsernameTaken)),
            "atteso UsernameTaken, ottenuto {esito:?}"
        );
    }

    // --- accesso ---

    #[tokio::test]
    async fn login_non_distingue_utente_assente_da_password_errata() {
        let repo = repository();
        let config = config_di_test();
        let nome = utente("veicolo_01");
        register(&repo, &config, &nome, "scorta12")
            .await
            .expect("registrazione riuscita");

        let inesistente = login(&repo, &utente("mai_visto"), "scorta12").await;
        let sbagliata = login(&repo, &nome, "scorta99").await;

        let messaggio_inesistente = match inesistente {
            Err(AuthError::InvalidCredentials) => AuthError::InvalidCredentials.to_string(),
            altro => panic!("atteso InvalidCredentials per l'utente assente: {altro:?}"),
        };
        let messaggio_sbagliata = match sbagliata {
            Err(AuthError::InvalidCredentials) => AuthError::InvalidCredentials.to_string(),
            altro => panic!("atteso InvalidCredentials per la password errata: {altro:?}"),
        };

        assert_eq!(
            messaggio_inesistente, messaggio_sbagliata,
            "i due casi devono essere indistinguibili da fuori"
        );
    }

    #[test]
    fn l_hash_fittizio_e_valido_e_non_verifica_nessuna_password() {
        // Se la costante non fosse una stringa PHC valida, `login` su un utente
        // inesistente restituirebbe un errore di hashing invece di InvalidCredentials,
        // e il caso sarebbe di nuovo distinguibile.
        assert!(
            !verify_password(HASH_FITTIZIO, "qualunque1").expect("la costante deve essere valida")
        );
    }

    // --- token di sessione ---

    #[test]
    fn un_token_e_esadecimale_di_64_caratteri() {
        let token = SessionToken::generate().expect("generazione riuscita");
        assert_eq!(token.as_str().len(), 64);
        assert!(
            token.as_str().chars().all(|c| c.is_ascii_hexdigit()),
            "atteso solo esadecimale, ottenuto {}",
            token.as_str()
        );
    }

    #[test]
    fn mille_token_consecutivi_sono_tutti_distinti() {
        let mut visti = HashSet::new();
        for _ in 0..1_000 {
            let token = SessionToken::generate().expect("generazione riuscita");
            assert!(visti.insert(token), "token ripetuto");
        }
        assert_eq!(visti.len(), 1_000);
    }

    #[test]
    fn parse_accetta_cio_che_generate_produce() {
        let originale = SessionToken::generate().expect("generazione riuscita");
        let riletto = SessionToken::parse(originale.as_str()).expect("un token generato e' valido");

        assert_eq!(riletto, originale);
    }

    #[test]
    fn parse_rifiuta_ogni_forma_non_valida() {
        let generato = SessionToken::generate().expect("generazione riuscita");
        let valido = generato.as_str();

        let casi = [
            ("stringa vuota", String::new()),
            ("un carattere in meno", valido[..63].to_owned()),
            ("un carattere in piu'", format!("{valido}0")),
            ("maiuscole", valido.to_uppercase()),
            ("carattere non esadecimale", format!("{}z", &valido[..63])),
            (
                "spazio in mezzo",
                format!("{} {}", &valido[..31], &valido[32..]),
            ),
            ("input molto lungo", "a".repeat(100_000)),
            // 32 caratteri accentati fanno 64 byte ma non 64 caratteri esadecimali:
            // il controllo non deve confondere la lunghezza in byte con quella in
            // caratteri, ne' andare in panico su un confine multibyte.
            ("caratteri multibyte", "\u{00e8}".repeat(32)),
        ];

        for (descrizione, testo) in casi {
            let esito = SessionToken::parse(&testo);
            assert!(
                matches!(esito, Err(AuthError::InvalidCredentials)),
                "{descrizione}: atteso InvalidCredentials, ottenuto {esito:?}"
            );
        }
    }

    // --- registro delle sessioni ---

    #[tokio::test]
    async fn insert_seguito_da_resolve_trova_l_utente() {
        let sessioni = SessionStore::new();
        let token = SessionToken::generate().expect("generazione riuscita");

        sessioni.insert(token.clone(), UserId(7)).await;

        assert_eq!(sessioni.resolve(&token).await, Some(UserId(7)));
    }

    #[tokio::test]
    async fn resolve_di_un_token_sconosciuto_e_none() {
        let sessioni = SessionStore::new();
        let token = SessionToken::generate().expect("generazione riuscita");

        assert_eq!(sessioni.resolve(&token).await, None);
    }

    #[tokio::test]
    async fn remove_cancella_la_sessione() {
        let sessioni = SessionStore::new();
        let token = SessionToken::generate().expect("generazione riuscita");
        sessioni.insert(token.clone(), UserId(7)).await;

        sessioni.remove(&token).await;

        assert_eq!(sessioni.resolve(&token).await, None);
    }

    #[tokio::test]
    async fn remove_by_user_cancella_solo_le_sessioni_di_quell_utente() {
        let sessioni = SessionStore::new();
        let primo = SessionToken::generate().expect("generazione riuscita");
        let secondo = SessionToken::generate().expect("generazione riuscita");
        let altrui = SessionToken::generate().expect("generazione riuscita");

        // Due connessioni dello stesso utente, piu' una di un altro.
        sessioni.insert(primo.clone(), UserId(7)).await;
        sessioni.insert(secondo.clone(), UserId(7)).await;
        sessioni.insert(altrui.clone(), UserId(9)).await;

        sessioni.remove_by_user(UserId(7)).await;

        assert_eq!(sessioni.resolve(&primo).await, None);
        assert_eq!(sessioni.resolve(&secondo).await, None);
        assert_eq!(
            sessioni.resolve(&altrui).await,
            Some(UserId(9)),
            "le sessioni degli altri utenti devono restare"
        );
    }
}
