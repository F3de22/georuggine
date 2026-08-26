//! Registrazione e accesso degli utenti: hashing delle password e policy.
//!
//! Le password non vengono mai salvate in chiaro: si conserva l'hash Argon2id nel
//! formato PHC, una stringa autocontenuta che porta con se' algoritmo, versione,
//! parametri e salt. La verifica rilegge i parametri da li', quindi cambiare
//! [`AuthConfig`] non invalida gli hash gia' scritti.
//!
//! La password in chiaro non compare mai in un log ne' in un messaggio d'errore:
//! [`AuthError::WeakPassword`] descrive cosa manca senza citare cio' che l'utente ha
//! scritto.
//!
//! Argon2 e' lento per costruzione, ed e' li' che sta la sua sicurezza: [`register`] e
//! [`login`] lo eseguono dentro `tokio::task::spawn_blocking`, insieme alle chiamate al
//! [`Repository`], che sono sincrone.

use std::fmt::Write as _;
use std::sync::Arc;

use argon2::password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString};
use argon2::{Algorithm, Argon2, Params, Version};
use georuggine_common::types::{UserId, Username};
use rand::TryRng as _;
use rand::rngs::SysRng;
use thiserror::Error;
use tokio::task;

use crate::storage::{Repository, StorageError};

/// Numero minimo di caratteri di una password.
const PASSWORD_MIN_LEN: usize = 8;

/// Byte casuali del salt: la lunghezza consigliata dalla specifica PHC.
const SALT_BYTES: usize = 16;

/// Byte casuali di un token di sessione.
const TOKEN_BYTES: usize = 32;

/// Hash Argon2id usato da [`login`] quando lo username non esiste.
///
/// Rende indistinguibili i due modi di fallire un accesso: senza, il caso "utente
/// inesistente" tornerebbe subito mentre "password sbagliata" pagherebbe la verifica
/// Argon2, e la differenza di tempo direbbe a chi prova quali username esistono.
///
/// Corrisponde a una password che nessuno conosce, con i parametri di
/// [`AuthConfig::default`]: chi cambia quella configurazione in produzione deve
/// rigenerare anche questa costante, altrimenti la differenza di tempo torna misurabile.
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
    /// Guasto di una primitiva crittografica o dell'attivita' che la esegue: parametri
    /// non validi, hash illeggibile, generatore casuale assente, task interrotto.
    #[error("errore interno di autenticazione: {0}")]
    Hashing(String),
    /// Errore proveniente dalla persistenza.
    #[error("errore di persistenza: {0}")]
    Storage(#[from] StorageError),
}

/// Parametri di costo di Argon2id.
///
/// Sono configurabili perche' i test possano abbassarli restando veloci e perche' in
/// produzione si possano alzare senza toccare il codice. Alzarli rende piu' caro
/// l'attacco a dizionario su un archivio rubato, ma ogni accesso costa piu' tempo di
/// CPU, che e' proprio la grandezza riportata nel log periodico.
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
fn errore_di_task(errore: task::JoinError) -> AuthError {
    AuthError::Hashing(format!("attivita' in background non completata: {errore}"))
}

/// Riempie `destinazione` con byte del generatore casuale del sistema operativo.
///
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
/// Il risultato e' una stringa PHC come
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
/// Il confronto lo fa la crate `argon2` a tempo costante: con `==` uscirebbe al primo
/// byte diverso, e il tempo di risposta rivelerebbe quanti byte sono stati indovinati.
/// I parametri di costo vengono da `hash` e non da [`AuthConfig`], quindi restano validi
/// anche gli hash calcolati con una configurazione precedente.
///
/// # Errori
///
/// [`AuthError::Hashing`] se `hash` non e' una stringa PHC leggibile. Una password
/// sbagliata non e' un errore: vale `Ok(false)`.
pub fn verify_password(hash: &str, password: &str) -> Result<bool, AuthError> {
    let analizzato = PasswordHash::new(hash).map_err(errore_di_hashing)?;

    match Argon2::default().verify_password(password.as_bytes(), &analizzato) {
        Ok(()) => Ok(true),
        Err(argon2::password_hash::Error::Password) => Ok(false),
        Err(errore) => Err(errore_di_hashing(errore)),
    }
}

/// Verifica che la password abbia almeno 8 caratteri, una cifra e una lettera.
///
/// # Errori
///
/// [`AuthError::WeakPassword`] con l'elenco di **tutti** i requisiti non rispettati, non
/// solo del primo, cosi' chi sceglie la password corregge tutto in una volta. Il
/// messaggio non riporta mai la password.
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

/// Token che identifica una sessione aperta, consegnato al client dopo l'accesso.
///
/// E' un newtype e non un alias di `String` perche' il compilatore lo distingua da uno
/// username, da un hash o da un qualunque altro testo che gira nel server.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SessionToken(String);

impl SessionToken {
    /// Genera un token nuovo da [`TOKEN_BYTES`] byte casuali.
    ///
    /// I byte sono resi in esadecimale minuscolo, quindi il token e' lungo 64 caratteri
    /// tutti stampabili e puo' viaggiare in un messaggio di testo senza codifiche
    /// aggiuntive.
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
                // `write!` su una `String` non puo' fallire: si ignora l'esito invece di
                // usare l'`expect` che il progetto vieta.
                let _ = write!(accumulatore, "{valore:02x}");
                accumulatore
            },
        );

        Ok(Self(esadecimale))
    }

    /// Il token come stringa esadecimale di 64 caratteri.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Registra un nuovo utente e ne restituisce l'id assegnato.
///
/// La password viene validata per prima, cosi' una password debole non fa pagare ne'
/// l'hashing ne' un accesso al database.
///
/// # Errori
///
/// - [`AuthError::WeakPassword`] se la password non rispetta la policy;
/// - [`AuthError::UsernameTaken`] se lo username e' gia' registrato: l'errore del
///   database non viene propagato, perche' parla di vincoli e tabelle;
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
/// [`HASH_FITTIZIO`], cosi' i due percorsi fanno lo stesso lavoro.
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

    use crate::storage::SqliteRepository;

    /// Parametri di Argon2 ridotti al minimo utile: la correttezza non dipende dal
    /// costo, e quelli di produzione peserebbero decine di millisecondi per test.
    fn config_di_test() -> AuthConfig {
        AuthConfig {
            memory_kib: 32,
            iterations: 1,
            parallelism: 1,
        }
    }

    /// Repository vuoto in RAM, dietro il puntatore condiviso che `register` e `login`
    /// si aspettano.
    fn repository() -> Arc<dyn Repository> {
        let istante = DateTime::from_timestamp(1_704_067_200, 0).expect("istante valido");
        Arc::new(
            SqliteRepository::new(":memory:", Arc::new(FakeClock::new(istante)))
                .expect("apertura in memoria riuscita"),
        )
    }

    fn utente(nome: &str) -> Username {
        Username::new(nome).expect("username di test valido")
    }

    /// Estrae il messaggio di un [`AuthError::WeakPassword`] atteso.
    fn carenze(password: &str) -> String {
        match valida_password(password) {
            Err(AuthError::WeakPassword(messaggio)) => messaggio,
            altro => panic!("atteso WeakPassword per {password:?}, ottenuto {altro:?}"),
        }
    }

    #[test]
    fn l_hash_e_phc_verificabile_e_con_salt_diverso_a_ogni_calcolo() {
        let config = config_di_test();
        let primo = hash_password("scorta12", &config).expect("hashing riuscito");
        let secondo = hash_password("scorta12", &config).expect("hashing riuscito");

        assert!(
            primo.starts_with("$argon2id$"),
            "atteso formato PHC di Argon2id, ottenuto {primo}"
        );
        assert_ne!(primo, secondo, "il salt casuale deve renderli diversi");

        // Entrambi restano validi: il salt viaggia dentro la stringa PHC.
        assert!(verify_password(&primo, "scorta12").expect("verifica riuscita"));
        assert!(verify_password(&secondo, "scorta12").expect("verifica riuscita"));

        assert!(
            !verify_password(&primo, "scorta13").expect("una password sbagliata non e' un errore")
        );
    }

    #[test]
    fn la_policy_elenca_tutte_le_carenze_della_password() {
        assert!(valida_password("scorta12").is_ok());

        assert!(carenze("scort1").contains("caratteri"));
        assert!(carenze("scortasenzanumeri").contains("cifra"));
        assert!(carenze("12345678").contains("lettera"));

        let corta_e_senza_cifre = carenze("abc");
        assert!(
            corta_e_senza_cifre.contains("caratteri"),
            "{corta_e_senza_cifre}"
        );
        assert!(
            corta_e_senza_cifre.contains("cifra"),
            "{corta_e_senza_cifre}"
        );
    }

    #[test]
    fn l_hash_fittizio_e_valido_e_non_verifica_nessuna_password() {
        // Se la costante non fosse una stringa PHC valida, `login` su un utente
        // inesistente darebbe un errore di hashing invece di InvalidCredentials, e il
        // caso tornerebbe distinguibile.
        assert!(
            !verify_password(HASH_FITTIZIO, "qualunque1").expect("la costante deve essere valida")
        );
    }

    #[tokio::test]
    async fn register_assegna_un_id_e_permette_il_login() {
        let repo = repository();
        let nome = utente("veicolo_01");

        let id = register(&repo, &config_di_test(), &nome, "scorta12")
            .await
            .expect("registrazione riuscita");
        let entrato = login(&repo, &nome, "scorta12")
            .await
            .expect("accesso riuscito");

        assert_eq!(entrato, id);
    }

    #[tokio::test]
    async fn register_con_password_debole_non_tocca_il_repository() {
        let repo = repository();

        let esito = register(&repo, &config_di_test(), &utente("veicolo_01"), "corta").await;

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

    #[tokio::test]
    async fn login_non_distingue_utente_assente_da_password_errata() {
        let repo = repository();
        let nome = utente("veicolo_01");
        register(&repo, &config_di_test(), &nome, "scorta12")
            .await
            .expect("registrazione riuscita");

        for esito in [
            login(&repo, &utente("mai_visto"), "scorta12").await,
            login(&repo, &nome, "scorta99").await,
        ] {
            assert!(
                matches!(esito, Err(AuthError::InvalidCredentials)),
                "i due casi devono essere indistinguibili: {esito:?}"
            );
        }
    }

    #[test]
    fn i_token_sono_esadecimali_di_64_caratteri_e_tutti_distinti() {
        let mut visti = HashSet::new();

        for _ in 0..1_000 {
            let token = SessionToken::generate().expect("generazione riuscita");
            assert_eq!(token.as_str().len(), 64);
            assert!(
                token
                    .as_str()
                    .bytes()
                    .all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')),
                "atteso solo esadecimale minuscolo, ottenuto {}",
                token.as_str()
            );
            assert!(visti.insert(token), "token ripetuto");
        }
    }
}
