//! Registrazione e accesso degli utenti.
//!
//! Le password sono salvate come hash Argon2id in formato PHC, che contiene anche salt e
//! parametri. L'hashing e gli accessi al [`Repository`] girano in `spawn_blocking`.

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

/// Hash Argon2id usato da [`login`] quando lo username non esiste, cosi' i due casi di
/// fallimento costano lo stesso tempo e non rivelano quali utenti sono registrati.
///
/// Va rigenerato se cambiano i parametri di [`AuthConfig::default`].
const HASH_FITTIZIO: &str = "$argon2id$v=19$m=19456,t=2,p=1$+VRtQ67z7l20ciN8AoheKQ$b6yROYVyGAc2PwIkF0UFTBTtyFdx2c2Rm94YEkCj3vU";

/// Errore restituito dalle operazioni di autenticazione.
#[derive(Debug, Error)]
pub enum AuthError {
    /// La password non rispetta la policy; il messaggio elenca cosa manca.
    #[error("password troppo debole: {0}")]
    WeakPassword(String),
    /// Username o password non validi, con un messaggio unico per i due casi.
    #[error("credenziali non valide")]
    InvalidCredentials,
    /// Lo username e' gia' assegnato a un altro utente.
    #[error("username gia' registrato")]
    UsernameTaken,
    /// Errore di una primitiva crittografica o del task che la esegue.
    #[error("errore interno di autenticazione: {0}")]
    Hashing(String),
    /// Errore proveniente dalla persistenza.
    #[error("errore di persistenza: {0}")]
    Storage(#[from] StorageError),
}

/// Parametri di costo di Argon2id; i test li abbassano per restare veloci.
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
    /// [`AuthError::Hashing`] se la combinazione non e' ammessa.
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

/// Riempie `destinazione` con byte casuali del sistema operativo, senza andare in panico
/// se l'entropia non e' disponibile.
///
/// # Errori
///
/// [`AuthError::Hashing`] se il generatore non e' disponibile.
fn byte_casuali(destinazione: &mut [u8]) -> Result<(), AuthError> {
    SysRng.try_fill_bytes(destinazione).map_err(|errore| {
        AuthError::Hashing(format!(
            "generatore di casualita' non disponibile: {errore}"
        ))
    })
}

/// Calcola l'hash Argon2id di una password con salt casuale, in formato PHC.
///
/// # Errori
///
/// [`AuthError::Hashing`] se i parametri non sono validi o manca il generatore casuale.
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

/// Verifica una password contro un hash PHC, a tempo costante e con i parametri
/// contenuti nell'hash.
///
/// # Errori
///
/// [`AuthError::Hashing`] se l'hash non e' leggibile; una password sbagliata da'
/// `Ok(false)`.
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
/// [`AuthError::WeakPassword`] con tutti i requisiti mancanti, senza riportare la password.
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
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SessionToken(String);

impl SessionToken {
    /// Genera un token di `TOKEN_BYTES` byte casuali, in esadecimale.
    ///
    /// # Errori
    ///
    /// [`AuthError::Hashing`] se il generatore casuale non e' disponibile.
    pub fn generate() -> Result<Self, AuthError> {
        let mut byte = [0_u8; TOKEN_BYTES];
        byte_casuali(&mut byte)?;

        let esadecimale = byte.iter().fold(
            String::with_capacity(TOKEN_BYTES * 2),
            |mut accumulatore, valore| {
                // `write!` su una `String` non fallisce.
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

/// Registra un nuovo utente e ne restituisce l'id.
///
/// # Errori
///
/// [`AuthError::WeakPassword`], [`AuthError::UsernameTaken`], oppure un errore di hashing
/// o di persistenza.
pub async fn register(
    repo: &Arc<dyn Repository>,
    config: &AuthConfig,
    username: &Username,
    password: &str,
) -> Result<UserId, AuthError> {
    valida_password(password)?;

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

/// Verifica le credenziali e restituisce l'id dell'utente.
///
/// Se lo username non esiste la password si verifica comunque contro `HASH_FITTIZIO`.
///
/// # Errori
///
/// [`AuthError::InvalidCredentials`] per utente inesistente o password errata, oppure un
/// errore di hashing o di persistenza.
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

    /// Parametri di Argon2 minimi, per test veloci.
    fn config_di_test() -> AuthConfig {
        AuthConfig {
            memory_kib: 32,
            iterations: 1,
            parallelism: 1,
        }
    }

    /// Repository vuoto in memoria.
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
        // Se la costante non fosse un PHC valido, `login` su un utente inesistente
        // darebbe un errore diverso da InvalidCredentials.
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
