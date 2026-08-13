//! Punto di ingresso del server Georuggine.
//!
//! Mette insieme i pezzi costruiti nei moduli della libreria e non contiene logica
//! propria: analizza gli argomenti, apre la persistenza, installa la gestione del
//! segnale di interruzione e cede il controllo a [`net::run`].
//!
//! Non va mai in panico: ogni guasto diventa un messaggio nel log e un codice di uscita
//! diverso da zero.

use std::process::ExitCode;
use std::sync::Arc;

use georuggine_common::clock::{Clock, SystemClock};
use georuggine_server::auth::SessionStore;
use georuggine_server::messaging::MessageHub;
use georuggine_server::net::{
    self, Avvio, ConnectionRegistry, Dipendenze, Osservazione, ServerConfig,
};
use georuggine_server::state::StateTracker;
use georuggine_server::storage::{Repository, SqliteRepository};
use tokio::sync::broadcast;

/// Numero di segnali di spegnimento che il canale conserva.
///
/// Ne basta uno: il segnale si manda una volta sola e chi si iscrive dopo trova la
/// connessione gia' in chiusura per altre vie.
const CAPACITA_SPEGNIMENTO: usize = 1;

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt::init();

    let config = match ServerConfig::from_args(std::env::args().skip(1)) {
        Ok(Avvio::Configurato(config)) => config,
        Ok(Avvio::Uso) => {
            print!("{}", net::USO);
            return ExitCode::SUCCESS;
        }
        Err(errore) => {
            // Un errore d'uso va sullo standard error e non nel log: chi ha sbagliato a
            // scrivere il comando lo sta leggendo dal terminale.
            eprintln!("{errore}\n\n{}", net::USO);
            return ExitCode::FAILURE;
        }
    };

    match avvia(config).await {
        Ok(()) => {
            tracing::info!("server terminato");
            ExitCode::SUCCESS
        }
        Err(errore) => {
            tracing::error!(%errore, "il server si e' fermato per un errore");
            ExitCode::FAILURE
        }
    }
}

/// Apre le risorse condivise e resta in ascolto fino allo spegnimento.
///
/// # Errori
///
/// L'errore di apertura del database o quello di ascolto sull'indirizzo richiesto: sono
/// i due casi in cui il server non puo' partire.
async fn avvia(config: ServerConfig) -> anyhow::Result<()> {
    // Un solo orologio per tutto il processo: la persistenza e il livello di rete devono
    // datare gli stessi fatti allo stesso modo.
    let orologio: Arc<dyn Clock> = Arc::new(SystemClock);
    let repo: Arc<dyn Repository> = Arc::new(SqliteRepository::new(
        &config.db_path,
        Arc::clone(&orologio),
    )?);
    tracing::info!(database = %config.db_path.display(), "persistenza aperta");

    let sessions = Arc::new(SessionStore::new());
    let registry = Arc::new(ConnectionRegistry::new());
    // La messaggistica riceve lo stesso registro delle connessioni che usa il livello di
    // rete: un secondo registro non vedrebbe nessuno dei veicoli collegati.
    let messaging = Arc::new(MessageHub::new(
        Arc::clone(&repo),
        Arc::clone(&registry),
        Arc::clone(&orologio),
    ));
    // La macchina a stati legge lo stesso orologio della persistenza: le posizioni e le
    // scadenze dei tre minuti devono essere datate dalla stessa sorgente.
    let osservazione = Osservazione::con_stati(Arc::new(StateTracker::new(Arc::clone(&orologio))));
    let (spegnimento, _) = broadcast::channel(CAPACITA_SPEGNIMENTO);

    // L'attesa del segnale vive in un task suo: `run` deve restare libera di accettare
    // connessioni, e il segnale puo' arrivare in qualunque momento.
    let segnale = spegnimento.clone();
    tokio::spawn(async move {
        match tokio::signal::ctrl_c().await {
            Ok(()) => {
                tracing::info!("interruzione richiesta: spegnimento in corso");
                // L'errore qui significa solo che nessuno e' piu' in ascolto, cioe' che
                // il server sta gia' terminando.
                let _ = segnale.send(());
            }
            Err(errore) => {
                tracing::warn!(%errore, "gestione dell'interruzione non disponibile");
            }
        }
    });

    net::run(
        config,
        Dipendenze {
            repo,
            sessions,
            registry,
            messaging,
            clock: orologio,
        },
        osservazione,
        spegnimento,
    )
    .await?;

    Ok(())
}
