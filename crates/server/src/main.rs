//! Punto di ingresso del server: legge gli argomenti, apre la persistenza, avvia console,
//! log del tempo di CPU e livello di rete, e gestisce lo spegnimento ordinato.

use std::io::{self, BufRead};
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use georuggine_common::clock::{Clock, SystemClock};
use georuggine_server::console::{self, ConsoleContext, ErroreComando};
use georuggine_server::cpu_log::CpuLogger;
use georuggine_server::messaging::MessageHub;
use georuggine_server::net::{
    self, Avvio, ConnectionRegistry, Dipendenze, Regolazioni, ServerConfig,
};
use georuggine_server::state::StateTracker;
use georuggine_server::storage::{Repository, SqliteRepository};
use tokio::sync::{broadcast, mpsc};
use tokio::time::timeout;

/// Intervallo fra due righe del log del tempo di CPU.
const INTERVALLO_LOG_CPU: Duration = Duration::from_secs(120);

/// Attesa massima concessa al log del tempo di CPU per scrivere la riga finale.
const ATTESA_CHIUSURA_LOG_CPU: Duration = Duration::from_secs(5);

/// Righe di stdin in coda verso la console; il margine serve a un file rediretto.
const CAPACITA_CONSOLE: usize = 16;

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
            // Errore d'uso: va sullo standard error, non nel log.
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
/// Apertura del database o ascolto sull'indirizzo richiesto falliti.
async fn avvia(config: ServerConfig) -> anyhow::Result<()> {
    // Un solo orologio per tutto il processo.
    let orologio: Arc<dyn Clock> = Arc::new(SystemClock);
    let repo: Arc<dyn Repository> = Arc::new(SqliteRepository::new(
        &config.db_path,
        Arc::clone(&orologio),
    )?);
    tracing::info!(database = %config.db_path.display(), "persistenza aperta");

    let registry = Arc::new(ConnectionRegistry::new());
    // Stesso registro del livello di rete, altrimenti la messaggistica non vedrebbe i
    // veicoli collegati.
    let messaging = Arc::new(MessageHub::new(
        Arc::clone(&repo),
        Arc::clone(&registry),
        Arc::clone(&orologio),
    ));
    let stati = Arc::new(StateTracker::new(Arc::clone(&orologio)));
    let (spegnimento, _) = broadcast::channel(1);

    let segnale = spegnimento.clone();
    tokio::spawn(async move {
        match tokio::signal::ctrl_c().await {
            Ok(()) => {
                tracing::info!("interruzione richiesta: spegnimento in corso");
                // Un errore indica solo che il server sta gia' terminando.
                let _ = segnale.send(());
            }
            Err(errore) => {
                tracing::warn!(%errore, "gestione dell'interruzione non disponibile");
            }
        }
    });

    // Ci si iscrive allo spegnimento prima di avviare la console: un `/esci` immediato
    // (per esempio da stdin rediretto) arriverebbe altrimenti prima che `net::run` sia in
    // ascolto sul canale.
    let mut richiesta_di_spegnimento = spegnimento.subscribe();

    avvia_console(ConsoleContext {
        repo: Arc::clone(&repo),
        registry: Arc::clone(&registry),
        messaging: Arc::clone(&messaging),
        clock: Arc::clone(&orologio),
        stati: Arc::clone(&stati),
        shutdown: spegnimento.clone(),
    });

    // Avviato prima di `net::run` per lo stesso motivo; la prima riga e' datata all'avvio.
    let log_cpu = tokio::spawn(
        CpuLogger::new(
            config.log_dir.clone(),
            INTERVALLO_LOG_CPU,
            Arc::clone(&orologio),
        )
        .avvia(Arc::clone(&registry), spegnimento.subscribe()),
    );

    tokio::select! {
        esito = net::run(
            config,
            Dipendenze {
                repo,
                registry,
                messaging,
                clock: orologio,
                stati,
            },
            Regolazioni::default(),
            spegnimento,
        ) => esito?,
        // Serve se lo spegnimento arriva prima che `run` si sia iscritto al canale.
        _ = richiesta_di_spegnimento.recv() => {
            tracing::info!("spegnimento richiesto: il server termina");
        }
    }

    // Si attende la riga finale del log CPU: alla fine di `main` il runtime termina i task
    // ancora vivi. Il timeout evita di bloccarsi su un disco che non risponde.
    if timeout(ATTESA_CHIUSURA_LOG_CPU, log_cpu).await.is_err() {
        tracing::warn!("log del tempo di CPU non chiuso entro la scadenza");
    }

    Ok(())
}

/// Avvia il lettore dello standard input e il ciclo della console.
///
/// La lettura sta su un thread separato e non su `spawn_blocking`: una lettura da stdin
/// non si puo' interrompere, e un task bloccante ritarderebbe lo spegnimento del runtime.
fn avvia_console(contesto: ConsoleContext) {
    let (mittente, righe) = mpsc::channel(CAPACITA_CONSOLE);

    std::thread::spawn(move || leggi_stdin(&mittente));
    tokio::spawn(cicla_console(contesto, righe));
}

/// Inoltra alla console ogni riga dello standard input, fino a EOF.
fn leggi_stdin(righe: &mpsc::Sender<String>) {
    let stdin = io::stdin();

    loop {
        let mut riga = String::new();
        match stdin.lock().read_line(&mut riga) {
            // Zero byte: fine dello standard input.
            Ok(0) => break,
            Ok(_) => {
                if righe.blocking_send(riga).is_err() {
                    // La console ha chiuso la coda.
                    break;
                }
            }
            Err(errore) => {
                tracing::warn!(%errore, "lettura dallo standard input interrotta");
                break;
            }
        }
    }

    tracing::debug!("standard input esaurito: la console non ricevera' altri comandi");
}

/// Esegue i comandi della console fino a EOF o a `/esci`.
async fn cicla_console(contesto: ConsoleContext, mut righe: mpsc::Receiver<String>) {
    while let Some(riga) = righe.recv().await {
        match console::analizza_comando(&riga) {
            Ok(comando) => {
                let esito = console::esegui(comando, &contesto).await;
                println!("{}", esito.testo);
                if esito.termina {
                    break;
                }
            }
            Err(ErroreComando::RigaVuota) => {}
            Err(errore) => println!("{errore}"),
        }
    }

    tracing::info!("console amministrativa terminata");
}
