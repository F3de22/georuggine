//! Punto di ingresso del server Georuggine.
//!
//! Mette insieme i pezzi costruiti nei moduli della libreria e non contiene logica
//! propria: analizza gli argomenti, apre la persistenza, installa la gestione del
//! segnale di interruzione e cede il controllo a [`net::run`].
//!
//! Non va mai in panico: ogni guasto diventa un messaggio nel log e un codice di uscita
//! diverso da zero.

use std::io::{self, BufRead};
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use georuggine_common::clock::{Clock, SystemClock};
use georuggine_server::auth::SessionStore;
use georuggine_server::console::{self, ConsoleContext, ErroreComando};
use georuggine_server::cpu_log::CpuLogger;
use georuggine_server::messaging::MessageHub;
use georuggine_server::net::{
    self, Avvio, ConnectionRegistry, Dipendenze, Osservazione, ServerConfig,
};
use georuggine_server::state::StateTracker;
use georuggine_server::storage::{Repository, SqliteRepository};
use tokio::sync::{broadcast, mpsc};
use tokio::time::timeout;

/// Numero di segnali di spegnimento che il canale conserva.
///
/// Ne basta uno: il segnale si manda una volta sola e chi si iscrive dopo trova la
/// connessione gia' in chiusura per altre vie.
const CAPACITA_SPEGNIMENTO: usize = 1;

/// Tempo fra due righe del log del tempo di CPU.
///
/// Due minuti sono il valore chiesto dal requisito R15.
const INTERVALLO_LOG_CPU: Duration = Duration::from_secs(120);

/// Attesa massima concessa al log del tempo di CPU per scrivere la riga finale.
///
/// E' la stessa scadenza che il livello di rete concede alla chiusura ordinata di una
/// connessione: qualche secondo basta a una scrittura su file, e oltre non si aspetta.
const ATTESA_CHIUSURA_LOG_CPU: Duration = Duration::from_secs(5);

/// Righe di stdin che possono attendere in coda prima che il lettore si fermi.
///
/// Una persona che digita non riempie mai la coda; il margine serve a un file rediretto
/// sullo standard input, che arriva tutto insieme.
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

    // Questo ricevitore esiste **prima** che la console possa mandare alcunche', ed e'
    // cio' che rende impossibile perdere il suo segnale: un canale `broadcast` recapita
    // solo a chi e' gia' iscritto, e [`net::run`] si iscrive per conto proprio soltanto
    // dopo aver aperto l'ascolto. Un `/esci` che arrivasse prima di quel momento, cosa
    // che capita davvero quando lo standard input e' un file che comincia con quel
    // comando, non troverebbe nessuno e lascerebbe il server in attesa per sempre.
    let mut richiesta_di_spegnimento = spegnimento.subscribe();

    // La console amministrativa riceve gli stessi esemplari condivisi che vanno al
    // livello di rete: un registro o una messaggistica diversi non vedrebbero nessuno
    // dei veicoli collegati.
    avvia_console(ConsoleContext {
        repo: Arc::clone(&repo),
        registry: Arc::clone(&registry),
        messaging: Arc::clone(&messaging),
        clock: Arc::clone(&orologio),
        shutdown: spegnimento.clone(),
    });

    // Il log del tempo di CPU parte prima di `net::run` per la stessa ragione della
    // console: si iscrive al canale di spegnimento adesso, mentre `run` lo fa solo dopo
    // aver aperto l'ascolto, e un segnale che arrivasse nel mezzo lascerebbe il task in
    // attesa per sempre. Ha anche un effetto utile: la prima riga del log e' datata
    // all'avvio del processo, non al momento in cui la porta e' pronta.
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
                sessions,
                registry,
                messaging,
                clock: orologio,
            },
            osservazione,
            spegnimento,
        ) => esito?,
        // Di norma e' `run` a fermarsi da se' e questo ramo non serve; conta nel caso in
        // cui il segnale sia partito prima che `run` si iscrivesse, perche' li' nessun
        // altro lo sentirebbe. Quando entrambi lo ricevono, `select!` sceglie a caso, e
        // le due uscite si equivalgono: `run` allo spegnimento smette solo di accettare,
        // e le connessioni gia' aperte hanno un proprio ricevitore con cui chiudersi.
        // Anche l'errore del ricevitore (mittente caduto, segnali saltati) e' un motivo
        // per smettere di aspettare.
        _ = richiesta_di_spegnimento.recv() => {
            tracing::info!("spegnimento richiesto: il server termina");
        }
    }

    // Si arriva qui solo dopo che il segnale di spegnimento e' partito, quindi il logger
    // lo ha gia' ricevuto e sta scrivendo la sua riga finale. L'attesa non e' un dettaglio
    // di cortesia: al ritorno di `main` il runtime viene distrutto e i task ancora vivi
    // spariscono, quindi senza questa attesa l'ultima riga del log andrebbe persa in una
    // corsa. Il tetto vale per il caso opposto, un disco che non risponde: si preferisce
    // perdere una riga piuttosto che lasciare il processo appeso allo spegnimento.
    if timeout(ATTESA_CHIUSURA_LOG_CPU, log_cpu).await.is_err() {
        tracing::warn!("log del tempo di CPU non chiuso entro la scadenza");
    }

    Ok(())
}

/// Avvia il lettore dello standard input e il ciclo della console amministrativa.
///
/// Sono due pezzi separati perche' vivono in mondi diversi. La lettura sta su un thread
/// di sistema e non su `tokio::task::spawn_blocking`: una lettura da stdin non si puo'
/// interrompere dall'esterno, e un'attivita' bloccante ancora in corso farebbe **attendere
/// lo spegnimento del runtime**, che al termine di `main` aspetta le attivita' gia'
/// partite. Il thread e' invece scollegato: se resta fermo su una riga che non arriva
/// mai, nessuno lo aspetta e il processo termina lo stesso. Percio' `/esci` ferma il
/// resto del server per la sua strada, cioe' il segnale sul canale di spegnimento, e non
/// dipende dal fatto che il lettore si accorga di qualcosa.
///
/// Il prezzo e' che il lettore non si ferma su richiesta: smette a EOF, oppure quando la
/// console chiude la propria coda e l'invio della riga successiva fallisce, cioe' con una
/// riga di ritardo.
fn avvia_console(contesto: ConsoleContext) {
    let (mittente, righe) = mpsc::channel(CAPACITA_CONSOLE);

    std::thread::spawn(move || leggi_stdin(&mittente));
    tokio::spawn(cicla_console(contesto, righe));
}

/// Manda alla console ogni riga letta dallo standard input, fino a EOF.
///
/// Gira su un thread suo e usa quindi `blocking_send`, che sospende il thread invece di
/// restituire un futuro: non c'e' nessun runtime da bloccare, perche' il contesto di
/// tokio non attraversa il confine di un thread di sistema.
fn leggi_stdin(righe: &mpsc::Sender<String>) {
    let stdin = io::stdin();

    loop {
        let mut riga = String::new();
        match stdin.lock().read_line(&mut riga) {
            // Zero byte letti sono la fine dello standard input, non una riga vuota.
            Ok(0) => break,
            Ok(_) => {
                if righe.blocking_send(riga).is_err() {
                    // La console ha chiuso la coda: non c'e' piu' nessuno da servire.
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

/// Esegue i comandi che arrivano dallo standard input e ne stampa l'esito.
///
/// Termina quando lo standard input finisce, cioe' quando il lettore lascia cadere il
/// proprio capo della coda, oppure quando un comando dichiara concluso il ciclo, cosa che
/// fa il solo `/esci`.
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
            // Una riga vuota non e' un errore dell'operatore: non c'e' niente da eseguire
            // e niente da dire.
            Err(ErroreComando::RigaVuota) => {}
            Err(errore) => println!("{errore}"),
        }
    }

    tracing::info!("console amministrativa terminata");
}
