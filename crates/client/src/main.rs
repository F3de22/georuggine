// Punto di ingresso del client Georuggine.
// Non contiene logica propria: analizza gli argomenti, costruisce e valida la sorgente di
// posizione, avvia la connessione di rete e cede il controllo alla TUI.
// Non va mai in panico: ogni guasto diventa un messaggio sullo standard error e un codice
// di uscita diverso da zero.

use std::process::ExitCode;
use std::sync::Arc;

use georuggine_client::core::{Avvio, ClientConfig, ClientHandle, Tempi, USO};
use georuggine_client::tui::{self, UiState};
use georuggine_common::clock::SystemClock;

#[tokio::main]
async fn main() -> ExitCode {
    let config = match ClientConfig::from_args(std::env::args().skip(1)) {
        Ok(Avvio::Configurato(config)) => config,
        Ok(Avvio::Uso) => {
            print!("{USO}");
            return ExitCode::SUCCESS;
        }
        Err(errore) => {
            // Un errore d'uso va sullo standard error e non nel log, quindi viene visto sul terminale
            eprintln!("{errore}\n\n{USO}");
            return ExitCode::FAILURE;
        }
    };

    // La sorgente si costruisce prima di aprire qualunque connessione: un percorso su file
    // mancante o illeggibile deve fermare l'avvio finche' c'e' ancora qualcuno che legge
    // l'errore, non trasformarsi in un veicolo che non si muove, quindi prima della creazione della TUI
    let sorgente = match config.source.build() {
        Ok(sorgente) => sorgente,
        Err(errore) => {
            eprintln!("sorgente di posizione non inizializzata: {errore}");
            return ExitCode::FAILURE;
        }
    };

    let stato = UiState::new(config.username.clone(), sorgente.sorgente.name());
    let mut handle = ClientHandle::spawn(config, sorgente, Arc::new(SystemClock), Tempi::default());

    let esito = tui::esegui(&mut handle, stato).await;

    // La TUI e' finita, per `/quit` dell'operatore o per un errore irrecuperabile del
    // terminale: si chiude e si aspetta il task di rete, non si abbaondona alla distruzione del runtime.
    if !handle.chiudi().await {
        eprintln!("il task di rete non si e' chiuso entro l'attesa");
    }

    match esito {
        Ok(()) => ExitCode::SUCCESS,
        Err(errore) => {
            eprintln!("interfaccia terminata per un errore: {errore}");
            ExitCode::FAILURE
        }
    }
}
