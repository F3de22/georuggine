//! Punto di ingresso del client: legge gli argomenti, prepara la sorgente di posizione,
//! avvia il task di rete e la TUI.

use std::process::ExitCode;
use std::sync::Arc;

use georuggine_client::core::{Avvio, ClientConfig, ClientHandle, Tempi, USO};
use georuggine_client::tui::{self, Chiusura, UiState};
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
            // Errore d'uso: va sullo standard error, non nel log.
            eprintln!("{errore}\n\n{USO}");
            return ExitCode::FAILURE;
        }
    };

    // La sorgente si costruisce prima della TUI: un file mancante deve fermare l'avvio
    // con un errore leggibile sul terminale.
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

    // Si attende la chiusura del task di rete, cosi' il Logout parte davvero.
    if !handle.chiudi().await {
        eprintln!("il task di rete non si e' chiuso entro l'attesa");
    }

    match esito {
        Ok(Chiusura::Utente) => ExitCode::SUCCESS,
        // Il task di rete si e' fermato da solo (accesso rifiutato, sessione sostituita):
        // il motivo si stampa qui, a terminale ripristinato.
        Ok(Chiusura::Rete(motivo)) => {
            eprintln!(
                "client terminato: {}",
                motivo.as_deref().unwrap_or("connessione chiusa dal server")
            );
            ExitCode::FAILURE
        }
        Err(errore) => {
            eprintln!("interfaccia terminata per un errore: {errore}");
            ExitCode::FAILURE
        }
    }
}
