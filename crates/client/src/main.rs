//! Punto di ingresso del client Georuggine.
//!
//! Non contiene logica propria: analizza gli argomenti, costruisce e valida la sorgente
//! di posizione, avvia il nucleo di rete e cede il controllo alla TUI. La logica vera
//! vive nei moduli di `georuggine_client`, gia' coperti dai loro test.
//!
//! Non va mai in panico: ogni guasto diventa un messaggio sullo standard error e un
//! codice di uscita diverso da zero.

use std::process::ExitCode;

use georuggine_client::core::{Avvio, ClientConfig, ClientHandle, USO};
use georuggine_client::tui::{self, UiState};

#[tokio::main]
async fn main() -> ExitCode {
    let config = match ClientConfig::from_args(std::env::args().skip(1)) {
        Ok(Avvio::Configurato(config)) => config,
        Ok(Avvio::Uso) => {
            print!("{USO}");
            return ExitCode::SUCCESS;
        }
        Err(errore) => {
            // Un errore d'uso va sullo standard error e non nel log: chi ha sbagliato a
            // scrivere il comando lo sta leggendo dal terminale.
            eprintln!("{errore}\n\n{USO}");
            return ExitCode::FAILURE;
        }
    };

    // La sorgente si costruisce qui, prima di aprire qualunque connessione: senza una
    // sorgente di posizioni funzionante non ha senso collegarsi, e un percorso su file
    // mancante o illeggibile deve fermare l'avvio finche' c'e' ancora qualcuno che legge
    // l'errore, non trasformarsi in un veicolo che non si muove. La lettura del file e'
    // bloccante ma siamo in fase di avvio, senza altri task in corso da affamare: e' la
    // stessa scelta che il server fa aprendo il database.
    let sorgente = match config.source.build() {
        Ok(sorgente) => sorgente,
        Err(errore) => {
            eprintln!("sorgente di posizione non inizializzata: {errore}");
            return ExitCode::FAILURE;
        }
    };

    // `name` restituisce un `&'static str`: leggerlo adesso non trattiene in prestito la
    // sorgente, che passa subito al task di rete.
    let stato = UiState::new(config.username.clone(), sorgente.sorgente.name());
    let mut handle = ClientHandle::spawn(config, sorgente);

    let esito = tui::esegui(&mut handle, stato).await;

    // La TUI e' finita, per `/quit` dell'operatore o per un errore irrecuperabile del
    // terminale: in entrambi i casi il task di rete va chiuso e atteso, non abbandonato
    // alla distruzione del runtime.
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
