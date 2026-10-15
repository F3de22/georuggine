//! Interfaccia a terminale del client, con `ratatui` e `crossterm`.
//!
//! Comunica con il task di rete solo tramite [`ClientHandle`].
//! Lo stato mostrato sta in [`UiState`] ed e' separato dal disegno, quindi si testa senza
//! un terminale.
//!
//! Il ciclo ridisegna circa 20 volte al secondo. La lettura della tastiera e' bloccante e
//! passa da `spawn_blocking`; gli eventi di rete si leggono con `try_recv` a ogni giro.

mod input;
mod render;
mod state;

use std::io::{self, Stdout};
use std::time::Duration;

use chrono::Utc;
use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers};
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TryRecvError;

pub use input::{ComandoUtente, ErroreInput, analizza_input};
pub use state::UiState;

use crate::core::{ClientCommand, ClientHandle};

/// Intervallo fra un `poll` di tastiera e il successivo: circa 20 fotogrammi al secondo.
const INTERVALLO_POLL: Duration = Duration::from_millis(50);

/// Motivo della chiusura della TUI, da mostrare dopo aver ripristinato il terminale.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Chiusura {
    /// L'operatore ha chiesto di uscire, con `/quit` o con Ctrl+C.
    Utente,
    /// Il task di rete e' terminato da solo; contiene l'ultimo errore ricevuto.
    Rete(Option<String>),
}

/// Avvia la TUI e la esegue fino all'uscita, ripristinando sempre il terminale, anche in
/// caso di panico. Restituisce il motivo della chiusura.
///
/// # Errori
///
/// Errore di I/O del terminale.
pub async fn esegui(handle: &mut ClientHandle, mut stato: UiState) -> io::Result<Chiusura> {
    let mut terminal = ratatui::try_init()?;

    let esito = ciclo_principale(&mut terminal, handle, &mut stato).await;

    ratatui::restore();
    esito
}

/// Ciclo di rendering: disegna, legge la tastiera e applica gli eventi di rete.
async fn ciclo_principale(
    terminal: &mut Terminal<CrosstermBackend<Stdout>>,
    handle: &mut ClientHandle,
    stato: &mut UiState,
) -> io::Result<Chiusura> {
    loop {
        terminal.draw(|frame| render::disegna(frame, stato))?;

        if let Some(tasto) = prossimo_tasto().await? {
            // Su Windows arriva anche il rilascio del tasto: si considera solo la pressione.
            if tasto.kind == KeyEventKind::Press
                && !gestisci_tasto(tasto, stato, &handle.command_tx).await
            {
                return Ok(Chiusura::Utente);
            }
        }

        loop {
            match handle.event_rx.try_recv() {
                Ok(evento) => stato.applica_evento(evento, Utc::now()),
                Err(TryRecvError::Empty) => break,
                // Il task di rete e' terminato: la TUI si chiude con l'ultimo errore.
                Err(TryRecvError::Disconnected) => {
                    return Ok(Chiusura::Rete(stato.ultimo_errore.clone()));
                }
            }
        }
    }
}

/// Attende un tasto fino a [`INTERVALLO_POLL`] senza bloccare l'esecutore; `None` se
/// non arriva o se l'evento non e' di tastiera.
///
/// # Errori
///
/// Errore di lettura dal terminale.
async fn prossimo_tasto() -> io::Result<Option<KeyEvent>> {
    let letto = tokio::task::spawn_blocking(|| -> io::Result<Option<KeyEvent>> {
        if !event::poll(INTERVALLO_POLL)? {
            return Ok(None);
        }
        match event::read()? {
            Event::Key(tasto) => Ok(Some(tasto)),
            _ => Ok(None),
        }
    })
    .await;

    match letto {
        Ok(esito) => esito,
        Err(errore) => Err(io::Error::other(errore)),
    }
}

/// Applica un tasto alla riga di input e, con Invio, invia il comando corrispondente.
///
/// Restituisce `false` quando l'operatore chiede di uscire.
async fn gestisci_tasto(
    tasto: KeyEvent,
    stato: &mut UiState,
    command_tx: &mpsc::Sender<ClientCommand>,
) -> bool {
    // In modalita' raw Ctrl+C arriva come tasto e va gestito qui.
    if tasto.modifiers.contains(KeyModifiers::CONTROL) {
        return match tasto.code {
            KeyCode::Char('c') => {
                let _ = command_tx.send(ClientCommand::Quit).await;
                false
            }
            // Altre combinazioni con Ctrl: ignorate.
            _ => true,
        };
    }

    match tasto.code {
        KeyCode::Enter => {
            let riga = std::mem::take(&mut stato.riga_di_input);
            match analizza_input(&riga) {
                Ok(ComandoUtente::Messaggio(testo)) => {
                    let _ = command_tx.send(ClientCommand::SendText(testo)).await;
                }
                Ok(ComandoUtente::PosizioneManuale(coordinate)) => {
                    let _ = command_tx
                        .send(ClientCommand::SetManualPosition(coordinate))
                        .await;
                }
                Ok(ComandoUtente::Analisi(range)) => {
                    let _ = command_tx.send(ClientCommand::RequestAnalysis(range)).await;
                }
                Ok(ComandoUtente::Esci) => {
                    let _ = command_tx.send(ClientCommand::Quit).await;
                    return false;
                }
                Err(ErroreInput::NienteDaFare) => {}
                Err(errore) => stato.ultimo_errore = Some(errore.to_string()),
            }
        }
        KeyCode::Backspace => {
            stato.riga_di_input.pop();
        }
        KeyCode::Char(carattere) => stato.riga_di_input.push(carattere),
        _ => {}
    }
    true
}
