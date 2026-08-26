//! TUI del client Georuggine: interfaccia a terminale costruita con `ratatui` e
//! `crossterm`.
//!
//! # Confine con la rete
//!
//! Questo modulo non importa `tokio::net` ne' i codec di `common`: comunica con il task di
//! rete solo attraverso [`ClientHandle`](crate::core::ClientHandle), mandando
//! [`ClientCommand`](crate::core::ClientCommand) e leggendo
//! [`ClientEvent`](crate::core::ClientEvent). Lo stato mostrato vive in [`UiState`],
//! aggiornato da [`UiState::applica_evento`]: separarlo dal disegno (in [`render`]) e' cio'
//! che lo rende verificabile senza aprire un terminale, come [`analizza_input`] lo e' senza
//! leggere una tastiera vera.
//!
//! # Ciclo di rendering
//!
//! [`ciclo_principale`] ridisegna a circa 20 fotogrammi al secondo: ogni giro attende fino
//! a [`INTERVALLO_POLL`] un tasto, invece di un poll piu' frequente che brucerebbe CPU
//! senza motivo.
//!
//! `crossterm::event::poll` e `read` sono pero' sincroni e bloccanti: chiamarli qui
//! fermerebbe per 50 ms alla volta un thread dell'esecutore, contro l'invariante che vieta
//! le operazioni bloccanti in un task async, e su un esecutore a un solo thread sarebbe il
//! task di rete a rimanerne affamato. Passano percio' da `spawn_blocking`, in
//! [`prossimo_tasto`].
//!
//! Gli eventi di rete non condividono un `select!` con quell'attesa: si drena `event_rx`
//! con `try_recv` a ogni giro di rendering, che non blocca mai e, con un giro ogni 50 ms,
//! ha la stessa latenza percepita.

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

/// Avvia la TUI: inizializza il terminale, ci gira dentro finche' l'operatore non chiede di
/// uscire (o la connessione termina in modo non recuperabile), poi lo ripristina.
///
/// `stato` arriva dal chiamante invece di essere costruito qui, cosi' il modulo resta
/// ignaro di `crate::core::ClientConfig`.
///
/// `ratatui::try_init` mette il terminale in modalita' raw, entra nello schermo alternativo
/// e installa un hook di panico che ripristina entrambi: un crash non lascia il terminale
/// rotto. Il ripristino avviene anche all'uscita normale, perche' e' eseguito qui qualunque
/// sia l'esito del ciclo.
///
/// L'handle arriva per riferimento: chiuderlo con `ClientHandle::chiudi` e' responsabilita'
/// di chi lo ha avviato, e consumarlo qui lo lascerebbe cadere senza attenderlo.
///
/// # Errori
///
/// L'errore di I/O se l'inizializzazione del terminale, il disegno di un fotogramma o la
/// lettura di un evento di tastiera falliscono.
pub async fn esegui(handle: &mut ClientHandle, mut stato: UiState) -> io::Result<()> {
    let mut terminal = ratatui::try_init()?;

    let esito = ciclo_principale(&mut terminal, handle, &mut stato).await;

    ratatui::restore();
    esito
}

/// Corpo del ciclo di rendering: disegna, legge la tastiera, drena gli eventi di rete,
/// finche' l'operatore non chiede di uscire o il canale degli eventi non si chiude.
async fn ciclo_principale(
    terminal: &mut Terminal<CrosstermBackend<Stdout>>,
    handle: &mut ClientHandle,
    stato: &mut UiState,
) -> io::Result<()> {
    loop {
        terminal.draw(|frame| render::disegna(frame, stato))?;

        if let Some(tasto) = prossimo_tasto().await? {
            // Su Windows crossterm segnala anche il rilascio di un tasto: senza questo
            // filtro ogni battuta conterebbe due volte.
            if tasto.kind == KeyEventKind::Press
                && !gestisci_tasto(tasto, stato, &handle.command_tx).await
            {
                return Ok(());
            }
        }

        loop {
            match handle.event_rx.try_recv() {
                Ok(evento) => stato.applica_evento(evento, Utc::now()),
                Err(TryRecvError::Empty) => break,
                // Il task di rete e' terminato: non arriveranno altri eventi, quindi anche
                // la TUI chiude.
                Err(TryRecvError::Disconnected) => return Ok(()),
            }
        }
    }
}

/// Attende fino a [`INTERVALLO_POLL`] un tasto premuto, senza bloccare l'esecutore.
///
/// Restituisce `None` se l'attesa scade senza eventi o se l'evento non e' di tastiera (un
/// ridimensionamento della finestra, per esempio): in entrambi i casi al chiamante basta
/// ridisegnare e riprovare.
///
/// # Errori
///
/// L'errore di I/O della lettura dal terminale, o il fallimento del task bloccante, che si
/// verifica solo se quest'ultimo va in panico.
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

/// Applica un tasto premuto allo stato dell'input, mandando un comando quando Invio
/// conferma una riga valida.
///
/// Ogni carattere digitato si accoda a [`UiState::riga_di_input`] e Backspace toglie
/// l'ultimo, cosi' la riga e' correggibile; gli altri tasti speciali non hanno un effetto
/// definito in questa TUI e vengono ignorati.
///
/// Restituisce falso solo quando l'operatore ha chiesto di uscire, con `/quit` o con
/// Ctrl+C: il chiamante deve allora interrompere il ciclo di rendering.
async fn gestisci_tasto(
    tasto: KeyEvent,
    stato: &mut UiState,
    command_tx: &mpsc::Sender<ClientCommand>,
) -> bool {
    // In modalita' raw il terminale non traduce piu' Ctrl+C in un segnale di interruzione:
    // la combinazione arriva qui come un tasto qualunque. Senza questo caso scriverebbe una
    // `c` nella riga di input, e l'operatore che prova la via consueta per uscire
    // troverebbe un'interfaccia che sembra bloccata.
    if tasto.modifiers.contains(KeyModifiers::CONTROL) {
        return match tasto.code {
            KeyCode::Char('c') => {
                let _ = command_tx.send(ClientCommand::Quit).await;
                false
            }
            // Le altre combinazioni con Ctrl vanno ignorate, non scritte nella riga.
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
                // Una riga vuota non e' un errore da mostrare.
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
