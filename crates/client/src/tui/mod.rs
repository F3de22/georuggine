//! TUI del client Georuggine (requisito P20): interfaccia a terminale costruita con
//! `ratatui` e `crossterm`.
//!
//! # Confine con la rete
//!
//! Questo modulo non importa `tokio::net`, i codec di `common`, ne' altro che parli
//! direttamente con la rete: comunica con il task di rete esclusivamente attraverso
//! [`ClientHandle`](crate::core::ClientHandle), mandando
//! [`ClientCommand`](crate::core::ClientCommand) su `command_tx` e leggendo
//! [`ClientEvent`](crate::core::ClientEvent) da `event_rx`. Lo stato mostrato vive
//! interamente in [`UiState`], aggiornato da [`UiState::applica_evento`]: separarlo dal
//! disegno (in [`render`]) e' cio' che lo rende verificabile senza aprire un terminale,
//! come [`analizza_input`] lo e' senza leggere una tastiera vera.
//!
//! # Ciclo di rendering ed eventi di rete
//!
//! [`ciclo_principale`] ridisegna a circa 20 fps: ogni giro attende fino a
//! [`INTERVALLO_POLL`] (50 ms) un tasto, invece di un poll piu' frequente che
//! brucerebbe CPU senza motivo (la CPU del client entra nelle misure di prestazioni del
//! progetto).
//!
//! [`crossterm::event::poll`] e [`crossterm::event::read`] sono pero' sincroni e
//! bloccanti: chiamarli qui bloccherebbe per 50 ms alla volta un thread dell'esecutore,
//! contro l'invariante del progetto che vieta le operazioni bloccanti in un task async.
//! Su un esecutore con un solo thread di lavoro sarebbe il task di rete a rimanerne
//! affamato. Passano percio' da [`tokio::task::spawn_blocking`], in
//! [`prossimo_tasto`], che li sposta sul pool dei blocchi lasciando libero l'esecutore.
//!
//! Gli eventi di rete non condividono comunque un `tokio::select!` con quell'attesa: si
//! drena `event_rx` con `try_recv` in un ciclo interno a ogni giro di rendering, che non
//! blocca mai e, con un giro ogni 50 ms, ha la stessa latenza percepita di un `select!`.

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

/// Avvia la TUI: inizializza il terminale, ci gira dentro finche' l'operatore non chiede
/// di uscire (o la connessione termina in modo non recuperabile), poi lo ripristina.
///
/// `stato` e' lo stato iniziale, tipicamente [`UiState::new`] con lo username e la
/// sorgente scelti dalla configurazione del client: separarlo dalla costruzione di
/// questa funzione tiene il modulo `tui` ignaro di `crate::core::ClientConfig`.
///
/// L'inizializzazione ([`ratatui::try_init`]) mette il terminale in modalita' raw,
/// entra nello schermo alternativo e installa un hook di panico che ripristina
/// entrambi prima di propagare il panico originale: un crash non lascia percio' il
/// terminale dell'operatore rotto. Il ripristino ([`ratatui::restore`]) avviene anche
/// all'uscita normale, non solo in caso di panico, perche' e' eseguito qui dopo
/// [`ciclo_principale`] indipendentemente dal suo esito.
///
/// L'handle arriva per riferimento e non per valore: chiuderlo e' responsabilita' di chi
/// lo ha avviato, che al ritorno di questa funzione deve chiamare
/// [`ClientHandle::chiudi`](crate::core::ClientHandle::chiudi) per far terminare il task
/// di rete in modo ordinato. Consumarlo qui lo lascerebbe invece cadere senza attenderlo.
///
/// # Errori
///
/// L'errore di I/O se l'inizializzazione del terminale, il disegno di un fotogramma o
/// la lettura di un evento di tastiera falliscono.
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
                // Il task di rete e' terminato (dopo `Quit` o un fallimento non
                // ripetibile dell'autenticazione): non arriveranno altri eventi, quindi
                // anche la TUI chiude.
                Err(TryRecvError::Disconnected) => return Ok(()),
            }
        }
    }
}

/// Attende fino a [`INTERVALLO_POLL`] un tasto premuto, senza bloccare l'esecutore.
///
/// L'attesa e la lettura sono sincrone e vivono percio' su
/// [`tokio::task::spawn_blocking`] (vedi la documentazione del modulo). Restituisce
/// `None` se l'attesa scade senza eventi o se l'evento non e' di tastiera (un
/// ridimensionamento della finestra, per esempio): in entrambi i casi al chiamante basta
/// ridisegnare e riprovare.
///
/// # Errori
///
/// L'errore di I/O della lettura dal terminale, o il fallimento del task bloccante, che
/// si verifica solo se quest'ultimo va in panico.
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
/// Ogni carattere digitato si accoda a [`UiState::riga_di_input`]; Backspace toglie
/// l'ultimo carattere, cosi' l'operatore puo' correggere un errore di battitura senza
/// dover premere Invio e riscrivere la riga da capo (un'estensione minima e ovvia
/// rispetto al solo "accoda", altrimenti la riga di input non sarebbe correggibile).
/// Gli altri tasti speciali (frecce, Tab, ...) non hanno un effetto definito in questa
/// TUI e vengono ignorati.
///
/// Restituisce falso solo quando l'operatore ha chiesto di uscire, con `/quit` o con
/// Ctrl+C: il chiamante deve allora interrompere il ciclo di rendering.
async fn gestisci_tasto(
    tasto: KeyEvent,
    stato: &mut UiState,
    command_tx: &mpsc::Sender<ClientCommand>,
) -> bool {
    // In modalita' raw il terminale non traduce piu' Ctrl+C in un segnale di
    // interruzione: la combinazione arriva qui come un tasto qualunque. Senza questo
    // caso Ctrl+C scriverebbe una `c` nella riga di input, e l'operatore che prova la
    // via consueta per uscire troverebbe un'interfaccia che sembra bloccata.
    if tasto.modifiers.contains(KeyModifiers::CONTROL) {
        return match tasto.code {
            KeyCode::Char('c') => {
                let _ = command_tx.send(ClientCommand::Quit).await;
                false
            }
            // Le altre combinazioni con Ctrl non hanno un effetto definito in questa
            // TUI: vanno ignorate, non scritte nella riga di input.
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
                // Non e' un errore da mostrare: vedi la documentazione del modulo
                // `input`.
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
