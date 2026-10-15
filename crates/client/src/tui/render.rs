//! Disegno dei quattro riquadri e della riga di input della TUI.
//!
//! Separato dal resto del modulo `tui` perche' e' l'unica parte che tocca
//! `ratatui::Frame`: [`UiState`] e [`super::input::analizza_input`] restano testabili
//! senza aprire un terminale, e questo file e' l'unico che non lo e'.
//!
//! # Colori e leggibilita'
//!
//! Verde e rosso restano leggibili sia su un terminale a sfondo scuro sia su uno a
//! sfondo chiaro, a differenza per esempio di un blu scuro o di un giallo tenue, che su
//! uno dei due sparisce quasi del tutto. Lo stato di connessione non si affida comunque
//! al solo colore: ogni riquadro che lo mostra affianca al colore un simbolo (● / ○) o
//! una parola ("Connesso" / "Disconnesso"), leggibile anche senza distinguere i colori.

use chrono::{DateTime, Utc};
use georuggine_common::protocol::MovementSummary;
use georuggine_common::types::{Coordinate, PositionRecord, UserState};
use ratatui::Frame;
use ratatui::layout::{Constraint, Layout};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, List, ListItem, Paragraph, Wrap};

use super::state::UiState;

/// Colore del simbolo e del testo di stato quando il client e' connesso.
const COLORE_CONNESSO: Color = Color::Green;

/// Colore del simbolo e del testo di stato quando il client non e' connesso.
const COLORE_DISCONNESSO: Color = Color::Red;

/// Disegna l'intera schermata: quattro riquadri piu' una riga di input in fondo.
///
/// Layout, dall'alto: a sinistra stato/username/coordinate correnti/ultimo invio/
/// sorgente, a destra le ultime posizioni inviate; in basso a sinistra il log dei
/// messaggi, a destra l'ultima analisi ricevuta. La riga di input occupa il fondo dello
/// schermo su tutta la larghezza.
pub(super) fn disegna(frame: &mut Frame, stato: &UiState) {
    let area = frame.area();

    let [corpo, riga_input] =
        Layout::vertical([Constraint::Min(0), Constraint::Length(3)]).areas(area);
    let [alto, basso] =
        Layout::vertical([Constraint::Percentage(50), Constraint::Percentage(50)]).areas(corpo);
    let [alto_sx, alto_dx] =
        Layout::horizontal([Constraint::Percentage(50), Constraint::Percentage(50)]).areas(alto);
    let [basso_sx, basso_dx] =
        Layout::horizontal([Constraint::Percentage(50), Constraint::Percentage(50)]).areas(basso);

    frame.render_widget(pannello_stato(stato), alto_sx);
    frame.render_widget(pannello_storico(stato), alto_dx);
    frame.render_widget(pannello_messaggi(stato), basso_sx);
    frame.render_widget(pannello_analisi(stato), basso_dx);
    frame.render_widget(pannello_input(stato), riga_input);
}

/// Riquadro in alto a sinistra: stato di connessione, username, movimento, sorgente,
/// ultima posizione inviata e, se presente, l'ultimo errore.
fn pannello_stato(stato: &UiState) -> Paragraph<'static> {
    let (simbolo, testo_stato, colore) = if stato.connesso {
        ("●", "Connesso", COLORE_CONNESSO)
    } else {
        ("○", "Disconnesso", COLORE_DISCONNESSO)
    };

    let mut righe = vec![
        Line::from(Span::styled(
            format!("{simbolo} {testo_stato}"),
            Style::new().fg(colore).add_modifier(Modifier::BOLD),
        )),
        Line::from(format!("Utente:       {}", stato.username.as_str())),
        Line::from(format!(
            "Movimento:    {}",
            testo_movimento(stato.stato_movimento)
        )),
        Line::from(format!("Sorgente:     {}", stato.sorgente)),
        Line::from(format!(
            "Ultimo invio: {}",
            testo_ultima_posizione(stato.ultima_posizione)
        )),
    ];

    if let Some(errore) = &stato.ultimo_errore {
        righe.push(Line::from(Span::styled(
            format!("! {errore}"),
            Style::new().fg(Color::Yellow),
        )));
    }

    Paragraph::new(righe)
        .block(Block::bordered().title(" Stato "))
        .wrap(Wrap { trim: true })
}

/// Testo dello stato di movimento, o un segnaposto se il server non lo ha ancora
/// notificato.
fn testo_movimento(stato: Option<UserState>) -> String {
    stato
        .map(|stato| stato.to_string())
        .unwrap_or_else(|| "sconosciuto".to_owned())
}

/// Testo dell'ultima posizione inviata, o un segnaposto se non ne e' ancora partita
/// nessuna.
fn testo_ultima_posizione(ultima: Option<(Coordinate, DateTime<Utc>)>) -> String {
    match ultima {
        Some((coordinate, quando)) => format!(
            "{:.5}, {:.5}  ({})",
            coordinate.lat(),
            coordinate.lon(),
            quando.format("%H:%M:%S")
        ),
        None => "nessuna".to_owned(),
    }
}

/// Riquadro in alto a destra: le ultime posizioni inviate, la piu' recente in cima, con
/// la distanza dalla precedente.
fn pannello_storico(stato: &UiState) -> List<'static> {
    let mut voci: Vec<ListItem> = stato
        .storico_posizioni
        .iter()
        .rev()
        .map(|(coordinate, quando, distanza)| {
            ListItem::new(format!(
                "{}  {:.5}, {:.5}  Δ{:.1} m",
                quando.format("%H:%M:%S"),
                coordinate.lat(),
                coordinate.lon(),
                distanza
            ))
        })
        .collect();

    if voci.is_empty() {
        voci.push(ListItem::new("nessuna posizione inviata"));
    }

    List::new(voci).block(Block::bordered().title(" Ultime posizioni "))
}

/// Riquadro in basso a sinistra: i messaggi ricevuti dal server, il piu' recente in
/// cima, con orario e se il messaggio era diretto o un broadcast.
fn pannello_messaggi(stato: &UiState) -> List<'static> {
    let mut voci: Vec<ListItem> = stato
        .messaggi
        .iter()
        .rev()
        .map(|(testo, quando, diretto)| {
            let tipo = if *diretto { "diretto" } else { "broadcast" };
            ListItem::new(Line::from(vec![
                Span::raw(format!("[{}] ", quando.format("%H:%M:%S"))),
                Span::styled(
                    format!("({tipo}) "),
                    Style::new().add_modifier(Modifier::ITALIC),
                ),
                Span::raw(testo.clone()),
            ]))
        })
        .collect();

    if voci.is_empty() {
        voci.push(ListItem::new("nessun messaggio ricevuto"));
    }

    List::new(voci).block(Block::bordered().title(" Messaggi "))
}

/// Riquadro in basso a destra: l'ultimo riepilogo dei movimenti ricevuto.
fn pannello_analisi(stato: &UiState) -> Paragraph<'static> {
    let righe = match &stato.ultima_analisi {
        Some(riepilogo) => righe_analisi(riepilogo),
        None => vec![Line::from(
            "Nessuna analisi disponibile: usa /analisi <today|week|month>.",
        )],
    };

    Paragraph::new(righe)
        .block(Block::bordered().title(" Ultima analisi "))
        .wrap(Wrap { trim: true })
}

/// Righe del riepilogo dei movimenti, gia' formattate per essere lette da una persona.
fn righe_analisi(riepilogo: &MovementSummary) -> Vec<Line<'static>> {
    let mut righe = vec![
        Line::from(format!(
            "Distanza:    {:.1} m",
            riepilogo.total_distance_meters
        )),
        Line::from(format!(
            "Velocita':   {:.1} km/h",
            riepilogo.average_speed_kmh
        )),
        Line::from(format!(
            "In moto:     {}",
            formatta_durata(riepilogo.moving_duration_secs)
        )),
        Line::from(format!(
            "In pausa:    {}",
            formatta_durata(riepilogo.pause_duration_secs)
        )),
        Line::from(format!("Soste:       {}", riepilogo.stop_count)),
    ];
    righe.extend(righe_tragitto(&riepilogo.route));
    righe
}

/// Il tragitto in poche righe: quanti punti, da dove e fino a dove.
///
/// Il riquadro non ha spazio per migliaia di punti: partenza e arrivo con i loro orari
/// dicono dove il veicolo e' andato, e il numero di punti quanto e' fitto il percorso.
fn righe_tragitto(route: &[PositionRecord]) -> Vec<Line<'static>> {
    let punto = |posizione: &PositionRecord| {
        format!(
            "{:.5}, {:.5}  ({})",
            posizione.coordinate.lat(),
            posizione.coordinate.lon(),
            posizione.recorded_at.format("%H:%M:%S")
        )
    };

    match route {
        [] => vec![Line::from("Tragitto:    nessun punto")],
        [solo] => vec![
            Line::from("Tragitto:    1 punto"),
            Line::from(format!("  in:        {}", punto(solo))),
        ],
        [primo, .., ultimo] => vec![
            Line::from(format!("Tragitto:    {} punti", route.len())),
            Line::from(format!("  da:        {}", punto(primo))),
            Line::from(format!("  a:         {}", punto(ultimo))),
        ],
    }
}

/// Durata in secondi come la legge una persona: `2h 05m 30s`, `5m 30s`, `30s`.
///
/// Duplica `formatta_durata` di `console.rs` nel server: e' privata li' e vive in un
/// crate diverso, quindi non c'e' modo di riusarla senza spostarla in `common` per una
/// singola funzione di formattazione testuale.
fn formatta_durata(secondi: u64) -> String {
    let ore = secondi / 3_600;
    let minuti = (secondi % 3_600) / 60;
    let resto = secondi % 60;

    if ore > 0 {
        format!("{ore}h {minuti:02}m {resto:02}s")
    } else if minuti > 0 {
        format!("{minuti}m {resto:02}s")
    } else {
        format!("{resto}s")
    }
}

/// Riga di input in fondo allo schermo.
fn pannello_input(stato: &UiState) -> Paragraph<'static> {
    Paragraph::new(format!("> {}", stato.riga_di_input)).block(
        Block::bordered().title(
            " Input: /msg <testo> · /pos <lat> <lon> · /analisi <today|week|month> · /quit ",
        ),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    use georuggine_common::types::UserId;

    fn punto(lat: f64, secondi: i64) -> PositionRecord {
        PositionRecord {
            user_id: UserId(1),
            coordinate: Coordinate::new(lat, 7.6869).expect("coordinata di test valida"),
            recorded_at: DateTime::from_timestamp(1_704_067_200 + secondi, 0)
                .expect("istante valido"),
        }
    }

    fn testo(righe: &[Line<'_>]) -> Vec<String> {
        righe.iter().map(ToString::to_string).collect()
    }

    #[test]
    fn il_tragitto_mostra_numero_di_punti_partenza_e_arrivo() {
        let righe = righe_tragitto(&[punto(45.0703, 0), punto(45.0712, 30), punto(45.0721, 60)]);

        assert_eq!(
            testo(&righe),
            vec![
                "Tragitto:    3 punti".to_owned(),
                "  da:        45.07030, 7.68690  (00:00:00)".to_owned(),
                "  a:         45.07210, 7.68690  (00:01:00)".to_owned(),
            ]
        );
    }

    #[test]
    fn un_tragitto_vuoto_o_di_un_punto_resta_leggibile() {
        assert_eq!(
            testo(&righe_tragitto(&[])),
            vec!["Tragitto:    nessun punto".to_owned()]
        );
        assert_eq!(
            testo(&righe_tragitto(&[punto(45.0703, 0)])),
            vec![
                "Tragitto:    1 punto".to_owned(),
                "  in:        45.07030, 7.68690  (00:00:00)".to_owned(),
            ]
        );
    }
}
