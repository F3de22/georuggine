//! Motore di analisi dei percorsi: requisiti R7, R8, R9 e R10.
//!
//! Il modulo prende una sequenza di [`PositionRecord`] e ne ricava un
//! [`MovementSummary`]: quanta strada ha fatto il veicolo, quanto tempo ha passato in
//! movimento e quanto fermo, quante soste ha fatto e a che velocita' media ha viaggiato.
//! La sorgente delle posizioni non e' affare di questo modulo: [`analyze`] e' una
//! funzione pura su una fetta di record, quindi la si prova senza database ne' rete.
//!
//! # Cos'e' movimento
//!
//! Il criterio e' lo stesso di [`state`], e per la stessa ragione: la sorgente simulata
//! sovrappone rumore alla posizione, quindi un veicolo fermo emette comunque letture che
//! differiscono nelle ultime cifre decimali. Un segmento conta come spostamento solo se
//! supera [`MOVEMENT_THRESHOLD_METERS`]; sotto soglia e' rumore, e non contribuisce ne'
//! alla distanza percorsa ne' al tempo in movimento. Da questo discende tutto il resto:
//! l'intervallo di tempo che accompagna un segmento sopra soglia e' tempo in movimento,
//! quello che accompagna un segmento sotto soglia e' tempo di pausa.
//!
//! [`state`]: crate::state
//!
//! # Pausa e sosta non sono la stessa cosa
//!
//! `pause_duration_secs` e' il tempo totale passato fermi, qualunque sia la durata delle
//! singole fermate; `stop_count` conta invece solo le pause che durano almeno
//! [`STILL_AFTER_SECS`], cioe' quelle abbastanza lunghe da essere una sosta secondo la
//! definizione di fermo del requisito R5. Una fermata di due minuti al semaforo entra nel
//! primo valore e non nel secondo.

use chrono::{
    DateTime, Datelike, Days, Local, MappedLocalTime, NaiveDate, NaiveTime, TimeZone, Utc,
};
use georuggine_common::protocol::{AnalysisRange, MovementSummary};
use georuggine_common::types::{
    MOVEMENT_THRESHOLD_METERS, PositionRecord, STILL_AFTER_SECS, haversine_meters,
};

/// Metri al secondo espressi in chilometri all'ora.
const SECONDI_PER_ORA_SU_METRI_PER_KM: f64 = 3.6;

/// Minuti in un giorno, limite della ricerca del primo istante valido di una giornata.
const MINUTI_IN_UN_GIORNO: u32 = 24 * 60;

/// Passo in minuti della stessa ricerca.
const PASSO_RICERCA_MINUTI: usize = 15;

/// Riepiloga i movimenti descritti da una sequenza di posizioni.
///
/// # Precondizione
///
/// `positions` deve essere gia' ordinato per `recorded_at` crescente. Non e' compito di
/// questa funzione ordinarlo: le posizioni arrivano da una query che le restituisce gia'
/// in ordine, e ordinare una copia a ogni chiamata costerebbe un `Vec` in piu' e un
/// `O(n log n)` per una garanzia che il chiamante ha gia'. La precondizione non e' pero'
/// una premessa di correttezza della memoria: se viene violata, gli intervalli
/// all'indietro contano zero secondi (vedi [`durata_secs`]) e la funzione restituisce
/// comunque un riepilogo coerente, con le distanze sommate e i tempi sottostimati.
///
/// # Come vengono calcolati i campi
///
/// Si scorrono le coppie di posizioni consecutive. Un segmento piu' lungo di
/// [`MOVEMENT_THRESHOLD_METERS`] e' uno spostamento: la sua lunghezza entra in
/// `total_distance_meters` e la sua durata in `moving_duration_secs`. Un segmento sotto
/// soglia e' rumore della sorgente: non somma distanza e la sua durata entra in
/// `pause_duration_secs`. Le durate dei segmenti sotto soglia consecutivi si accumulano
/// nella stessa pausa, che si chiude al primo segmento sopra soglia (o alla fine della
/// sequenza) e vale una sosta in `stop_count` solo se ha raggiunto [`STILL_AFTER_SECS`].
///
/// `average_speed_kmh` e' la velocita' tenuta mentre ci si muoveva, quindi divide la
/// distanza per il solo tempo in movimento; con zero secondi in movimento vale `0.0` e
/// mai `NaN` o infinito. Con meno di due posizioni non esiste nessun segmento e tutti i
/// campi numerici valgono zero.
///
/// # Costo
///
/// Tempo lineare nel numero di posizioni, ma `route` e' una copia dell'intero ingresso:
/// su percorsi da qualche migliaio di punti la memoria (e il messaggio che ne deriva)
/// cresce di conseguenza. La semplificazione del percorso e' un miglioramento successivo,
/// fuori da questo passo.
pub fn analyze(positions: &[PositionRecord]) -> MovementSummary {
    let mut total_distance_meters = 0.0;
    let mut moving_duration_secs: u64 = 0;
    let mut pause_duration_secs: u64 = 0;
    let mut stop_count: u32 = 0;
    // Durata della pausa ancora aperta, azzerata da ogni segmento in movimento.
    let mut pausa_aperta_secs: u64 = 0;

    for coppia in positions.windows(2) {
        let [precedente, corrente] = coppia else {
            // `windows(2)` non produce mai fette di lunghezza diversa da due.
            continue;
        };

        let distanza = haversine_meters(precedente.coordinate, corrente.coordinate);
        let intervallo_secs = durata_secs(precedente.recorded_at, corrente.recorded_at);

        if distanza > MOVEMENT_THRESHOLD_METERS {
            total_distance_meters += distanza;
            moving_duration_secs = moving_duration_secs.saturating_add(intervallo_secs);
            stop_count = chiudi_pausa(stop_count, pausa_aperta_secs);
            pausa_aperta_secs = 0;
        } else {
            pause_duration_secs = pause_duration_secs.saturating_add(intervallo_secs);
            pausa_aperta_secs = pausa_aperta_secs.saturating_add(intervallo_secs);
        }
    }

    // Una pausa che arriva fino all'ultima posizione non ha un segmento in movimento che
    // la chiuda: senza questa riga l'ultima sosta del percorso non verrebbe contata.
    stop_count = chiudi_pausa(stop_count, pausa_aperta_secs);

    MovementSummary {
        total_distance_meters,
        moving_duration_secs,
        pause_duration_secs,
        average_speed_kmh: velocita_media_kmh(total_distance_meters, moving_duration_secs),
        stop_count,
        route: positions.to_vec(),
    }
}

/// Aggiunge una sosta al conteggio se la pausa appena chiusa era abbastanza lunga.
///
/// La soglia e' inclusa: una pausa di esattamente [`STILL_AFTER_SECS`] secondi e' gia'
/// un veicolo fermo secondo il requisito R5, come in [`StateTracker::tick`].
///
/// [`StateTracker::tick`]: crate::state::StateTracker::tick
fn chiudi_pausa(stop_count: u32, pausa_secs: u64) -> u32 {
    if pausa_secs >= STILL_AFTER_SECS {
        // Saturare invece che andare in overflow: oltre quattro miliardi di soste il
        // conteggio smette di salire, che e' meglio di un panico in release.
        stop_count.saturating_add(1)
    } else {
        stop_count
    }
}

/// Secondi fra due istanti, zero se il secondo non viene dopo il primo.
///
/// `num_seconds` tronca verso lo zero, quindi le frazioni di secondo fra due rilevazioni
/// non vengono contate. La conversione a `u64` scarta le durate negative, che possono
/// nascere solo se la precondizione di ordinamento di [`analyze`] e' violata: contarle
/// come zero evita che un ingresso disordinato produca durate assurde.
fn durata_secs(inizio: DateTime<Utc>, fine: DateTime<Utc>) -> u64 {
    u64::try_from((fine - inizio).num_seconds()).unwrap_or(0)
}

/// Velocita' media in km/h su una distanza in metri e una durata in secondi.
///
/// Con durata nulla restituisce `0.0` invece di dividere per zero: in virgola mobile la
/// divisione non fa terminare il processo ma produce infinito, o `NaN` se anche la
/// distanza e' nulla, e quel valore finirebbe nel [`MovementSummary`] mandato al client.
fn velocita_media_kmh(distanza_metri: f64, durata_secs: u64) -> f64 {
    if durata_secs == 0 {
        return 0.0;
    }
    distanza_metri / durata_secs as f64 * SECONDI_PER_ORA_SU_METRI_PER_KM
}

/// Traduce un [`AnalysisRange`] negli estremi temporali con cui interrogare lo storico.
///
/// E' un trait di estensione e non un `impl` inerente perche' [`AnalysisRange`] appartiene
/// a `georuggine-common`: la regola dell'orfano vieta di aggiungergli metodi da qui. Chi
/// vuole chiamare [`to_bounds`] deve percio' importare il trait.
///
/// [`to_bounds`]: AnalysisRangeExt::to_bounds
pub trait AnalysisRangeExt {
    /// Estremi `(from, to)` dell'intervallo, in UTC e con entrambi gli estremi inclusi.
    ///
    /// Le tre varianti di calendario partono dalla mezzanotte **locale** del giorno, del
    /// lunedi' o del primo del mese a cui `now` appartiene, e arrivano a `now` stesso.
    /// L'estremo superiore e' l'istante corrente e non la fine del periodo perche' oltre
    /// `now` non esistono posizioni da leggere, e cosi' `from <= to` vale sempre senza
    /// dover restituire un limite che cade nel futuro.
    ///
    /// La mezzanotte si calcola nel fuso di sistema (`chrono::Local`) perche' "oggi" e'
    /// il giorno di chi guarda la flotta, non quello UTC; il risultato torna in UTC per
    /// essere confrontabile con i `recorded_at` salvati, che sono sempre UTC.
    ///
    /// [`AnalysisRange::Custom`] restituisce i propri estremi immutati: verificare che
    /// `from` non venga dopo `to` e' compito del chiamante.
    fn to_bounds(&self, now: DateTime<Utc>) -> (DateTime<Utc>, DateTime<Utc>);
}

impl AnalysisRangeExt for AnalysisRange {
    fn to_bounds(&self, now: DateTime<Utc>) -> (DateTime<Utc>, DateTime<Utc>) {
        estremi_nel_fuso(self, now, &Local)
    }
}

/// Corpo di [`AnalysisRangeExt::to_bounds`], parametrico sul fuso orario.
///
/// Il fuso e' un parametro invece che `Local` fisso solo per i test: il comportamento sui
/// cambi di ora dipende dal database dei fusi del sistema, e con `Local` un test non
/// potrebbe esercitare in modo deterministico una mezzanotte inesistente o ripetuta.
fn estremi_nel_fuso<Tz: TimeZone>(
    range: &AnalysisRange,
    now: DateTime<Utc>,
    fuso: &Tz,
) -> (DateTime<Utc>, DateTime<Utc>) {
    let oggi = now.with_timezone(fuso).date_naive();

    let giorno_iniziale = match range {
        AnalysisRange::Custom { from, to } => return (*from, *to),
        AnalysisRange::Today => oggi,
        AnalysisRange::CurrentWeek => lunedi_della_settimana(oggi),
        AnalysisRange::CurrentMonth => oggi.with_day(1).unwrap_or(oggi),
    };

    (primo_istante_del_giorno(giorno_iniziale, fuso), now)
}

/// Lunedi' della settimana a cui il giorno appartiene, il giorno stesso se e' un lunedi'.
///
/// Una domenica appartiene alla settimana che finisce quel giorno, quindi si torna
/// indietro di sei giorni e non avanti di uno.
fn lunedi_della_settimana(giorno: NaiveDate) -> NaiveDate {
    let arretramento = u64::from(giorno.weekday().num_days_from_monday());
    // `checked_sub_days` fallisce solo a ridosso della data minima rappresentabile, dove
    // restare sul giorno stesso e' comunque un estremo inferiore valido.
    giorno
        .checked_sub_days(Days::new(arretramento))
        .unwrap_or(giorno)
}

/// Primo istante di un giorno di calendario in un dato fuso, espresso in UTC.
///
/// La mezzanotte locale non e' sempre un istante che esiste: nei fusi che spostano avanti
/// l'orologio a mezzanotte le 00:00 vengono saltate, e in quelli che lo spostano indietro
/// capitano due volte. Nel primo caso si cerca in avanti a passi di
/// [`PASSO_RICERCA_MINUTI`] il primo orario che esiste davvero, nel secondo si prende la
/// prima delle due occorrenze, cosi' l'intervallo copre comunque tutta la giornata locale.
/// Se nessun orario del giorno fosse valido, cosa che nessun fuso reale produce, si
/// ripiega sulla mezzanotte UTC: un estremo inferiore ragionevole, mai un panico.
fn primo_istante_del_giorno<Tz: TimeZone>(giorno: NaiveDate, fuso: &Tz) -> DateTime<Utc> {
    for minuti in (0..MINUTI_IN_UN_GIORNO).step_by(PASSO_RICERCA_MINUTI) {
        let Some(orario) = NaiveTime::from_hms_opt(minuti / 60, minuti % 60, 0) else {
            continue;
        };
        match fuso.from_local_datetime(&giorno.and_time(orario)) {
            MappedLocalTime::Single(istante) => return istante.to_utc(),
            MappedLocalTime::Ambiguous(prima, _) => return prima.to_utc(),
            MappedLocalTime::None => {}
        }
    }

    giorno.and_time(NaiveTime::MIN).and_utc()
}

#[cfg(test)]
mod tests {
    use super::*;

    use chrono::{FixedOffset, NaiveDateTime, Offset, Weekday};
    use georuggine_common::types::{Coordinate, UserId};

    /// Utente a cui appartengono le posizioni dei test.
    const UTENTE: UserId = UserId(1);

    /// Latitudine di partenza dei test: piazza Castello, Torino.
    const LAT_PARTENZA: f64 = 45.0703;

    /// Longitudine di partenza dei test.
    const LON_PARTENZA: f64 = 7.6869;

    /// Gradi di latitudine che valgono circa 100 m.
    ///
    /// Un grado di latitudine vale circa 111_195 m sulla sfera usata da
    /// [`haversine_meters`], quindi 0,000_9 gradi valgono circa 100,08 m: venti volte
    /// [`MOVEMENT_THRESHOLD_METERS`], abbastanza perche' il segmento sia inequivocabilmente
    /// un movimento.
    const GRADI_PER_CENTO_METRI: f64 = 0.000_9;

    /// Gradi di latitudine che valgono circa 1 m, quindi sotto la soglia di movimento.
    const GRADI_PER_UN_METRO: f64 = 0.000_009;

    /// Costruisce una coordinata che il test da' per valida.
    fn coord(lat: f64, lon: f64) -> Coordinate {
        Coordinate::new(lat, lon).expect("coordinata di test valida")
    }

    /// Posizione a `passi` scatti di [`GRADI_PER_CENTO_METRI`] a nord della partenza,
    /// piu' `rumore` scatti di [`GRADI_PER_UN_METRO`], rilevata a `secondi` dall'inizio.
    fn punto(passi: f64, rumore: f64, secondi: i64) -> PositionRecord {
        PositionRecord {
            user_id: UTENTE,
            coordinate: coord(
                LAT_PARTENZA + passi * GRADI_PER_CENTO_METRI + rumore * GRADI_PER_UN_METRO,
                LON_PARTENZA,
            ),
            recorded_at: istante(secondi),
        }
    }

    /// Istante UTC a `secondi` da 2026-01-01T00:00:00Z.
    fn istante(secondi: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(1_767_225_600 + secondi, 0).expect("istante di test valido")
    }

    /// Distanza fra due punti separati da uno scatto di [`GRADI_PER_CENTO_METRI`].
    ///
    /// Si misura con la stessa formula che usa [`analyze`]: i test sulla distanza
    /// verificano che i segmenti giusti vengano sommati, non che l'emisenoverso sia
    /// corretto, che e' gia' verificato nei test di [`haversine_meters`].
    fn passo_metri() -> f64 {
        haversine_meters(punto(0.0, 0.0, 0).coordinate, punto(1.0, 0.0, 0).coordinate)
    }

    /// Verifica che due valori in virgola mobile coincidano entro l'1%.
    fn quasi_uguale(misurato: f64, atteso: f64, cosa: &str) {
        assert!(
            (misurato - atteso).abs() <= atteso.abs() * 0.01,
            "{cosa}: misurato {misurato}, atteso {atteso} entro l'1%"
        );
    }

    // --- analyze: percorso lineare ---

    #[test]
    fn un_percorso_rettilineo_ha_distanza_e_velocita_attese() {
        // Dieci punti in linea retta, uno scatto di circa 100 m ogni 10 s: nove segmenti
        // da percorrere, 90 s di movimento, circa 10 m/s ovvero 36 km/h.
        let posizioni: Vec<_> = (0..10)
            .map(|i| punto(f64::from(i), 0.0, i64::from(i) * 10))
            .collect();

        let riepilogo = analyze(&posizioni);

        quasi_uguale(
            riepilogo.total_distance_meters,
            9.0 * passo_metri(),
            "distanza totale",
        );
        quasi_uguale(riepilogo.total_distance_meters, 900.0, "distanza totale");
        assert_eq!(riepilogo.moving_duration_secs, 90);
        assert_eq!(riepilogo.pause_duration_secs, 0);
        assert_eq!(riepilogo.stop_count, 0);
        quasi_uguale(riepilogo.average_speed_kmh, 36.0, "velocita' media");
        assert_eq!(
            riepilogo.route, posizioni,
            "la rotta riporta i punti ricevuti"
        );
    }

    #[test]
    fn un_segmento_sotto_soglia_non_somma_distanza() {
        // Due letture separate da circa un metro: rumore della sorgente, non movimento.
        let posizioni = vec![punto(0.0, 0.0, 0), punto(0.0, 1.0, 10)];

        let riepilogo = analyze(&posizioni);

        assert_eq!(riepilogo.total_distance_meters, 0.0);
        assert_eq!(riepilogo.moving_duration_secs, 0);
        assert_eq!(riepilogo.pause_duration_secs, 10);
    }

    // --- analyze: pause e soste ---

    #[test]
    fn una_pausa_di_dieci_minuti_non_abbassa_la_velocita_media() {
        // Due scatti da 100 m in 10 s l'uno, poi dieci minuti fermi divisi in cinque
        // letture di rumore, poi altri due scatti: 40 s di movimento in tutto.
        let mut posizioni = vec![punto(0.0, 0.0, 0), punto(1.0, 0.0, 10), punto(2.0, 0.0, 20)];
        for i in 1..=5 {
            posizioni.push(punto(2.0, f64::from(i % 2), 20 + i64::from(i) * 120));
        }
        posizioni.push(punto(3.0, 0.0, 630));
        posizioni.push(punto(4.0, 0.0, 640));

        let riepilogo = analyze(&posizioni);

        assert_eq!(riepilogo.pause_duration_secs, 600);
        assert_eq!(riepilogo.moving_duration_secs, 40);
        assert_eq!(riepilogo.stop_count, 1);
        quasi_uguale(
            riepilogo.total_distance_meters,
            4.0 * passo_metri(),
            "distanza totale",
        );
        // 400 m in 40 s sono 10 m/s: se la pausa entrasse nel divisore la velocita'
        // scenderebbe a poco piu' di 2 km/h.
        quasi_uguale(riepilogo.average_speed_kmh, 36.0, "velocita' media");
    }

    #[test]
    fn una_pausa_di_due_minuti_non_conta_come_sosta() {
        // Centoventi secondi fermi: meno di STILL_AFTER_SECS, quindi tempo di pausa ma
        // non una sosta.
        let posizioni = vec![
            punto(0.0, 0.0, 0),
            punto(1.0, 0.0, 10),
            punto(1.0, 1.0, 130),
            punto(2.0, 0.0, 140),
        ];

        let riepilogo = analyze(&posizioni);

        assert_eq!(riepilogo.pause_duration_secs, 120);
        assert_eq!(
            riepilogo.stop_count, 0,
            "sotto STILL_AFTER_SECS la pausa non e' una sosta"
        );
        assert_eq!(riepilogo.moving_duration_secs, 20);
    }

    #[test]
    fn una_pausa_lunga_quanto_la_soglia_conta_come_sosta() {
        let soglia = i64::try_from(STILL_AFTER_SECS).expect("soglia rappresentabile");
        let posizioni = vec![
            punto(0.0, 0.0, 0),
            punto(1.0, 0.0, 10),
            punto(1.0, 1.0, 10 + soglia),
        ];

        let riepilogo = analyze(&posizioni);

        assert_eq!(riepilogo.stop_count, 1, "la soglia e' inclusa");
    }

    #[test]
    fn due_pause_separate_da_un_movimento_contano_due_soste() {
        let soglia = i64::try_from(STILL_AFTER_SECS).expect("soglia rappresentabile");
        // Fermo per la soglia, un solo scatto di movimento, di nuovo fermo per la soglia.
        let posizioni = vec![
            punto(0.0, 0.0, 0),
            punto(0.0, 1.0, soglia),
            punto(1.0, 0.0, soglia + 10),
            punto(1.0, 1.0, soglia * 2 + 10),
        ];

        let riepilogo = analyze(&posizioni);

        assert_eq!(
            riepilogo.stop_count, 2,
            "un intervallo di movimento chiude la pausa e ne apre un'altra"
        );
        assert_eq!(
            riepilogo.pause_duration_secs,
            u64::try_from(soglia * 2).expect("durata non negativa")
        );
        assert_eq!(riepilogo.moving_duration_secs, 10);
    }

    #[test]
    fn piu_intervalli_fermi_di_fila_sono_una_sola_sosta() {
        // Tre intervalli da 100 s sotto soglia: 300 s di pausa in una fermata sola.
        let posizioni = vec![
            punto(0.0, 0.0, 0),
            punto(0.0, 1.0, 100),
            punto(0.0, 0.0, 200),
            punto(0.0, 1.0, 300),
        ];

        let riepilogo = analyze(&posizioni);

        assert_eq!(riepilogo.pause_duration_secs, 300);
        assert_eq!(riepilogo.stop_count, 1);
    }

    #[test]
    fn un_lungo_fermo_seguito_da_movimento_non_abbassa_la_velocita() {
        // Un'ora fermi, poi due scatti da 100 m in 10 s l'uno.
        let posizioni = vec![
            punto(0.0, 0.0, 0),
            punto(0.0, 1.0, 3_600),
            punto(1.0, 0.0, 3_610),
            punto(2.0, 0.0, 3_620),
        ];

        let riepilogo = analyze(&posizioni);

        assert_eq!(riepilogo.pause_duration_secs, 3_600);
        assert_eq!(riepilogo.moving_duration_secs, 20);
        assert_eq!(riepilogo.stop_count, 1);
        // 200 m in 20 s: sull'intera durata sarebbero stati meno di 0,2 km/h.
        quasi_uguale(riepilogo.average_speed_kmh, 36.0, "velocita' media");
    }

    // --- analyze: casi limite ---

    #[test]
    fn nessuna_posizione_produce_un_riepilogo_a_zero() {
        let riepilogo = analyze(&[]);

        assert_eq!(riepilogo.total_distance_meters, 0.0);
        assert_eq!(riepilogo.moving_duration_secs, 0);
        assert_eq!(riepilogo.pause_duration_secs, 0);
        assert_eq!(riepilogo.average_speed_kmh, 0.0);
        assert_eq!(riepilogo.stop_count, 0);
        assert!(riepilogo.route.is_empty());
    }

    #[test]
    fn una_sola_posizione_non_produce_segmenti() {
        let posizioni = vec![punto(0.0, 0.0, 0)];

        let riepilogo = analyze(&posizioni);

        assert_eq!(riepilogo.total_distance_meters, 0.0);
        assert_eq!(riepilogo.moving_duration_secs, 0);
        assert_eq!(riepilogo.pause_duration_secs, 0);
        assert_eq!(riepilogo.average_speed_kmh, 0.0);
        assert_eq!(riepilogo.stop_count, 0);
        assert_eq!(riepilogo.route, posizioni);
    }

    #[test]
    fn due_posizioni_con_lo_stesso_istante_non_dividono_per_zero() {
        // Uno spostamento oltre soglia in un intervallo di durata nulla: il tempo in
        // movimento resta zero, quindi la velocita' media non ha un divisore.
        let posizioni = vec![punto(0.0, 0.0, 0), punto(1.0, 0.0, 0)];

        let riepilogo = analyze(&posizioni);

        assert_eq!(riepilogo.moving_duration_secs, 0);
        assert_eq!(riepilogo.pause_duration_secs, 0);
        assert_eq!(riepilogo.average_speed_kmh, 0.0);
        assert!(
            riepilogo.average_speed_kmh.is_finite(),
            "la velocita' media non deve essere NaN ne' infinita"
        );
        assert!(riepilogo.total_distance_meters.is_finite());
        quasi_uguale(
            riepilogo.total_distance_meters,
            passo_metri(),
            "distanza totale",
        );
    }

    #[test]
    fn posizioni_fuori_ordine_non_producono_durate_negative() {
        // Precondizione violata di proposito: gli intervalli all'indietro valgono zero,
        // cosi' nessuna sottrazione va in prestito sotto zero.
        let posizioni = vec![punto(1.0, 0.0, 100), punto(0.0, 0.0, 0)];

        let riepilogo = analyze(&posizioni);

        assert_eq!(riepilogo.moving_duration_secs, 0);
        assert_eq!(riepilogo.pause_duration_secs, 0);
        assert!(riepilogo.average_speed_kmh.is_finite());
    }

    // --- to_bounds: fusi di prova ---

    /// Offset invernale del fuso di prova: un'ora avanti su UTC.
    fn ora_solare() -> FixedOffset {
        FixedOffset::east_opt(3_600).expect("offset di test valido")
    }

    /// Offset estivo del fuso di prova: due ore avanti su UTC.
    fn ora_legale() -> FixedOffset {
        FixedOffset::east_opt(7_200).expect("offset di test valido")
    }

    /// Giorno in cui il fuso di prova salta l'ora fra le 00:00 e le 01:00 locali.
    fn giorno_del_salto() -> NaiveDate {
        NaiveDate::from_ymd_opt(2026, 3, 29).expect("data di test valida")
    }

    /// Giorno in cui il fuso di prova ripete l'ora fra le 00:00 e le 01:00 locali.
    fn giorno_della_ripetizione() -> NaiveDate {
        NaiveDate::from_ymd_opt(2026, 10, 25).expect("data di test valida")
    }

    /// Fuso orario di prova che sposta il cambio d'ora a mezzanotte.
    ///
    /// I fusi europei cambiano l'ora alle 02:00 o alle 03:00 locali, quindi la loro
    /// mezzanotte esiste sempre e non esercita ne' il caso [`MappedLocalTime::None`] ne'
    /// [`MappedLocalTime::Ambiguous`]. Fusi reali con quel comportamento esistono, ma
    /// dipenderebbero dal database dei fusi del sistema e renderebbero il test diverso da
    /// macchina a macchina: questo fuso finto li riproduce in modo deterministico.
    #[derive(Debug, Clone, Copy)]
    struct FusoConCambioAMezzanotte;

    impl TimeZone for FusoConCambioAMezzanotte {
        type Offset = FixedOffset;

        fn from_offset(_offset: &FixedOffset) -> Self {
            Self
        }

        fn offset_from_local_date(&self, _local: &NaiveDate) -> MappedLocalTime<FixedOffset> {
            MappedLocalTime::Single(ora_solare())
        }

        fn offset_from_local_datetime(
            &self,
            local: &NaiveDateTime,
        ) -> MappedLocalTime<FixedOffset> {
            let prima_ora_del_giorno =
                local.time() < NaiveTime::from_hms_opt(1, 0, 0).unwrap_or_default();
            if local.date() == giorno_del_salto() && prima_ora_del_giorno {
                MappedLocalTime::None
            } else if local.date() == giorno_della_ripetizione() && prima_ora_del_giorno {
                MappedLocalTime::Ambiguous(ora_legale(), ora_solare())
            } else if local.date() < giorno_del_salto()
                || local.date() >= giorno_della_ripetizione()
            {
                MappedLocalTime::Single(ora_solare())
            } else {
                MappedLocalTime::Single(ora_legale())
            }
        }

        fn offset_from_utc_date(&self, _utc: &NaiveDate) -> FixedOffset {
            ora_solare()
        }

        fn offset_from_utc_datetime(&self, utc: &NaiveDateTime) -> FixedOffset {
            if utc.date() < giorno_del_salto() || utc.date() >= giorno_della_ripetizione() {
                ora_solare()
            } else {
                ora_legale()
            }
        }
    }

    /// Istante UTC corrispondente all'ora locale indicata nel fuso di sistema.
    ///
    /// I test di [`AnalysisRangeExt::to_bounds`] partono sempre da un'ora locale e
    /// verificano il risultato in ora locale: cosi' le attese non dipendono dal fuso
    /// della macchina che li esegue, che su una postazione italiana e sulla CI e' diverso.
    fn locale(anno: i32, mese: u32, giorno: u32, ora: u32, minuto: u32) -> DateTime<Utc> {
        let data = NaiveDate::from_ymd_opt(anno, mese, giorno).expect("data di test valida");
        let orario = NaiveTime::from_hms_opt(ora, minuto, 0).expect("orario di test valido");
        match Local.from_local_datetime(&data.and_time(orario)) {
            MappedLocalTime::Single(istante) => istante.to_utc(),
            MappedLocalTime::Ambiguous(prima, _) => prima.to_utc(),
            MappedLocalTime::None => panic!("l'ora locale scelta dal test deve esistere"),
        }
    }

    /// Data e ora locali di un istante UTC, nel fuso di sistema.
    fn in_locale(istante: DateTime<Utc>) -> NaiveDateTime {
        istante.with_timezone(&Local).naive_local()
    }

    /// Verifica che l'istante cada sulla mezzanotte locale della data attesa.
    fn e_mezzanotte_locale(istante: DateTime<Utc>, atteso: NaiveDate) {
        let locale = in_locale(istante);
        assert_eq!(locale.date(), atteso, "giorno di partenza inatteso");
        assert_eq!(
            locale.time(),
            NaiveTime::MIN,
            "l'intervallo deve partire dalla mezzanotte locale"
        );
    }

    // --- to_bounds: giorno, settimana, mese ---

    #[test]
    fn today_parte_dalla_mezzanotte_locale_del_giorno_di_now() {
        let adesso = locale(2026, 8, 12, 15, 30);

        let (from, to) = AnalysisRange::Today.to_bounds(adesso);

        e_mezzanotte_locale(
            from,
            NaiveDate::from_ymd_opt(2026, 8, 12).expect("data valida"),
        );
        assert_eq!(to, adesso, "l'estremo superiore e' l'istante corrente");
    }

    #[test]
    fn current_week_parte_dal_lunedi_della_settimana() {
        // 2026-08-12 e' un mercoledi': il lunedi' della sua settimana e' il 10.
        let adesso = locale(2026, 8, 12, 15, 30);
        assert_eq!(in_locale(adesso).date().weekday(), Weekday::Wed);

        let (from, to) = AnalysisRange::CurrentWeek.to_bounds(adesso);

        e_mezzanotte_locale(
            from,
            NaiveDate::from_ymd_opt(2026, 8, 10).expect("data valida"),
        );
        assert_eq!(to, adesso);
    }

    #[test]
    fn current_week_di_una_domenica_parte_dal_lunedi_precedente() {
        // 2026-08-16 e' una domenica: la sua settimana e' quella che finisce quel giorno,
        // quindi comincia il lunedi' 10 e non il 17.
        let adesso = locale(2026, 8, 16, 23, 59);
        assert_eq!(in_locale(adesso).date().weekday(), Weekday::Sun);

        let (from, _) = AnalysisRange::CurrentWeek.to_bounds(adesso);

        e_mezzanotte_locale(
            from,
            NaiveDate::from_ymd_opt(2026, 8, 10).expect("data valida"),
        );
    }

    #[test]
    fn current_week_di_un_lunedi_parte_da_quel_giorno_stesso() {
        let adesso = locale(2026, 8, 10, 0, 30);
        assert_eq!(in_locale(adesso).date().weekday(), Weekday::Mon);

        let (from, _) = AnalysisRange::CurrentWeek.to_bounds(adesso);

        e_mezzanotte_locale(
            from,
            NaiveDate::from_ymd_opt(2026, 8, 10).expect("data valida"),
        );
    }

    #[test]
    fn current_week_a_cavallo_di_due_mesi_risale_al_mese_prima() {
        // 2026-04-01 e' un mercoledi': il lunedi' della sua settimana cade a marzo.
        let adesso = locale(2026, 4, 1, 9, 0);
        assert_eq!(in_locale(adesso).date().weekday(), Weekday::Wed);

        let (from, _) = AnalysisRange::CurrentWeek.to_bounds(adesso);

        e_mezzanotte_locale(
            from,
            NaiveDate::from_ymd_opt(2026, 3, 30).expect("data valida"),
        );
    }

    #[test]
    fn current_month_parte_dal_primo_del_mese() {
        let adesso = locale(2026, 8, 12, 15, 30);

        let (from, to) = AnalysisRange::CurrentMonth.to_bounds(adesso);

        e_mezzanotte_locale(
            from,
            NaiveDate::from_ymd_opt(2026, 8, 1).expect("data valida"),
        );
        assert_eq!(to, adesso);
    }

    #[test]
    fn current_month_del_primo_giorno_parte_da_quel_giorno_stesso() {
        let adesso = locale(2026, 2, 1, 0, 1);

        let (from, _) = AnalysisRange::CurrentMonth.to_bounds(adesso);

        e_mezzanotte_locale(
            from,
            NaiveDate::from_ymd_opt(2026, 2, 1).expect("data valida"),
        );
    }

    // --- to_bounds: Custom ---

    #[test]
    fn custom_restituisce_gli_estremi_ricevuti() {
        let from = istante(0);
        let to = istante(86_400);

        assert_eq!(
            AnalysisRange::Custom { from, to }.to_bounds(istante(1_000)),
            (from, to)
        );
    }

    #[test]
    fn custom_non_valida_gli_estremi_invertiti() {
        // La validazione da > a e' compito del chiamante: qui gli estremi passano cosi'
        // come sono, senza correzioni silenziose.
        let from = istante(86_400);
        let to = istante(0);

        assert_eq!(
            AnalysisRange::Custom { from, to }.to_bounds(istante(1_000)),
            (from, to)
        );
    }

    // --- to_bounds: invarianti ---

    #[test]
    fn from_non_supera_mai_to() {
        let istanti = [
            locale(2026, 1, 1, 0, 0),
            locale(2026, 2, 28, 23, 59),
            locale(2026, 6, 15, 12, 0),
            locale(2026, 12, 31, 23, 59),
            locale(2024, 2, 29, 6, 0),
        ];
        let intervalli = [
            AnalysisRange::Today,
            AnalysisRange::CurrentWeek,
            AnalysisRange::CurrentMonth,
        ];

        for adesso in istanti {
            for intervallo in intervalli {
                let (from, to) = intervallo.to_bounds(adesso);
                assert!(
                    from <= to,
                    "{intervallo:?} su {adesso} ha restituito ({from}, {to})"
                );
            }
        }
    }

    #[test]
    fn il_cambio_di_ora_legale_non_inverte_gli_estremi() {
        // Ultima domenica di marzo e di ottobre 2026, i giorni in cui i fusi europei
        // cambiano l'ora. Il mezzogiorno locale esiste in entrambi i casi, quindi il
        // test non dipende dal fuso della macchina; l'invariante verificata e' che gli
        // estremi restino ordinati e che chrono non vada in panico.
        for (mese, giorno) in [(3, 29), (10, 25)] {
            let adesso = locale(2026, mese, giorno, 12, 0);
            for intervallo in [
                AnalysisRange::Today,
                AnalysisRange::CurrentWeek,
                AnalysisRange::CurrentMonth,
            ] {
                let (from, to) = intervallo.to_bounds(adesso);
                assert!(
                    from <= to,
                    "{intervallo:?} sul cambio d'ora ha restituito ({from}, {to})"
                );
                assert!(from <= adesso);
            }
        }
    }

    // --- to_bounds: mezzanotte inesistente o ripetuta ---

    #[test]
    fn una_mezzanotte_inesistente_scala_al_primo_istante_valido() {
        // Le 00:00 del 29 marzo non esistono nel fuso di prova: l'intervallo deve
        // partire dal primo istante che esiste, cioe' l'01:00 con l'offset estivo.
        let adesso = giorno_del_salto()
            .and_hms_opt(12, 0, 0)
            .expect("orario valido")
            .and_utc();

        let (from, to) = estremi_nel_fuso(&AnalysisRange::Today, adesso, &FusoConCambioAMezzanotte);

        assert_eq!(
            from,
            NaiveDate::from_ymd_opt(2026, 3, 28)
                .and_then(|d| d.and_hms_opt(23, 0, 0))
                .expect("istante valido")
                .and_utc(),
            "le 01:00 con offset +02:00 sono le 23:00 UTC del giorno prima"
        );
        assert_eq!(to, adesso);
        assert!(from <= to);
    }

    #[test]
    fn una_mezzanotte_ripetuta_sceglie_la_prima_occorrenza() {
        // Le 00:00 del 25 ottobre capitano due volte nel fuso di prova: si prende la
        // prima, cosi' l'intervallo copre tutta la giornata locale.
        let adesso = giorno_della_ripetizione()
            .and_hms_opt(12, 0, 0)
            .expect("orario valido")
            .and_utc();

        let (from, _) = estremi_nel_fuso(&AnalysisRange::Today, adesso, &FusoConCambioAMezzanotte);

        assert_eq!(
            from,
            NaiveDate::from_ymd_opt(2026, 10, 24)
                .and_then(|d| d.and_hms_opt(22, 0, 0))
                .expect("istante valido")
                .and_utc(),
            "le 00:00 con offset +02:00 sono le 22:00 UTC del giorno prima"
        );
    }

    #[test]
    fn il_fuso_di_prova_ha_davvero_un_salto_e_una_ripetizione() {
        // Fissa le assunzioni su cui poggiano i due test precedenti: se il fuso finto
        // smettesse di avere il salto, quei test passerebbero senza provare nulla.
        let mezzanotte_del_salto = giorno_del_salto().and_time(NaiveTime::MIN);
        assert!(matches!(
            FusoConCambioAMezzanotte.offset_from_local_datetime(&mezzanotte_del_salto),
            MappedLocalTime::None
        ));

        let mezzanotte_ripetuta = giorno_della_ripetizione().and_time(NaiveTime::MIN);
        assert!(matches!(
            FusoConCambioAMezzanotte.offset_from_local_datetime(&mezzanotte_ripetuta),
            MappedLocalTime::Ambiguous(prima, seconda)
                if prima.fix() == ora_legale() && seconda.fix() == ora_solare()
        ));
    }
}
