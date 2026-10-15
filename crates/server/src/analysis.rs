//! Analisi dei percorsi: da una sequenza di [`PositionRecord`] a un [`MovementSummary`].
//!
//! Un segmento fra due posizioni consecutive e' movimento se supera
//! [`MOVEMENT_THRESHOLD_METERS`], altrimenti e' pausa. Una pausa lunga almeno
//! [`STILL_AFTER_SECS`] conta anche come sosta.

use chrono::{
    DateTime, Datelike, Days, Local, MappedLocalTime, NaiveDate, NaiveTime, TimeZone, Utc,
};
use georuggine_common::protocol::{AnalysisRange, MovementSummary};
use georuggine_common::types::{
    MOVEMENT_THRESHOLD_METERS, PositionRecord, STILL_AFTER_SECS, haversine_meters,
};

/// Metri al secondo espressi in chilometri all'ora.
const SECONDI_PER_ORA_SU_METRI_PER_KM: f64 = 3.6;

/// Minuti in un giorno, limite della ricerca del primo istante valido di una data.
const MINUTI_IN_UN_GIORNO: u32 = 24 * 60;

/// Passo in minuti della stessa ricerca.
const PASSO_RICERCA_MINUTI: usize = 15;

/// Punti massimi del tragitto in un riepilogo: circa 45 KB sul filo, ben sotto il limite
/// di un frame anche su un mese di dati.
pub const MAX_PUNTI_TRAGITTO: usize = 1_000;

/// Riepiloga i movimenti di una sequenza di posizioni ordinate per `recorded_at`.
///
/// La velocita' media considera il solo tempo in movimento e vale 0 se non ce n'e'.
/// Distanze e durate usano tutte le posizioni; il tragitto restituito e' un campione di
/// al massimo [`MAX_PUNTI_TRAGITTO`] punti.
pub fn analyze(positions: &[PositionRecord]) -> MovementSummary {
    let mut total_distance_meters = 0.0;
    let mut moving_duration_secs: u64 = 0;
    let mut pause_duration_secs: u64 = 0;
    let mut stop_count: u32 = 0;
    // Durata della pausa in corso, azzerata da ogni segmento in movimento.
    let mut pausa_aperta_secs: u64 = 0;

    for coppia in positions.windows(2) {
        let [precedente, corrente] = coppia else {
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

    // Chiude un'eventuale pausa che arriva fino all'ultima posizione.
    stop_count = chiudi_pausa(stop_count, pausa_aperta_secs);

    MovementSummary {
        total_distance_meters,
        moving_duration_secs,
        pause_duration_secs,
        average_speed_kmh: velocita_media_kmh(total_distance_meters, moving_duration_secs),
        stop_count,
        route: campiona_tragitto(positions, MAX_PUNTI_TRAGITTO),
        route_points: u64::try_from(positions.len()).unwrap_or(u64::MAX),
    }
}

/// Riduce il tragitto ad al massimo `massimo` punti presi a intervalli regolari,
/// mantenendo sempre il primo e l'ultimo.
fn campiona_tragitto(positions: &[PositionRecord], massimo: usize) -> Vec<PositionRecord> {
    let n = positions.len();
    if n <= massimo {
        return positions.to_vec();
    }
    // Evita la divisione per zero nella formula sotto.
    if massimo < 2 {
        return positions.iter().take(massimo).cloned().collect();
    }

    (0..massimo)
        .filter_map(|i| {
            let indice = i.saturating_mul(n - 1) / (massimo - 1);
            positions.get(indice).cloned()
        })
        .collect()
}

/// Aggiunge una sosta se la pausa appena chiusa raggiunge la soglia (inclusa).
fn chiudi_pausa(stop_count: u32, pausa_secs: u64) -> u32 {
    if pausa_secs >= STILL_AFTER_SECS {
        stop_count.saturating_add(1)
    } else {
        stop_count
    }
}

/// Secondi fra due istanti, zero se il secondo precede il primo.
fn durata_secs(inizio: DateTime<Utc>, fine: DateTime<Utc>) -> u64 {
    u64::try_from((fine - inizio).num_seconds()).unwrap_or(0)
}

/// Velocita' media in km/h; 0 con durata nulla, per non produrre infinito o `NaN`.
fn velocita_media_kmh(distanza_metri: f64, durata_secs: u64) -> f64 {
    if durata_secs == 0 {
        return 0.0;
    }
    distanza_metri / durata_secs as f64 * SECONDI_PER_ORA_SU_METRI_PER_KM
}

/// Estremi temporali di un [`AnalysisRange`] (trait di estensione, perche' il tipo sta
/// in `georuggine-common`).
pub trait AnalysisRangeExt {
    /// Estremi `(from, to)` in UTC, inclusi: dalla mezzanotte locale del primo giorno
    /// del periodo fino a `now`. [`AnalysisRange::Custom`] restituisce i propri estremi.
    fn to_bounds(&self, now: DateTime<Utc>) -> (DateTime<Utc>, DateTime<Utc>);
}

impl AnalysisRangeExt for AnalysisRange {
    fn to_bounds(&self, now: DateTime<Utc>) -> (DateTime<Utc>, DateTime<Utc>) {
        estremi_nel_fuso(self, now, &Local)
    }
}

/// Corpo di [`AnalysisRangeExt::to_bounds`], parametrico sul fuso per poterlo testare
/// sui cambi d'ora.
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

/// Lunedi' della settimana del giorno indicato.
fn lunedi_della_settimana(giorno: NaiveDate) -> NaiveDate {
    let arretramento = u64::from(giorno.weekday().num_days_from_monday());
    giorno
        .checked_sub_days(Days::new(arretramento))
        .unwrap_or(giorno)
}

/// Primo istante di un giorno nel fuso indicato, in UTC.
///
/// Se la mezzanotte non esiste per un cambio d'ora si prende il primo orario valido;
/// se e' ripetuta, la prima occorrenza.
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

    /// Coordinate di partenza dei test: piazza Castello, Torino.
    const LAT_PARTENZA: f64 = 45.0703;
    const LON_PARTENZA: f64 = 7.6869;

    /// Gradi di latitudine che valgono circa 100 m, ben oltre la soglia di movimento.
    const GRADI_PER_CENTO_METRI: f64 = 0.000_9;

    /// Gradi di latitudine che valgono circa 1 m, quindi sotto la soglia di movimento.
    const GRADI_PER_UN_METRO: f64 = 0.000_009;

    /// Soglia di sosta nel tipo con cui i test costruiscono gli istanti.
    fn soglia() -> i64 {
        i64::try_from(STILL_AFTER_SECS).expect("soglia rappresentabile")
    }

    /// Posizione `passi` volte 100 m e `rumore` volte 1 m a nord della partenza, a
    /// `secondi` dall'inizio.
    fn punto(passi: f64, rumore: f64, secondi: i64) -> PositionRecord {
        PositionRecord {
            user_id: UTENTE,
            coordinate: Coordinate::new(
                LAT_PARTENZA + passi * GRADI_PER_CENTO_METRI + rumore * GRADI_PER_UN_METRO,
                LON_PARTENZA,
            )
            .expect("coordinata di test valida"),
            recorded_at: istante(secondi),
        }
    }

    /// Istante UTC a `secondi` da 2026-01-01T00:00:00Z.
    fn istante(secondi: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(1_767_225_600 + secondi, 0).expect("istante di test valido")
    }

    /// Verifica che due valori in virgola mobile coincidano entro l'1%.
    fn quasi_uguale(misurato: f64, atteso: f64, cosa: &str) {
        assert!(
            (misurato - atteso).abs() <= atteso.abs() * 0.01,
            "{cosa}: misurato {misurato}, atteso {atteso} entro l'1%"
        );
    }

    #[test]
    fn un_percorso_rettilineo_ha_distanza_e_velocita_attese() {
        // Dieci punti a 100 m l'uno dall'altro ogni 10 s: 900 m in 90 s, 36 km/h.
        let posizioni: Vec<_> = (0..10)
            .map(|i| punto(f64::from(i), 0.0, i64::from(i) * 10))
            .collect();

        let riepilogo = analyze(&posizioni);

        quasi_uguale(riepilogo.total_distance_meters, 900.0, "distanza totale");
        assert_eq!(riepilogo.moving_duration_secs, 90);
        assert_eq!(riepilogo.pause_duration_secs, 0);
        assert_eq!(riepilogo.stop_count, 0);
        quasi_uguale(riepilogo.average_speed_kmh, 36.0, "velocita' media");
        assert_eq!(riepilogo.route, posizioni, "la rotta riporta i punti dati");
    }

    #[test]
    fn un_segmento_sotto_soglia_e_pausa_e_non_somma_distanza() {
        // Due letture separate da circa un metro: rumore della sorgente, non movimento.
        let riepilogo = analyze(&[punto(0.0, 0.0, 0), punto(0.0, 1.0, 10)]);

        assert_eq!(riepilogo.total_distance_meters, 0.0);
        assert_eq!(riepilogo.moving_duration_secs, 0);
        assert_eq!(riepilogo.pause_duration_secs, 10);
    }

    #[test]
    fn la_pausa_non_entra_nel_divisore_della_velocita_media() {
        // Un'ora fermi, poi 200 m in 20 s: la media resta 36 km/h.
        let riepilogo = analyze(&[
            punto(0.0, 0.0, 0),
            punto(0.0, 1.0, 3_600),
            punto(1.0, 0.0, 3_610),
            punto(2.0, 0.0, 3_620),
        ]);

        assert_eq!(riepilogo.pause_duration_secs, 3_600);
        assert_eq!(riepilogo.moving_duration_secs, 20);
        assert_eq!(riepilogo.stop_count, 1);
        quasi_uguale(riepilogo.average_speed_kmh, 36.0, "velocita' media");
    }

    #[test]
    fn una_pausa_vale_una_sosta_solo_dalla_soglia_in_su() {
        // Due minuti fermi: tempo di pausa, ma sotto STILL_AFTER_SECS.
        let breve = analyze(&[
            punto(0.0, 0.0, 0),
            punto(1.0, 0.0, 10),
            punto(1.0, 1.0, 130),
            punto(2.0, 0.0, 140),
        ]);
        assert_eq!(breve.pause_duration_secs, 120);
        assert_eq!(breve.stop_count, 0);
        assert_eq!(breve.moving_duration_secs, 20);

        // Esattamente la soglia: la soglia e' inclusa.
        let al_limite = analyze(&[
            punto(0.0, 0.0, 0),
            punto(1.0, 0.0, 10),
            punto(1.0, 1.0, 10 + soglia()),
        ]);
        assert_eq!(al_limite.stop_count, 1, "la soglia e' inclusa");

        // Tre intervalli sotto soglia di fila: 300 s di pausa in una fermata sola.
        let contigui = analyze(&[
            punto(0.0, 0.0, 0),
            punto(0.0, 1.0, 100),
            punto(0.0, 0.0, 200),
            punto(0.0, 1.0, 300),
        ]);
        assert_eq!(contigui.pause_duration_secs, 300);
        assert_eq!(contigui.stop_count, 1);
    }

    #[test]
    fn due_pause_separate_da_un_movimento_contano_due_soste() {
        // Fermo per la soglia, un solo scatto di movimento, di nuovo fermo per la soglia.
        let riepilogo = analyze(&[
            punto(0.0, 0.0, 0),
            punto(0.0, 1.0, soglia()),
            punto(1.0, 0.0, soglia() + 10),
            punto(1.0, 1.0, soglia() * 2 + 10),
        ]);

        assert_eq!(
            riepilogo.stop_count, 2,
            "un intervallo di movimento chiude la pausa e ne apre un'altra"
        );
        assert_eq!(
            riepilogo.pause_duration_secs,
            u64::try_from(soglia() * 2).expect("durata non negativa")
        );
        assert_eq!(riepilogo.moving_duration_secs, 10);
    }

    #[test]
    fn senza_segmenti_il_riepilogo_e_a_zero() {
        for posizioni in [vec![], vec![punto(0.0, 0.0, 0)]] {
            let riepilogo = analyze(&posizioni);

            assert_eq!(riepilogo.total_distance_meters, 0.0);
            assert_eq!(riepilogo.moving_duration_secs, 0);
            assert_eq!(riepilogo.pause_duration_secs, 0);
            assert_eq!(riepilogo.average_speed_kmh, 0.0);
            assert_eq!(riepilogo.stop_count, 0);
            assert_eq!(riepilogo.route, posizioni);
        }
    }

    #[test]
    fn durate_nulle_o_negative_non_producono_valori_assurdi() {
        // Spostamento in un intervallo nullo: la velocita' non deve dividere per zero.
        let istantaneo = analyze(&[punto(0.0, 0.0, 0), punto(1.0, 0.0, 0)]);
        assert_eq!(istantaneo.moving_duration_secs, 0);
        assert_eq!(istantaneo.average_speed_kmh, 0.0);
        assert!(istantaneo.total_distance_meters.is_finite());

        // Precondizione violata di proposito: gli intervalli all'indietro valgono zero.
        let disordinato = analyze(&[punto(1.0, 0.0, 100), punto(0.0, 0.0, 0)]);
        assert_eq!(disordinato.moving_duration_secs, 0);
        assert_eq!(disordinato.pause_duration_secs, 0);
        assert!(disordinato.average_speed_kmh.is_finite());
    }

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

    /// Fuso di prova con il cambio d'ora a mezzanotte, per esercitare mezzanotti
    /// inesistenti e ripetute senza dipendere dal database dei fusi.
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

    /// Istante UTC dell'ora locale indicata nel fuso di sistema.
    fn locale(anno: i32, mese: u32, giorno: u32, ora: u32, minuto: u32) -> DateTime<Utc> {
        let data = NaiveDate::from_ymd_opt(anno, mese, giorno).expect("data di test valida");
        let orario = NaiveTime::from_hms_opt(ora, minuto, 0).expect("orario di test valido");
        match Local.from_local_datetime(&data.and_time(orario)) {
            MappedLocalTime::Single(istante) => istante.to_utc(),
            MappedLocalTime::Ambiguous(prima, _) => prima.to_utc(),
            MappedLocalTime::None => panic!("l'ora locale scelta dal test deve esistere"),
        }
    }

    /// Verifica che l'istante cada sulla mezzanotte locale della data attesa.
    fn e_mezzanotte_locale(istante: DateTime<Utc>, anno: i32, mese: u32, giorno: u32) {
        let atteso = NaiveDate::from_ymd_opt(anno, mese, giorno).expect("data valida");
        let locale = istante.with_timezone(&Local).naive_local();

        assert_eq!(locale.date(), atteso, "giorno di partenza inatteso");
        assert_eq!(
            locale.time(),
            NaiveTime::MIN,
            "l'intervallo deve partire dalla mezzanotte locale"
        );
    }

    #[test]
    fn le_varianti_di_calendario_partono_dalla_mezzanotte_locale_e_arrivano_a_now() {
        // 2026-08-12 e' un mercoledi': il lunedi' della sua settimana e' il 10.
        let adesso = locale(2026, 8, 12, 15, 30);
        assert_eq!(
            adesso.with_timezone(&Local).naive_local().date().weekday(),
            Weekday::Wed
        );

        for (intervallo, mese, giorno) in [
            (AnalysisRange::Today, 8, 12),
            (AnalysisRange::CurrentWeek, 8, 10),
            (AnalysisRange::CurrentMonth, 8, 1),
        ] {
            let (from, to) = intervallo.to_bounds(adesso);
            e_mezzanotte_locale(from, 2026, mese, giorno);
            assert_eq!(to, adesso, "l'estremo superiore e' l'istante corrente");
        }
    }

    #[test]
    fn current_week_risale_sempre_al_lunedi_della_settimana() {
        for (adesso, mese_atteso, giorno_atteso) in [
            // Una domenica appartiene alla settimana che finisce quel giorno.
            (locale(2026, 8, 16, 23, 59), 8, 10),
            // Un lunedi' parte da se stesso.
            (locale(2026, 8, 10, 0, 30), 8, 10),
            // 2026-04-01 e' un mercoledi': il lunedi' cade nel mese prima.
            (locale(2026, 4, 1, 9, 0), 3, 30),
        ] {
            let (from, _) = AnalysisRange::CurrentWeek.to_bounds(adesso);
            e_mezzanotte_locale(from, 2026, mese_atteso, giorno_atteso);
        }

        // Il primo del mese parte da se stesso.
        let (from, _) = AnalysisRange::CurrentMonth.to_bounds(locale(2026, 2, 1, 0, 1));
        e_mezzanotte_locale(from, 2026, 2, 1);
    }

    #[test]
    fn custom_restituisce_gli_estremi_ricevuti_senza_correggerli() {
        // Gli estremi passano invariati, anche se invertiti.
        for (from, to) in [(istante(0), istante(86_400)), (istante(86_400), istante(0))] {
            assert_eq!(
                AnalysisRange::Custom { from, to }.to_bounds(istante(1_000)),
                (from, to)
            );
        }
    }

    #[test]
    fn from_non_supera_mai_to_nemmeno_sui_cambi_d_ora() {
        let istanti = [
            locale(2026, 1, 1, 0, 0),
            locale(2026, 2, 28, 23, 59),
            locale(2026, 12, 31, 23, 59),
            locale(2024, 2, 29, 6, 0),
            // Ultima domenica di marzo e di ottobre: cambio dell'ora in Europa.
            locale(2026, 3, 29, 12, 0),
            locale(2026, 10, 25, 12, 0),
        ];

        for adesso in istanti {
            for intervallo in [
                AnalysisRange::Today,
                AnalysisRange::CurrentWeek,
                AnalysisRange::CurrentMonth,
            ] {
                let (from, to) = intervallo.to_bounds(adesso);
                assert!(
                    from <= to && from <= adesso,
                    "{intervallo:?} su {adesso} ha restituito ({from}, {to})"
                );
            }
        }
    }

    #[test]
    fn una_mezzanotte_inesistente_o_ripetuta_non_rompe_l_intervallo() {
        // Verifica preliminare che il fuso di prova abbia salto e ripetizione.
        assert!(matches!(
            FusoConCambioAMezzanotte
                .offset_from_local_datetime(&giorno_del_salto().and_time(NaiveTime::MIN)),
            MappedLocalTime::None
        ));
        assert!(matches!(
            FusoConCambioAMezzanotte
                .offset_from_local_datetime(&giorno_della_ripetizione().and_time(NaiveTime::MIN)),
            MappedLocalTime::Ambiguous(prima, seconda)
                if prima.fix() == ora_legale() && seconda.fix() == ora_solare()
        ));

        // Le 00:00 del 29 marzo non esistono: si parte dalle 01:00 (+02:00).
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
                .and_utc()
        );
        assert!(from <= to);

        // Le 00:00 del 25 ottobre sono ripetute: si prende la prima.
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
                .and_utc()
        );
    }

    #[test]
    fn un_tragitto_lungo_viene_campionato_ma_i_totali_restano_completi() {
        // Un mese abbondante di invii ogni 30 secondi, un passo di 100 m ciascuno.
        let posizioni: Vec<PositionRecord> = (0..50_000)
            .map(|i| punto(f64::from(i % 100), 0.0, i64::from(i) * 30))
            .collect();

        let riepilogo = analyze(&posizioni);

        assert_eq!(riepilogo.route_points, 50_000);
        assert_eq!(riepilogo.route.len(), MAX_PUNTI_TRAGITTO);
        assert_eq!(
            riepilogo.route.first(),
            posizioni.first(),
            "la partenza resta vera"
        );
        assert_eq!(
            riepilogo.route.last(),
            posizioni.last(),
            "l'arrivo resta vero"
        );
        assert!(
            riepilogo
                .route
                .windows(2)
                .all(|coppia| coppia[0].recorded_at < coppia[1].recorded_at),
            "il campione resta in ordine cronologico e senza ripetizioni"
        );
        // Le durate si calcolano su tutte le posizioni, non sul campione.
        assert_eq!(
            riepilogo.moving_duration_secs + riepilogo.pause_duration_secs,
            49_999 * 30
        );
    }

    #[test]
    fn un_tragitto_corto_torna_intero() {
        let posizioni: Vec<PositionRecord> = (0..10)
            .map(|i| punto(f64::from(i), 0.0, i64::from(i) * 10))
            .collect();

        let riepilogo = analyze(&posizioni);

        assert_eq!(riepilogo.route, posizioni);
        assert_eq!(riepilogo.route_points, 10);
    }
}
