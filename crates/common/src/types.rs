//! Tipi di dominio: coordinate validate, identita' degli utenti, stato di movimento e
//! record di posizione.

use std::fmt;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Distanza minima in metri perche' due posizioni successive contino come spostamento.
///
/// Assorbe il rumore delle coordinate simulate, che varia anche a veicolo fermo.
pub const MOVEMENT_THRESHOLD_METERS: f64 = 5.0;

/// Secondi senza movimento dopo i quali un veicolo passa a [`UserState::Still`].
pub const STILL_AFTER_SECS: u64 = 180;

/// Raggio medio terrestre in metri (IUGG), usato da [`haversine_meters`].
const EARTH_RADIUS_METERS: f64 = 6_371_008.8;

/// Numero minimo di caratteri di uno [`Username`].
const USERNAME_MIN_LEN: usize = 3;

/// Numero massimo di caratteri di uno [`Username`].
const USERNAME_MAX_LEN: usize = 32;

/// Errore restituito da [`Coordinate::new`] quando i valori sono fuori intervallo.
#[derive(Debug, Clone, PartialEq, Error)]
pub enum CoordinateError {
    /// La latitudine non appartiene all'intervallo `[-90, 90]`.
    #[error("latitudine {0} fuori dall'intervallo [-90, 90]")]
    LatitudineFuoriIntervallo(f64),
    /// La longitudine non appartiene all'intervallo `[-180, 180]`.
    #[error("longitudine {0} fuori dall'intervallo [-180, 180]")]
    LongitudineFuoriIntervallo(f64),
}

/// Punto sulla superficie terrestre, in gradi decimali.
///
/// La deserializzazione passa da [`Coordinate::new`], quindi anche una coordinata
/// arrivata dalla rete e' sempre in intervallo.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "CoordinateRepr")]
pub struct Coordinate {
    lat: f64,
    lon: f64,
}

/// Forma di trasporto di [`Coordinate`], usata per validare in deserializzazione.
#[derive(Deserialize)]
struct CoordinateRepr {
    lat: f64,
    lon: f64,
}

impl Coordinate {
    /// Costruisce una coordinata, estremi inclusi; `NaN` e infiniti sono rifiutati.
    ///
    /// # Errori
    ///
    /// [`CoordinateError`] se latitudine o longitudine sono fuori intervallo.
    pub fn new(lat: f64, lon: f64) -> Result<Self, CoordinateError> {
        if !(-90.0..=90.0).contains(&lat) {
            return Err(CoordinateError::LatitudineFuoriIntervallo(lat));
        }
        if !(-180.0..=180.0).contains(&lon) {
            return Err(CoordinateError::LongitudineFuoriIntervallo(lon));
        }
        Ok(Self { lat, lon })
    }

    /// Latitudine in gradi decimali, sempre in `[-90, 90]`.
    pub const fn lat(&self) -> f64 {
        self.lat
    }

    /// Longitudine in gradi decimali, sempre in `[-180, 180]`.
    pub const fn lon(&self) -> f64 {
        self.lon
    }
}

impl TryFrom<CoordinateRepr> for Coordinate {
    type Error = CoordinateError;

    fn try_from(repr: CoordinateRepr) -> Result<Self, Self::Error> {
        Self::new(repr.lat, repr.lon)
    }
}

/// Identificatore numerico di un utente, assegnato dal server.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct UserId(pub u32);

/// Errore restituito da [`Username::new`] quando il nome non rispetta il formato.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum UsernameError {
    /// Il numero di caratteri e' fuori da `3..=32`.
    #[error("username di {0} caratteri: la lunghezza ammessa e' da 3 a 32")]
    LunghezzaNonValida(usize),
    /// E' presente un carattere diverso da alfanumerico ASCII o underscore.
    #[error("carattere non ammesso {0:?}: sono permessi solo alfanumerici ASCII e underscore")]
    CarattereNonAmmesso(char),
}

/// Nome utente: da 3 a 32 caratteri, alfanumerici ASCII o underscore.
///
/// Come [`Coordinate`], viene validato anche in deserializzazione.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String")]
pub struct Username(String);

impl Username {
    /// Costruisce uno username valido.
    ///
    /// # Errori
    ///
    /// [`UsernameError`] se lunghezza o caratteri non sono ammessi.
    pub fn new(raw: &str) -> Result<Self, UsernameError> {
        Self::valida(raw)?;
        Ok(Self(raw.to_owned()))
    }

    /// Il nome come stringa.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Verifica lunghezza e caratteri senza allocare.
    fn valida(raw: &str) -> Result<(), UsernameError> {
        let lunghezza = raw.chars().count();
        if !(USERNAME_MIN_LEN..=USERNAME_MAX_LEN).contains(&lunghezza) {
            return Err(UsernameError::LunghezzaNonValida(lunghezza));
        }
        match raw
            .chars()
            .find(|c| !c.is_ascii_alphanumeric() && *c != '_')
        {
            Some(c) => Err(UsernameError::CarattereNonAmmesso(c)),
            None => Ok(()),
        }
    }
}

impl TryFrom<String> for Username {
    type Error = UsernameError;

    fn try_from(raw: String) -> Result<Self, Self::Error> {
        Self::valida(&raw)?;
        Ok(Self(raw))
    }
}

/// Stato di movimento di un utente.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum UserState {
    /// L'utente non ha una connessione attiva verso il server.
    Disconnected,
    /// L'utente e' connesso ma non si sposta da almeno [`STILL_AFTER_SECS`] secondi.
    Still,
    /// L'utente si e' spostato di almeno [`MOVEMENT_THRESHOLD_METERS`] metri di recente.
    Moving,
}

impl fmt::Display for UserState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let testo = match self {
            Self::Disconnected => "Disconnesso",
            Self::Still => "Fermo",
            Self::Moving => "In movimento",
        };
        f.write_str(testo)
    }
}

/// Posizione di un utente in un dato istante, come persistita dal server.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PositionRecord {
    /// Utente a cui la posizione appartiene.
    pub user_id: UserId,
    /// Punto rilevato.
    pub coordinate: Coordinate,
    /// Istante della rilevazione, in UTC.
    pub recorded_at: DateTime<Utc>,
}

/// Distanza in metri fra due coordinate, con la formula dell'emisenoverso su sfera.
pub fn haversine_meters(a: Coordinate, b: Coordinate) -> f64 {
    let lat_a = a.lat.to_radians();
    let lat_b = b.lat.to_radians();
    let delta_lat = (b.lat - a.lat).to_radians();
    let delta_lon = (b.lon - a.lon).to_radians();

    let h = (delta_lat / 2.0).sin().powi(2)
        + lat_a.cos() * lat_b.cos() * (delta_lon / 2.0).sin().powi(2);

    2.0 * EARTH_RADIUS_METERS * h.sqrt().atan2((1.0 - h).sqrt())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn coord(lat: f64, lon: f64) -> Coordinate {
        Coordinate::new(lat, lon).expect("coordinata di test valida")
    }

    fn torino() -> Coordinate {
        coord(45.0703, 7.6869)
    }

    fn milano() -> Coordinate {
        coord(45.4642, 9.19)
    }

    #[test]
    fn coordinate_ai_limiti_sono_accettate() {
        for (lat, lon) in [(90.0, 180.0), (-90.0, -180.0), (0.0, 0.0)] {
            let c = Coordinate::new(lat, lon).expect("i limiti sono inclusi");
            assert_eq!((c.lat(), c.lon()), (lat, lon));
        }
    }

    #[test]
    fn coordinate_fuori_intervallo_o_non_numeriche_sono_rifiutate() {
        assert_eq!(
            Coordinate::new(90.1, 0.0),
            Err(CoordinateError::LatitudineFuoriIntervallo(90.1))
        );
        assert_eq!(
            Coordinate::new(0.0, -180.1),
            Err(CoordinateError::LongitudineFuoriIntervallo(-180.1))
        );
        assert!(Coordinate::new(f64::NAN, 0.0).is_err());
        assert!(Coordinate::new(0.0, f64::INFINITY).is_err());
    }

    #[test]
    fn la_deserializzazione_passa_dalla_validazione() {
        // La forma di trasporto di `Coordinate` e' una coppia di f64.
        let fuori = postcard::to_stdvec(&(500.0_f64, 0.0_f64)).expect("serializzazione riuscita");
        assert!(postcard::from_bytes::<Coordinate>(&fuori).is_err());

        let corto = postcard::to_stdvec("ab").expect("serializzazione riuscita");
        assert!(postcard::from_bytes::<Username>(&corto).is_err());
    }

    #[test]
    fn position_record_sopravvive_al_roundtrip() {
        let originale = PositionRecord {
            user_id: UserId(7),
            coordinate: milano(),
            recorded_at: DateTime::from_timestamp(1_700_000_000, 0).expect("istante valido"),
        };

        let bytes = postcard::to_stdvec(&originale).expect("serializzazione riuscita");
        let decodificato: PositionRecord =
            postcard::from_bytes(&bytes).expect("deserializzazione riuscita");
        assert_eq!(decodificato, originale);
    }

    #[test]
    fn username_validi_sono_accettati() {
        for valido in ["abc", "veicolo_01", "A1_b2", &"u".repeat(32)] {
            let username = Username::new(valido).expect("username valido");
            assert_eq!(username.as_str(), valido);
        }
    }

    #[test]
    fn username_fuori_formato_sono_rifiutati() {
        assert_eq!(
            Username::new("ab"),
            Err(UsernameError::LunghezzaNonValida(2))
        );
        assert_eq!(
            Username::new(&"u".repeat(33)),
            Err(UsernameError::LunghezzaNonValida(33))
        );
        assert_eq!(
            Username::new("mio veicolo"),
            Err(UsernameError::CarattereNonAmmesso(' '))
        );
        assert_eq!(
            Username::new("mio-veicolo"),
            Err(UsernameError::CarattereNonAmmesso('-'))
        );
    }

    #[test]
    fn userstate_si_mostra_in_italiano() {
        assert_eq!(UserState::Disconnected.to_string(), "Disconnesso");
        assert_eq!(UserState::Still.to_string(), "Fermo");
        assert_eq!(UserState::Moving.to_string(), "In movimento");
    }

    #[test]
    fn haversine_e_nulla_su_se_stessa_e_simmetrica() {
        assert_eq!(haversine_meters(torino(), torino()), 0.0);
        assert_eq!(
            haversine_meters(torino(), milano()),
            haversine_meters(milano(), torino())
        );
    }

    #[test]
    fn haversine_torino_milano_e_circa_126_km() {
        let distanza = haversine_meters(torino(), milano());
        assert!(
            (distanza - 126_000.0).abs() < 2_000.0,
            "distanza fuori tolleranza: {distanza} m"
        );
    }

    #[test]
    fn uno_spostamento_di_un_metro_sta_sotto_la_soglia() {
        // Un grado di latitudine vale circa 111_320 m: 9e-6 gradi sono circa 1 m.
        let distanza = haversine_meters(coord(45.0, 7.6869), coord(45.000_009, 7.6869));
        assert!(
            distanza < MOVEMENT_THRESHOLD_METERS,
            "misurati {distanza} m"
        );
    }
}
