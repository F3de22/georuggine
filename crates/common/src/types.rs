//! Tipi di dominio di Georuggine: coordinate validate, identita' degli utenti,
//! stato di movimento e record di posizione.

use std::fmt;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Distanza minima in metri fra due posizioni successive dello stesso veicolo perche'
/// lo spostamento conti come movimento.
///
/// Non si confrontano le coordinate per uguaglianza esatta per due motivi. Il primo e'
/// di dominio: la sorgente simulata sovrappone rumore alla posizione, quindi un veicolo
/// fermo emette letture che differiscono comunque nelle ultime cifre decimali e
/// `precedente == corrente` sarebbe quasi sempre falso, facendo risultare l'intera flotta
/// perennemente in movimento. Il secondo e' numerico: confrontare `f64` con `==` e'
/// inaffidabile di per se', perche' valori ottenuti da calcoli diversi possono
/// rappresentare lo stesso punto senza avere gli stessi bit. La soglia assorbe il rumore
/// restando molto sotto allo spostamento di un veicolo reale fra due invii.
pub const MOVEMENT_THRESHOLD_METERS: f64 = 5.0;

/// Secondi senza movimento dopo i quali un veicolo passa a [`UserState::Still`].
///
/// Requisito R5 della traccia: un utente che non si sposta per tre minuti va segnalato
/// come fermo ai client di monitoraggio.
pub const STILL_AFTER_SECS: u64 = 180;

/// Raggio medio terrestre in metri, usato da [`haversine_meters`].
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
/// I campi sono privati e la deserializzazione passa dal costruttore validante, quindi
/// una `Coordinate` non puo' esistere in uno stato fuori intervallo nemmeno se i byte
/// arrivano dalla rete.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "CoordinateRepr")]
pub struct Coordinate {
    lat: f64,
    lon: f64,
}

/// Forma di trasporto di [`Coordinate`]: esiste solo per far passare la
/// deserializzazione da [`Coordinate::new`]. Ha gli stessi campi nello stesso ordine,
/// quindi la codifica sul filo e' identica.
#[derive(Deserialize)]
struct CoordinateRepr {
    lat: f64,
    lon: f64,
}

impl Coordinate {
    /// Costruisce una coordinata verificando che i valori siano in intervallo.
    ///
    /// Gli estremi sono inclusi. I valori non numerici (`NaN`, infiniti) falliscono i
    /// confronti d'intervallo e vengono quindi rifiutati.
    ///
    /// # Errori
    ///
    /// Restituisce [`CoordinateError`] se la latitudine e' fuori da `[-90, 90]` o la
    /// longitudine fuori da `[-180, 180]`.
    pub fn new(lat: f64, lon: f64) -> Result<Self, CoordinateError> {
        if !(-90.0..=90.0).contains(&lat) {
            return Err(CoordinateError::LatitudineFuoriIntervallo(lat));
        }
        if !(-180.0..=180.0).contains(&lon) {
            return Err(CoordinateError::LongitudineFuoriIntervallo(lon));
        }
        Ok(Self { lat, lon })
    }

    /// Latitudine in gradi decimali, sempre compresa in `[-90, 90]`.
    pub const fn lat(&self) -> f64 {
        self.lat
    }

    /// Longitudine in gradi decimali, sempre compresa in `[-180, 180]`.
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

/// Nome scelto da un utente, validato alla costruzione.
///
/// Deve avere da 3 a 32 caratteri, tutti alfanumerici ASCII oppure underscore. Come per
/// [`Coordinate`], il campo e' privato e la deserializzazione passa dalla validazione.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String")]
pub struct Username(String);

impl Username {
    /// Costruisce uno username validando lunghezza e caratteri ammessi.
    ///
    /// # Errori
    ///
    /// Restituisce [`UsernameError`] se il nome non ha da 3 a 32 caratteri o se contiene
    /// un carattere diverso da alfanumerico ASCII o underscore.
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

/// Stato di movimento di un utente, come mostrato ai client di monitoraggio.
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

/// Distanza in metri fra due coordinate lungo la superficie terrestre.
///
/// Usa la formula dell'emisenoverso su una sfera di raggio 6_371_008.8 m (raggio medio
/// IUGG). L'approssimazione sferica basta a distinguere un veicolo fermo da uno in
/// movimento: l'errore rispetto all'ellissoide resta sotto lo 0,5%, molto meno della
/// soglia di [`MOVEMENT_THRESHOLD_METERS`].
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

    /// Costruisce una coordinata che il test da' per valida.
    fn coord(lat: f64, lon: f64) -> Coordinate {
        Coordinate::new(lat, lon).expect("coordinata di test valida")
    }

    // --- Coordinate ---

    #[test]
    fn coordinate_ai_limiti_sono_accettate() {
        for (lat, lon) in [
            (90.0, 180.0),
            (-90.0, -180.0),
            (90.0, -180.0),
            (-90.0, 180.0),
            (0.0, 0.0),
        ] {
            let c = Coordinate::new(lat, lon).expect("i limiti sono inclusi");
            assert_eq!(c.lat(), lat);
            assert_eq!(c.lon(), lon);
        }
    }

    #[test]
    fn latitudine_oltre_il_limite_e_rifiutata() {
        assert_eq!(
            Coordinate::new(90.1, 0.0),
            Err(CoordinateError::LatitudineFuoriIntervallo(90.1))
        );
    }

    #[test]
    fn longitudine_oltre_il_limite_e_rifiutata() {
        assert_eq!(
            Coordinate::new(0.0, -180.1),
            Err(CoordinateError::LongitudineFuoriIntervallo(-180.1))
        );
    }

    #[test]
    fn coordinate_non_numeriche_sono_rifiutate() {
        assert!(Coordinate::new(f64::NAN, 0.0).is_err());
        assert!(Coordinate::new(0.0, f64::INFINITY).is_err());
    }

    #[test]
    fn coordinate_invalide_sono_rifiutate_in_deserializzazione() {
        // Forma di trasporto di `Coordinate`: due f64 in sequenza.
        let bytes = postcard::to_stdvec(&(500.0_f64, 0.0_f64)).expect("serializzazione riuscita");
        assert!(postcard::from_bytes::<Coordinate>(&bytes).is_err());
    }

    #[test]
    fn coordinate_valide_sopravvivono_al_roundtrip() {
        let originale = coord(45.0703, 7.6869);
        let bytes = postcard::to_stdvec(&originale).expect("serializzazione riuscita");
        let decodificata: Coordinate =
            postcard::from_bytes(&bytes).expect("deserializzazione riuscita");
        assert_eq!(decodificata, originale);
    }

    // --- Username ---

    #[test]
    fn username_validi_sono_accettati() {
        for valido in ["abc", "veicolo_01", "A1_b2", &"u".repeat(32)] {
            let username = Username::new(valido).expect("username valido");
            assert_eq!(username.as_str(), valido);
        }
    }

    #[test]
    fn username_troppo_corto_e_rifiutato() {
        assert_eq!(
            Username::new("ab"),
            Err(UsernameError::LunghezzaNonValida(2))
        );
    }

    #[test]
    fn username_troppo_lungo_e_rifiutato() {
        let lungo = "u".repeat(33);
        assert_eq!(
            Username::new(&lungo),
            Err(UsernameError::LunghezzaNonValida(33))
        );
    }

    #[test]
    fn username_con_spazi_e_rifiutato() {
        assert_eq!(
            Username::new("mio veicolo"),
            Err(UsernameError::CarattereNonAmmesso(' '))
        );
    }

    #[test]
    fn username_con_trattino_e_rifiutato() {
        assert_eq!(
            Username::new("mio-veicolo"),
            Err(UsernameError::CarattereNonAmmesso('-'))
        );
    }

    #[test]
    fn username_invalidi_sono_rifiutati_in_deserializzazione() {
        let bytes = postcard::to_stdvec("ab").expect("serializzazione riuscita");
        assert!(postcard::from_bytes::<Username>(&bytes).is_err());
    }

    // --- UserState ---

    #[test]
    fn userstate_si_mostra_in_italiano() {
        assert_eq!(UserState::Disconnected.to_string(), "Disconnesso");
        assert_eq!(UserState::Still.to_string(), "Fermo");
        assert_eq!(UserState::Moving.to_string(), "In movimento");
    }

    // --- PositionRecord ---

    #[test]
    fn position_record_sopravvive_al_roundtrip() {
        let originale = PositionRecord {
            user_id: UserId(7),
            coordinate: coord(45.4642, 9.19),
            recorded_at: DateTime::from_timestamp(1_700_000_000, 0).expect("istante valido"),
        };
        let bytes = postcard::to_stdvec(&originale).expect("serializzazione riuscita");
        let decodificato: PositionRecord =
            postcard::from_bytes(&bytes).expect("deserializzazione riuscita");
        assert_eq!(decodificato, originale);
    }

    // --- haversine ---

    #[test]
    fn haversine_di_un_punto_con_se_stesso_e_nulla() {
        let torino = coord(45.0703, 7.6869);
        assert_eq!(haversine_meters(torino, torino), 0.0);
    }

    #[test]
    fn haversine_torino_milano_e_circa_126_km() {
        let torino = coord(45.0703, 7.6869);
        let milano = coord(45.4642, 9.19);
        let distanza = haversine_meters(torino, milano);
        assert!(
            (distanza - 126_000.0).abs() < 2_000.0,
            "distanza Torino-Milano fuori tolleranza: {distanza} m"
        );
    }

    #[test]
    fn haversine_e_simmetrica() {
        let torino = coord(45.0703, 7.6869);
        let milano = coord(45.4642, 9.19);
        assert_eq!(
            haversine_meters(torino, milano),
            haversine_meters(milano, torino)
        );
    }

    #[test]
    fn haversine_di_un_metro_sta_sotto_la_soglia_di_movimento() {
        // Un grado di latitudine vale circa 111_320 m: 9e-6 gradi sono circa 1 m.
        let partenza = coord(45.0, 7.6869);
        let arrivo = coord(45.0 + 0.000_009, 7.6869);
        let distanza = haversine_meters(partenza, arrivo);
        assert!(
            distanza < MOVEMENT_THRESHOLD_METERS,
            "un metro dovrebbe stare sotto la soglia, misurati {distanza} m"
        );
    }
}
