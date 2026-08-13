//! Sorgenti di posizione del veicolo: le strategie con cui il client simula il proprio
//! movimento (requisito R6).
//!
//! Il client non ha un GPS: la posizione da inviare al server arriva da una
//! [`PositionSource`], che il resto del programma usa dietro il trait senza sapere quale
//! strategia ci sia dietro. Qui vivono le prime due, [`FileSource`] e
//! [`RandomWalkSource`]; le altre due previste dalla traccia (percorso interpolato e
//! inserimento manuale) si aggiungeranno alle varianti di [`SourceKind`].
//!
//! Come per la logica di dominio del server, nessuna sorgente chiama `Utc::now()`: il
//! tempo entra come parametro di [`PositionSource::next_position`], cosi' i test possono
//! far scorrere ore in pochi microsecondi e verificare i tempi esatti di ogni punto.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use chrono::{DateTime, TimeDelta, Utc};
use georuggine_common::types::Coordinate;
use rand::rngs::StdRng;
use rand::{RngExt as _, SeedableRng as _};

/// Raggio medio terrestre in metri.
///
/// E' lo stesso valore (raggio medio IUGG) che `georuggine_common::types` usa in
/// `haversine_meters`, ed e' importante che lo resti: [`destinazione`] e la funzione di
/// distanza sono l'una l'inversa dell'altra solo se lavorano sulla stessa sfera. Con due
/// raggi diversi un punto costruito a esattamente `passo_massimo` metri risulterebbe
/// misurato a una distanza leggermente diversa, e il controllo sul raggio massimo di
/// [`RandomWalkSource`] potrebbe accettare punti appena fuori. La costante e' privata in
/// `common`, quindi va ripetuta qui.
const EARTH_RADIUS_METERS: f64 = 6_371_008.8;

/// Numero di colonne attese in una riga del CSV di [`FileSource`].
const COLONNE_ATTESE: usize = 3;

/// Intestazione attesa come prima riga del CSV di [`FileSource`].
const HEADER_ATTESO: &str = "lat,lon,offset_secs";

/// Quante direzioni diverse prova [`RandomWalkSource`] prima di rinunciare a muoversi.
///
/// Serve a garantire che ogni chiamata termini. Il caso in cui il limite viene raggiunto
/// e' quello del veicolo praticamente incollato al bordo del raggio massimo, dove quasi
/// tutte le direzioni portano fuori: sedici tentativi bastano a trovarne una valida in
/// tutti i casi realistici, e quando davvero non ce ne sono (raggio piu' piccolo del
/// passo) restare fermi per un giro e' la risposta giusta, non un errore.
const TENTATIVI_MASSIMI: u8 = 16;

/// Sorgente da cui il client legge la posizione da inviare al server.
///
/// Il trait richiede [`Send`] perche' la sorgente vive nel task che invia le posizioni,
/// e tokio puo' spostare quel task fra i thread dell'esecutore.
pub trait PositionSource: Send {
    /// Posizione da inviare nell'istante `now`, oppure `None` a simulazione finita.
    ///
    /// `None` non e' un errore: significa che la sorgente ha esaurito quello che aveva
    /// da riprodurre e che il chiamante puo' smettere di interrogarla. Una sorgente che
    /// ha ancora qualcosa da dire ma nulla di *nuovo* in questo istante non restituisce
    /// `None`: ripete l'ultima posizione nota, che e' esattamente cio' che farebbe un
    /// GPS su un veicolo fermo.
    fn next_position(&mut self, now: DateTime<Utc>) -> Option<Coordinate>;

    /// Nome della strategia, per i log e la diagnostica.
    fn name(&self) -> &'static str;
}

/// Un punto del percorso letto da file: dove si trova e dopo quanto va emesso.
#[derive(Debug, Clone, Copy, PartialEq)]
struct Punto {
    /// Coordinata da inviare.
    coordinate: Coordinate,
    /// Ritardo rispetto al tempo zero della sorgente.
    offset: TimeDelta,
}

/// Sorgente che riproduce un percorso registrato in un file CSV.
///
/// Il file ha l'intestazione `lat,lon,offset_secs` e una riga per punto, dove
/// `offset_secs` e' il ritardo in secondi rispetto al **tempo zero** della sorgente. Il
/// tempo zero e' il primo istante passato a [`PositionSource::next_position`], non
/// l'istante di costruzione: fra il caricamento del file e la prima posizione inviata
/// possono passare la connessione al server e l'autenticazione, e non ha senso che il
/// percorso scorra durante l'attesa.
///
/// Ogni chiamata fa avanzare il percorso di al massimo un punto: se il chiamante torna
/// in ritardo e piu' righe sono nel frattempo scadute, escono una alla volta e
/// nell'ordine del file, senza saltarne nessuna. Quando non c'e' nessun punto nuovo da
/// consegnare la sorgente ripete l'ultima coordinata emessa: e' cosi' che il percorso di
/// esempio produce la pausa che fa passare il veicolo a `Still` (requisito R5).
///
/// # Esempio
///
/// ```no_run
/// use chrono::Utc;
/// use georuggine_client::sources::{FileSource, PositionSource};
///
/// # fn main() -> std::io::Result<()> {
/// let mut sorgente = FileSource::from_path("assets/percorso_demo.csv", false)?;
/// if let Some(posizione) = sorgente.next_position(Utc::now()) {
///     println!("{} {}", posizione.lat(), posizione.lon());
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct FileSource {
    /// Punti del percorso, nell'ordine del file. Non e' mai vuoto: un file senza punti
    /// utilizzabili viene rifiutato da [`FileSource::from_path`].
    punti: Vec<Punto>,
    /// Se vero il percorso riparte da capo dopo l'ultimo punto.
    loop_forever: bool,
    /// Istante da cui si contano gli offset, fissato alla prima chiamata.
    zero: Option<DateTime<Utc>>,
    /// Indice del prossimo punto da emettere.
    prossimo: usize,
    /// Ultima coordinata emessa, ripetuta finche' non scade il punto successivo.
    ultima: Option<Coordinate>,
    /// Vero quando il percorso e' finito e non ripartira': da qui in poi solo `None`.
    esaurita: bool,
}

impl FileSource {
    /// Carica e valida il percorso dal file indicato.
    ///
    /// La lettura avviene subito e non alla prima chiamata: un file mancante, illeggibile
    /// o senza punti utilizzabili si scopre all'avvio del client, quando c'e' ancora
    /// qualcuno che legge l'errore, e non mezz'ora dopo sotto forma di veicolo che non
    /// si muove.
    ///
    /// Le righe malformate non fanno fallire il caricamento: vengono scartate una per
    /// una con un `tracing::warn` che ne riporta il numero di riga nel file, cosi' un
    /// percorso registrato con qualche riga corrotta resta utilizzabile. Sono malformate
    /// le righe con meno di [`COLONNE_ATTESE`] colonne, con latitudine o longitudine non
    /// numeriche o fuori intervallo, e con offset non intero o negativo; le colonne oltre
    /// la terza vengono ignorate, e le righe vuote saltate in silenzio. La prima riga e'
    /// sempre trattata come intestazione e scartata.
    ///
    /// La lettura e' sincrona e bloccante: va chiamata in fase di avvio o dentro
    /// `tokio::task::spawn_blocking`, mai in un task async.
    ///
    /// # Errori
    ///
    /// - l'errore di `std::fs` se il file non esiste o non e' leggibile;
    /// - [`io::ErrorKind::InvalidData`] se dopo lo scarto delle righe malformate non
    ///   resta nessun punto: una sorgente con zero posizioni non e' una simulazione ma
    ///   un errore di configurazione, e restituire una sorgente che si spegne alla prima
    ///   chiamata lo nasconderebbe.
    pub fn from_path(percorso: impl AsRef<Path>, loop_forever: bool) -> io::Result<Self> {
        let percorso = percorso.as_ref();
        let contenuto = fs::read_to_string(percorso)?;
        let punti = analizza(&contenuto, percorso);

        if punti.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "{}: nessuna riga valida, atteso un CSV con intestazione `{HEADER_ATTESO}`",
                    percorso.display()
                ),
            ));
        }

        tracing::info!(
            percorso = %percorso.display(),
            punti = punti.len(),
            loop_forever,
            "percorso caricato"
        );

        Ok(Self {
            punti,
            loop_forever,
            zero: None,
            prossimo: 0,
            ultima: None,
            esaurita: false,
        })
    }

    /// Ricomincia il percorso da capo fissando `now` come nuovo tempo zero.
    fn ricomincia(&mut self, now: DateTime<Utc>) {
        self.zero = Some(now);
        self.prossimo = 0;
    }
}

impl PositionSource for FileSource {
    /// Consegna il prossimo punto scaduto, oppure ripete l'ultima posizione nota.
    ///
    /// Quando l'ultimo punto viene consegnato la sorgente decide subito il proprio
    /// destino: con `loop_forever` il tempo zero si sposta su `now` e il percorso
    /// riparte dalla prima riga (il periodo del ciclo diventa cosi' l'offset dell'ultimo
    /// punto, stabile giro dopo giro); senza, la sorgente e' esaurita e ogni chiamata
    /// successiva restituisce `None`.
    fn next_position(&mut self, now: DateTime<Utc>) -> Option<Coordinate> {
        if self.esaurita {
            return None;
        }

        let zero = *self.zero.get_or_insert(now);

        if let Some(punto) = self.punti.get(self.prossimo).copied() {
            if now.signed_duration_since(zero) >= punto.offset {
                self.prossimo += 1;
                self.ultima = Some(punto.coordinate);

                if self.prossimo >= self.punti.len() {
                    if self.loop_forever {
                        self.ricomincia(now);
                    } else {
                        self.esaurita = true;
                    }
                }

                return Some(punto.coordinate);
            }
        }

        // Nessun punto nuovo e' scaduto: il veicolo e' fermo dove l'abbiamo lasciato.
        // Prima ancora del primo punto vale la sua coordinata, che e' la posizione di
        // partenza del percorso.
        self.ultima
            .or_else(|| self.punti.first().map(|punto| punto.coordinate))
    }

    fn name(&self) -> &'static str {
        "file"
    }
}

/// Estrae i punti validi dal contenuto del CSV, scartando le righe malformate.
///
/// `percorso` compare solo nei messaggi di diagnostica, per distinguere i warn di file
/// diversi.
fn analizza(contenuto: &str, percorso: &Path) -> Vec<Punto> {
    let mut punti = Vec::new();

    for (indice, riga) in contenuto.lines().enumerate() {
        // I numeri di riga dei messaggi sono quelli che si vedono in un editor.
        let numero = indice + 1;

        if indice == 0 {
            if riga.trim() != HEADER_ATTESO {
                tracing::warn!(
                    percorso = %percorso.display(),
                    riga = numero,
                    trovato = riga.trim(),
                    "intestazione inattesa, saltata comunque: atteso `{HEADER_ATTESO}`"
                );
            }
            continue;
        }

        if riga.trim().is_empty() {
            continue;
        }

        match analizza_riga(riga) {
            Ok(punto) => punti.push(punto),
            Err(motivo) => tracing::warn!(
                percorso = %percorso.display(),
                riga = numero,
                "riga scartata: {motivo}"
            ),
        }
    }

    punti
}

/// Interpreta una riga di dati del CSV.
///
/// # Errori
///
/// La descrizione di cio' che non va, gia' pronta per il log. Non riporta la riga intera
/// per non riversare nei log un file di input arbitrariamente lungo: il numero di riga
/// basta a ritrovarla.
fn analizza_riga(riga: &str) -> Result<Punto, String> {
    let mut colonne = riga.split(',');
    let (Some(lat), Some(lon), Some(offset)) = (colonne.next(), colonne.next(), colonne.next())
    else {
        return Err(format!("attese almeno {COLONNE_ATTESE} colonne"));
    };

    let lat: f64 = lat
        .trim()
        .parse()
        .map_err(|_| "latitudine non numerica".to_owned())?;
    let lon: f64 = lon
        .trim()
        .parse()
        .map_err(|_| "longitudine non numerica".to_owned())?;
    let coordinate = Coordinate::new(lat, lon).map_err(|errore| errore.to_string())?;

    let secondi: i64 = offset
        .trim()
        .parse()
        .map_err(|_| "offset_secs non e' un intero di secondi".to_owned())?;
    if secondi < 0 {
        return Err(format!("offset_secs negativo ({secondi})"));
    }
    let offset = TimeDelta::try_seconds(secondi)
        .ok_or_else(|| format!("offset_secs fuori intervallo ({secondi})"))?;

    Ok(Punto { coordinate, offset })
}

/// Sorgente che vaga a caso attorno a un punto di partenza, senza mai finire.
///
/// A ogni chiamata sceglie una distanza in `[0, passo_massimo]` e una direzione in
/// `[0, 360)` gradi e si sposta di conseguenza, rifiutando gli spostamenti che
/// porterebbero oltre `raggio_massimo` metri dalla partenza. Serve a far girare la demo
/// senza un percorso registrato: il veicolo resta nella zona che gli e' stata assegnata
/// e si muove in modo abbastanza irregolare da esercitare la logica di stato del server.
///
/// Il generatore e' [`StdRng`] seminato con [`StdRng::seed_from_u64`] e non quello di
/// sistema usato in `auth.rs`: li' serviva imprevedibilita' crittografica, qui serve il
/// contrario, ossia che due esecuzioni con lo stesso seme producano lo stesso cammino.
/// Una demo si puo' cosi' ripetere identica, e i test possono verificare proprieta' su
/// migliaia di passi. `StdRng` non e' portabile fra versioni di `rand`: il seme fissa la
/// sequenza a parita' di dipendenze, non per sempre, e nessun test si appoggia a valori
/// numerici attesi.
#[derive(Debug)]
pub struct RandomWalkSource {
    /// Centro dell'area consentita.
    partenza: Coordinate,
    /// Posizione corrente del veicolo.
    corrente: Coordinate,
    /// Distanza massima dalla partenza, in metri.
    raggio_massimo_m: f64,
    /// Spostamento massimo per chiamata, in metri.
    passo_massimo_m: f64,
    /// Generatore deterministico del cammino.
    rng: StdRng,
}

impl RandomWalkSource {
    /// Costruisce il cammino casuale attorno a `partenza`.
    ///
    /// `raggio_massimo_m` e `passo_massimo_m` sono normalizzati a zero se negativi o non
    /// finiti, con un warn: sono parametri di configurazione, e un valore assurdo deve
    /// produrre un veicolo fermo e un messaggio nel log, non una posizione priva di
    /// senso. Con raggio nullo, o passo nullo, il veicolo resta sulla coordinata di
    /// partenza.
    pub fn new(
        partenza: Coordinate,
        raggio_massimo_m: f64,
        passo_massimo_m: f64,
        seed: u64,
    ) -> Self {
        Self {
            partenza,
            corrente: partenza,
            raggio_massimo_m: normalizza_metri(raggio_massimo_m, "raggio_massimo_m"),
            passo_massimo_m: normalizza_metri(passo_massimo_m, "passo_massimo_m"),
            rng: StdRng::seed_from_u64(seed),
        }
    }
}

impl PositionSource for RandomWalkSource {
    /// Sposta il veicolo di un passo casuale e restituisce la nuova posizione.
    ///
    /// Non restituisce mai `None`: e' una simulazione senza fine, e l'istante `now` non
    /// la influenza (il passo dipende dal numero di chiamate, non dal tempo trascorso).
    ///
    /// La distanza si estrae una volta sola, la direzione a ogni tentativo: se il punto
    /// che ne esce cade oltre il raggio massimo si riprova con un'altra direzione, fino
    /// a [`TENTATIVI_MASSIMI`] volte. Esaurite le prove il veicolo resta dov'e' per
    /// questo giro, cosi' la chiamata termina sempre.
    fn next_position(&mut self, _now: DateTime<Utc>) -> Option<Coordinate> {
        // `random::<f64>()` estrae in `[0, 1)` e non puo' fallire su nessun valore del
        // passo, nemmeno zero: `random_range` andrebbe invece in panico su un intervallo
        // vuoto, e qui i panici sono vietati.
        let distanza_m = self.rng.random::<f64>() * self.passo_massimo_m;
        if distanza_m <= 0.0 {
            return Some(self.corrente);
        }

        for _ in 0..TENTATIVI_MASSIMI {
            let direzione_gradi = self.rng.random::<f64>() * 360.0;
            if let Some(candidato) = destinazione(self.corrente, distanza_m, direzione_gradi) {
                let dalla_partenza =
                    georuggine_common::types::haversine_meters(self.partenza, candidato);
                if dalla_partenza <= self.raggio_massimo_m {
                    self.corrente = candidato;
                    return Some(candidato);
                }
            }
        }

        Some(self.corrente)
    }

    fn name(&self) -> &'static str {
        "random-walk"
    }
}

/// Riporta a zero un parametro in metri negativo o non finito, segnalandolo.
fn normalizza_metri(valore: f64, nome: &'static str) -> f64 {
    if valore.is_finite() && valore >= 0.0 {
        valore
    } else {
        tracing::warn!(
            parametro = nome,
            valore,
            "valore non valido, trattato come 0"
        );
        0.0
    }
}

/// Punto che si raggiunge partendo da `origine` e percorrendo `distanza_m` metri lungo
/// la direzione `direzione_gradi` (0 = nord, 90 = est).
///
/// Usa la formula del punto di destinazione su sfera, la cui inversa e' esattamente
/// l'emisenoverso di `georuggine_common::types::haversine_meters`: applicando questa e
/// poi misurando con quella si riottiene `distanza_m`, ed e' proprio cio' su cui si
/// appoggia il controllo del raggio massimo di [`RandomWalkSource`]. La scelta e' quindi
/// per coerenza, non per precisione assoluta: rispetto all'ellissoide l'errore resta
/// sotto lo 0,5%, del tutto irrilevante su spostamenti di decine di metri.
///
/// L'alternativa piatta (sommare `distanza / 111320` ai gradi di latitudine, con la
/// correzione in coseno sulla longitudine) sarebbe accurata quanto basta a corto raggio,
/// ma degenera vicino ai poli e va gestita a mano sull'antimeridiano, mentre questa
/// formula e' definita ovunque; l'unico prezzo sono poche chiamate trigonometriche per
/// passo.
///
/// Restituisce `None` solo se il risultato non fosse una coordinata valida, cosa che la
/// formula non produce: il caso e' gestito senza panico invece che dato per impossibile.
fn destinazione(origine: Coordinate, distanza_m: f64, direzione_gradi: f64) -> Option<Coordinate> {
    // Distanza percorsa espressa come angolo al centro della sfera.
    let delta = distanza_m / EARTH_RADIUS_METERS;
    let direzione = direzione_gradi.to_radians();
    let lat = origine.lat().to_radians();
    let lon = origine.lon().to_radians();

    let sin_lat_arrivo = lat.sin() * delta.cos() + lat.cos() * delta.sin() * direzione.cos();
    let lat_arrivo = sin_lat_arrivo.asin();
    let lon_arrivo = lon
        + (direzione.sin() * delta.sin() * lat.cos())
            .atan2(delta.cos() - lat.sin() * sin_lat_arrivo);

    Coordinate::new(
        lat_arrivo.to_degrees(),
        normalizza_longitudine(lon_arrivo.to_degrees()),
    )
    .ok()
}

/// Riporta una longitudine in `[-180, 180)`, anche dopo aver scavalcato l'antimeridiano.
///
/// `rem_euclid` da' sempre un resto non negativo, quindi la formula vale anche per le
/// longitudini negative, dove il `%` di Rust restituirebbe un valore negativo.
fn normalizza_longitudine(gradi: f64) -> f64 {
    (gradi + 180.0).rem_euclid(360.0) - 180.0
}

/// Strategia di simulazione scelta dalla configurazione del client.
///
/// E' il punto in cui la configurazione diventa codice: le varianti descrivono i
/// parametri, [`SourceKind::build`] costruisce l'oggetto. Le due strategie che mancano
/// alla traccia (percorso interpolato e inserimento manuale) si aggiungono qui come
/// nuove varianti, senza toccare chi consuma le posizioni.
#[derive(Debug, Clone, PartialEq)]
pub enum SourceKind {
    /// Percorso registrato su file CSV, riprodotto da [`FileSource`].
    File {
        /// Percorso del file CSV.
        percorso: PathBuf,
        /// Se vero il percorso riparte da capo invece di finire.
        loop_forever: bool,
    },
    /// Cammino casuale attorno a un punto, prodotto da [`RandomWalkSource`].
    RandomWalk {
        /// Punto di partenza e centro dell'area consentita.
        partenza: Coordinate,
        /// Distanza massima dalla partenza, in metri.
        raggio_massimo_m: f64,
        /// Spostamento massimo per chiamata, in metri.
        passo_massimo_m: f64,
        /// Seme del generatore: a parita' di seme il cammino si ripete identico.
        seed: u64,
    },
}

impl SourceKind {
    /// Costruisce la sorgente descritta da questa variante.
    ///
    /// # Errori
    ///
    /// Gli errori di [`FileSource::from_path`], che qui non vengono assorbiti: un
    /// percorso illeggibile o senza punti deve fermare l'avvio del client, non
    /// trasformarsi in un veicolo che non si muove. [`SourceKind::RandomWalk`] non puo'
    /// fallire e restituisce sempre `Ok`.
    pub fn build(&self) -> io::Result<Box<dyn PositionSource>> {
        match self {
            Self::File {
                percorso,
                loop_forever,
            } => Ok(Box::new(FileSource::from_path(percorso, *loop_forever)?)),
            Self::RandomWalk {
                partenza,
                raggio_massimo_m,
                passo_massimo_m,
                seed,
            } => Ok(Box::new(RandomWalkSource::new(
                *partenza,
                *raggio_massimo_m,
                *passo_massimo_m,
                *seed,
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::time::{SystemTime, UNIX_EPOCH};

    use georuggine_common::types::haversine_meters;

    /// Costruisce una coordinata che il test da' per valida.
    fn coord(lat: f64, lon: f64) -> Coordinate {
        Coordinate::new(lat, lon).expect("coordinata di test valida")
    }

    /// Istante di riferimento dei test, piu' `secondi`.
    ///
    /// Le sorgenti non guardano mai l'orologio da sole: ricevono `now` dal chiamante,
    /// quindi i test possono far scorrere il tempo a passi esatti.
    fn a(secondi: i64) -> DateTime<Utc> {
        let base = DateTime::from_timestamp(1_704_067_200, 0).expect("istante valido");
        base + TimeDelta::try_seconds(secondi).expect("scarto valido")
    }

    /// Punto di partenza dei test sul cammino casuale: Piazza Castello, Torino.
    fn torino() -> Coordinate {
        coord(45.0703, 7.6869)
    }

    /// File CSV di prova, cancellato quando il test finisce.
    struct FileDiProva {
        /// Percorso del file creato.
        percorso: PathBuf,
    }

    impl FileDiProva {
        /// Scrive `contenuto` in un file temporaneo unico per esecuzione.
        fn nuovo(nome: &str, contenuto: &str) -> Self {
            let percorso = percorso_temporaneo(nome);
            fs::write(&percorso, contenuto).expect("scrittura del file di prova riuscita");
            Self { percorso }
        }

        /// Percorso del file scritto.
        fn percorso(&self) -> &Path {
            &self.percorso
        }
    }

    impl Drop for FileDiProva {
        fn drop(&mut self) {
            let _ = fs::remove_file(&self.percorso);
        }
    }

    /// Percorso di un file di prova nella cartella temporanea, unico per esecuzione.
    fn percorso_temporaneo(nome: &str) -> PathBuf {
        let unico = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        std::env::temp_dir().join(format!(
            "georuggine-sources-{nome}-{}-{unico}.csv",
            std::process::id()
        ))
    }

    // --- FileSource: sequenza di base ---

    #[test]
    fn tre_righe_valide_escono_in_ordine_ai_tempi_giusti() {
        let file = FileDiProva::nuovo(
            "ordine",
            "lat,lon,offset_secs\n45.07,7.68,0\n45.08,7.69,60\n45.09,7.70,120\n",
        );
        let mut sorgente = FileSource::from_path(file.percorso(), false).expect("file valido");

        assert_eq!(sorgente.next_position(a(0)), Some(coord(45.07, 7.68)));
        assert_eq!(sorgente.next_position(a(60)), Some(coord(45.08, 7.69)));
        assert_eq!(sorgente.next_position(a(120)), Some(coord(45.09, 7.70)));
        assert_eq!(
            sorgente.next_position(a(180)),
            None,
            "dopo l'ultima riga la simulazione e' finita"
        );
        assert_eq!(
            sorgente.next_position(a(240)),
            None,
            "e resta finita a ogni chiamata successiva"
        );
    }

    #[test]
    fn il_tempo_zero_e_la_prima_chiamata_non_la_costruzione() {
        let file = FileDiProva::nuovo(
            "tempo-zero",
            "lat,lon,offset_secs\n45.07,7.68,0\n45.08,7.69,60\n",
        );
        let mut sorgente = FileSource::from_path(file.percorso(), false).expect("file valido");

        // La sorgente viene usata mille secondi dopo la costruzione: gli offset si
        // contano da qui, altrimenti tutte le righe risulterebbero gia' scadute.
        assert_eq!(sorgente.next_position(a(1_000)), Some(coord(45.07, 7.68)));
        assert_eq!(
            sorgente.next_position(a(1_030)),
            Some(coord(45.07, 7.68)),
            "la seconda riga non e' ancora scaduta: si ripete l'ultima coordinata"
        );
        assert_eq!(sorgente.next_position(a(1_060)), Some(coord(45.08, 7.69)));
    }

    #[test]
    fn prima_che_la_riga_zero_scada_restituisce_la_prima_coordinata() {
        let file = FileDiProva::nuovo(
            "attesa-iniziale",
            "lat,lon,offset_secs\n45.07,7.68,30\n45.08,7.69,60\n",
        );
        let mut sorgente = FileSource::from_path(file.percorso(), false).expect("file valido");

        assert_eq!(
            sorgente.next_position(a(0)),
            Some(coord(45.07, 7.68)),
            "il veicolo e' fermo sulla posizione iniziale finche' la prima riga non scade"
        );
        assert_eq!(sorgente.next_position(a(30)), Some(coord(45.07, 7.68)));
        assert_eq!(sorgente.next_position(a(60)), Some(coord(45.08, 7.69)));
    }

    #[test]
    fn piu_righe_scadute_escono_una_alla_volta_senza_saltarne() {
        let file = FileDiProva::nuovo(
            "recupero",
            "lat,lon,offset_secs\n45.07,7.68,0\n45.08,7.69,10\n45.09,7.70,20\n",
        );
        let mut sorgente = FileSource::from_path(file.percorso(), false).expect("file valido");

        assert_eq!(sorgente.next_position(a(0)), Some(coord(45.07, 7.68)));
        // Il chiamante torna molto in ritardo: le righe arretrate non vengono saltate,
        // escono una per chiamata e nell'ordine del file.
        assert_eq!(sorgente.next_position(a(100)), Some(coord(45.08, 7.69)));
        assert_eq!(sorgente.next_position(a(100)), Some(coord(45.09, 7.70)));
        assert_eq!(sorgente.next_position(a(100)), None);
    }

    // --- FileSource: righe malformate ---

    #[test]
    fn una_riga_malformata_non_impedisce_la_lettura_delle_altre() {
        let file = FileDiProva::nuovo(
            "riga-rotta",
            "lat,lon,offset_secs\n45.07,7.68,0\nqui,non,ci_sono,numeri\n45.09,7.70,60\n",
        );
        let mut sorgente =
            FileSource::from_path(file.percorso(), false).expect("una riga rotta non fa fallire");

        assert_eq!(sorgente.next_position(a(0)), Some(coord(45.07, 7.68)));
        assert_eq!(sorgente.next_position(a(60)), Some(coord(45.09, 7.70)));
        assert_eq!(sorgente.next_position(a(120)), None);
    }

    #[test]
    fn ogni_forma_di_riga_malformata_viene_scartata() {
        let file = FileDiProva::nuovo(
            "forme-rotte",
            concat!(
                "lat,lon,offset_secs\n",
                "45.07,7.68\n",    // meno di tre colonne
                "abc,7.68,10\n",   // latitudine non numerica
                "45.07,xyz,10\n",  // longitudine non numerica
                "91.0,7.68,10\n",  // latitudine fuori intervallo
                "45.07,7.68,-1\n", // offset negativo
                "45.07,7.68,x\n",  // offset non numerico
                "45.09,7.70,0\n",  // unica riga valida
            ),
        );
        let mut sorgente =
            FileSource::from_path(file.percorso(), false).expect("una riga e' buona");

        assert_eq!(sorgente.next_position(a(0)), Some(coord(45.09, 7.70)));
        assert_eq!(sorgente.next_position(a(1)), None);
    }

    #[test]
    fn le_righe_vuote_non_contano_come_malformate() {
        let file = FileDiProva::nuovo(
            "righe-vuote",
            "lat,lon,offset_secs\n\n45.07,7.68,0\n   \n45.08,7.69,60\n\n",
        );
        let mut sorgente = FileSource::from_path(file.percorso(), false).expect("file valido");

        assert_eq!(sorgente.next_position(a(0)), Some(coord(45.07, 7.68)));
        assert_eq!(sorgente.next_position(a(60)), Some(coord(45.08, 7.69)));
    }

    // --- FileSource: file senza punti utilizzabili ---

    #[test]
    fn un_file_con_solo_l_header_e_un_errore_di_costruzione() {
        let file = FileDiProva::nuovo("solo-header", "lat,lon,offset_secs\n");

        let errore = FileSource::from_path(file.percorso(), false)
            .expect_err("una sorgente senza punti non e' una simulazione");
        assert_eq!(errore.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn un_file_di_sole_righe_malformate_e_un_errore_di_costruzione() {
        let file = FileDiProva::nuovo("tutto-rotto", "lat,lon,offset_secs\nabc,def,ghi\n1,2\n\n");

        let errore = FileSource::from_path(file.percorso(), false)
            .expect_err("nessuna riga valida: errore alla costruzione");
        assert_eq!(errore.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn un_file_completamente_vuoto_e_un_errore_di_costruzione() {
        let file = FileDiProva::nuovo("vuoto", "");

        let errore =
            FileSource::from_path(file.percorso(), false).expect_err("un file vuoto non ha punti");
        assert_eq!(errore.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn un_file_mancante_e_un_errore_di_costruzione() {
        let percorso = percorso_temporaneo("mai-creato");

        let errore = FileSource::from_path(&percorso, false)
            .expect_err("il file non esiste e lo si scopre subito");
        assert_eq!(errore.kind(), io::ErrorKind::NotFound);
    }

    // --- FileSource: ripetizione ciclica ---

    #[test]
    fn loop_forever_ricomincia_con_un_nuovo_tempo_zero() {
        let file = FileDiProva::nuovo(
            "ciclo",
            "lat,lon,offset_secs\n45.07,7.68,0\n45.08,7.69,60\n",
        );
        let mut sorgente = FileSource::from_path(file.percorso(), true).expect("file valido");

        // Primo giro: tempo zero fissato ad a(0).
        assert_eq!(sorgente.next_position(a(0)), Some(coord(45.07, 7.68)));
        assert_eq!(sorgente.next_position(a(60)), Some(coord(45.08, 7.69)));
        // L'ultima riga ha ridefinito il tempo zero ad a(60): il secondo giro riparte
        // dalla prima riga, il cui offset e' zero.
        assert_eq!(
            sorgente.next_position(a(90)),
            Some(coord(45.07, 7.68)),
            "secondo giro: si ricomincia dalla prima riga"
        );
        assert_eq!(sorgente.next_position(a(120)), Some(coord(45.08, 7.69)));
        // Terzo giro: il ciclo e' ripetibile, non solo una seconda passata.
        assert_eq!(
            sorgente.next_position(a(150)),
            Some(coord(45.07, 7.68)),
            "terzo giro: il ciclo e' stabile"
        );
        assert_eq!(sorgente.next_position(a(180)), Some(coord(45.08, 7.69)));
    }

    #[test]
    fn loop_forever_non_restituisce_mai_none() {
        let file = FileDiProva::nuovo("ciclo-infinito", "lat,lon,offset_secs\n45.07,7.68,0\n");
        let mut sorgente = FileSource::from_path(file.percorso(), true).expect("file valido");

        for secondo in 0..500 {
            assert!(
                sorgente.next_position(a(secondo)).is_some(),
                "una sorgente ciclica non finisce mai (secondo {secondo})"
            );
        }
    }

    // --- RandomWalkSource ---

    #[test]
    fn stesso_seed_stessa_sequenza() {
        let mut prima = RandomWalkSource::new(torino(), 500.0, 20.0, 42);
        let mut seconda = RandomWalkSource::new(torino(), 500.0, 20.0, 42);

        for passo in 0..500 {
            assert_eq!(
                prima.next_position(a(passo)),
                seconda.next_position(a(passo)),
                "due istanze con lo stesso seme devono coincidere al passo {passo}"
            );
        }
    }

    #[test]
    fn semi_diversi_danno_sequenze_diverse() {
        let mut prima = RandomWalkSource::new(torino(), 500.0, 20.0, 1);
        let mut seconda = RandomWalkSource::new(torino(), 500.0, 20.0, 2);

        let una: Vec<_> = (0..20).map(|i| prima.next_position(a(i))).collect();
        let altra: Vec<_> = (0..20).map(|i| seconda.next_position(a(i))).collect();

        assert_ne!(una, altra, "semi diversi devono produrre cammini diversi");
    }

    #[test]
    fn non_esce_mai_dal_raggio_massimo_su_mille_chiamate() {
        let raggio = 100.0;
        let mut sorgente = RandomWalkSource::new(torino(), raggio, 30.0, 7);

        for chiamata in 0..1_000 {
            let posizione = sorgente
                .next_position(a(chiamata))
                .expect("il cammino casuale non finisce mai");
            let distanza = haversine_meters(torino(), posizione);
            assert!(
                distanza <= raggio,
                "chiamata {chiamata}: {distanza} m dalla partenza, oltre il raggio di {raggio} m"
            );
        }
    }

    #[test]
    fn nessuno_spostamento_supera_il_passo_massimo() {
        let passo = 30.0;
        let mut sorgente = RandomWalkSource::new(torino(), 5_000.0, passo, 11);
        let mut precedente = torino();

        for chiamata in 0..500 {
            let posizione = sorgente
                .next_position(a(chiamata))
                .expect("il cammino casuale non finisce mai");
            let spostamento = haversine_meters(precedente, posizione);
            assert!(
                spostamento <= passo + 1e-6,
                "chiamata {chiamata}: spostamento di {spostamento} m, oltre il passo di {passo} m"
            );
            precedente = posizione;
        }
    }

    #[test]
    fn passo_nullo_resta_sempre_sulla_stessa_coordinata() {
        let mut sorgente = RandomWalkSource::new(torino(), 100.0, 0.0, 3);

        for chiamata in 0..1_000 {
            assert_eq!(
                sorgente.next_position(a(chiamata)),
                Some(torino()),
                "con passo nullo la posizione non cambia (chiamata {chiamata})"
            );
        }
    }

    #[test]
    fn raggio_nullo_lascia_il_veicolo_fermo_senza_bloccarsi() {
        // Ogni tentativo uscirebbe dal raggio: la sorgente deve arrendersi dopo un numero
        // limitato di prove e restituire la posizione corrente, non ciclare all'infinito.
        let mut sorgente = RandomWalkSource::new(torino(), 0.0, 50.0, 3);

        for chiamata in 0..200 {
            assert_eq!(
                sorgente.next_position(a(chiamata)),
                Some(torino()),
                "con raggio nullo il veicolo resta fermo (chiamata {chiamata})"
            );
        }
    }

    #[test]
    fn parametri_negativi_diventano_un_veicolo_fermo() {
        let mut sorgente = RandomWalkSource::new(torino(), -10.0, f64::NAN, 3);

        assert_eq!(sorgente.next_position(a(0)), Some(torino()));
    }

    #[test]
    fn il_cammino_casuale_si_muove_davvero() {
        let mut sorgente = RandomWalkSource::new(torino(), 500.0, 20.0, 5);
        let posizioni: Vec<_> = (0..50)
            .filter_map(|i| sorgente.next_position(a(i)))
            .collect();

        assert!(
            posizioni.iter().any(|p| *p != torino()),
            "con passo e raggio non nulli il veicolo deve spostarsi"
        );
    }

    // --- geometria ---

    #[test]
    fn destinazione_e_l_inversa_dell_emisenoverso() {
        // La distanza misurata sul punto costruito deve tornare quella richiesta: e'
        // l'invariante su cui si appoggia il controllo del raggio massimo.
        for direzione in [0.0, 45.0, 90.0, 180.0, 270.0, 359.9] {
            let arrivo = destinazione(torino(), 250.0, direzione).expect("punto valido");
            let misurata = haversine_meters(torino(), arrivo);
            assert!(
                (misurata - 250.0).abs() < 0.01,
                "direzione {direzione}: attesi 250 m, misurati {misurata} m"
            );
        }
    }

    #[test]
    fn destinazione_verso_nord_alza_la_latitudine() {
        let arrivo = destinazione(torino(), 1_000.0, 0.0).expect("punto valido");

        assert!(
            arrivo.lat() > torino().lat(),
            "verso nord la latitudine sale"
        );
        assert!(
            (arrivo.lon() - torino().lon()).abs() < 1e-9,
            "verso nord la longitudine non cambia"
        );
    }

    #[test]
    fn la_longitudine_resta_in_intervallo_oltre_l_antimeridiano() {
        let sul_bordo = coord(0.0, 179.999_9);
        let arrivo = destinazione(sul_bordo, 1_000.0, 90.0).expect("punto valido");

        assert!(
            (-180.0..180.0).contains(&arrivo.lon()),
            "longitudine fuori intervallo: {}",
            arrivo.lon()
        );
        assert!(
            arrivo.lon() < 0.0,
            "scavalcando l'antimeridiano la longitudine diventa negativa: {}",
            arrivo.lon()
        );
    }

    // --- SourceKind ---

    #[test]
    fn la_factory_costruisce_la_sorgente_corrispondente() {
        let file = FileDiProva::nuovo("factory", "lat,lon,offset_secs\n45.07,7.68,0\n");

        let da_file = SourceKind::File {
            percorso: file.percorso().to_path_buf(),
            loop_forever: false,
        }
        .build()
        .expect("costruzione riuscita");
        assert_eq!(da_file.name(), "file");

        let casuale = SourceKind::RandomWalk {
            partenza: torino(),
            raggio_massimo_m: 500.0,
            passo_massimo_m: 20.0,
            seed: 42,
        }
        .build()
        .expect("il cammino casuale non puo' fallire");
        assert_eq!(casuale.name(), "random-walk");
    }

    #[test]
    fn la_factory_propaga_l_errore_del_file() {
        let esito = SourceKind::File {
            percorso: percorso_temporaneo("factory-mancante"),
            loop_forever: false,
        }
        .build();

        assert!(
            esito.is_err(),
            "un file illeggibile deve emergere dalla factory, non dopo"
        );
    }

    // --- percorso di esempio ---

    #[test]
    fn il_percorso_demo_e_valido_e_contiene_una_pausa() {
        let percorso = Path::new(env!("CARGO_MANIFEST_DIR")).join("assets/percorso_demo.csv");
        let mut sorgente =
            FileSource::from_path(&percorso, false).expect("il percorso demo deve caricarsi");

        // Il client campiona la sorgente a intervalli regolari: si simula la stessa cosa.
        const PASSO_SECS: i64 = 30;
        let mut letture: Vec<(i64, Coordinate)> = Vec::new();
        let mut istante = 0;
        while let Some(posizione) = sorgente.next_position(a(istante)) {
            letture.push((istante, posizione));
            istante += PASSO_SECS;
            assert!(istante < 7_200, "il percorso demo non termina");
        }

        assert!(
            letture.len() >= 20,
            "attesi almeno 20 punti, letti {}",
            letture.len()
        );

        let inizio = letture
            .first()
            .map(|(_, p)| *p)
            .expect("almeno una lettura");
        let fine = letture.last().map(|(_, p)| *p).expect("almeno una lettura");
        assert!(
            haversine_meters(inizio, fine) > 500.0,
            "il percorso demo deve coprire una distanza plausibile"
        );

        // La pausa: la coordinata non cambia per almeno quattro minuti di fila.
        let mut pausa_massima = 0;
        let mut inizio_pausa = 0;
        for coppia in letture.windows(2) {
            if let [(_, precedente), (istante, corrente)] = coppia {
                if precedente != corrente {
                    inizio_pausa = *istante;
                }
                pausa_massima = pausa_massima.max(istante - inizio_pausa);
            }
        }
        assert!(
            pausa_massima >= 240,
            "attesa una pausa di almeno 4 minuti, la piu' lunga dura {pausa_massima} s"
        );
    }
}
