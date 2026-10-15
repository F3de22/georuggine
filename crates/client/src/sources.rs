//! Sorgenti di posizione del veicolo: le strategie con cui il client simula il proprio
//! movimento.
//!
//! Il client non ha un GPS: la posizione da inviare al server arriva da una
//! [`PositionSource`], che il resto del programma usa dietro il trait senza sapere quale
//! strategia ci sia dietro. Le quattro previste dalla traccia sono [`FileSource`],
//! [`RandomWalkSource`], [`RouteSource`] e [`ManualSource`]; la configurazione le sceglie
//! attraverso le varianti di [`SourceKind`].
//!
//! Come per la logica di dominio del server, nessuna sorgente chiama `Utc::now()`: il
//! tempo entra come parametro di [`PositionSource::next_position`], cosi' i test possono
//! far scorrere ore in pochi microsecondi.
//!
//! Le posizioni intermedie di [`RouteSource`] si costruiscono con [`destinazione`],
//! camminando lungo la geodetica. La scorciatoia consueta — la media pesata dei gradi di
//! latitudine e longitudine — sembra equivalente ma non lo e': i meridiani si stringono
//! verso i poli, quindi il punto medio dei gradi non e' il punto medio del cammino, e su
//! una rotta lunga la velocita' apparente non risulterebbe piu' costante.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::mpsc::{self, Receiver, Sender};
use std::time::Duration;

use chrono::{DateTime, TimeDelta, Utc};
use georuggine_common::types::Coordinate;
use rand::rngs::StdRng;
use rand::{RngExt as _, SeedableRng as _};

/// Raggio medio terrestre in metri.
///
/// E' lo stesso valore che `haversine_meters` usa in `common`, e deve restarlo:
/// [`destinazione`] e la funzione di distanza sono l'una l'inversa dell'altra solo se
/// lavorano sulla stessa sfera, altrimenti il controllo sul raggio massimo di
/// [`RandomWalkSource`] accetterebbe punti appena fuori. La costante e' privata in
/// `common`, quindi va ripetuta qui.
const EARTH_RADIUS_METERS: f64 = 6_371_008.8;

/// Numero di colonne attese in una riga del CSV di [`FileSource`].
const COLONNE_ATTESE: usize = 3;

/// Intestazione attesa come prima riga del CSV di [`FileSource`].
const HEADER_ATTESO: &str = "lat,lon,offset_secs";

/// Quante direzioni diverse prova [`RandomWalkSource`] prima di rinunciare a muoversi.
///
/// Garantisce che ogni chiamata termini. Il limite si raggiunge solo con un veicolo
/// incollato al bordo del raggio massimo, dove quasi tutte le direzioni portano fuori.
const TENTATIVI_MASSIMI: u8 = 16;

/// Raggio di tolleranza, in metri, entro cui [`RouteSource`] considera il veicolo su uno
/// degli estremi del tragitto.
///
/// Il tempo di arrivo esce da una divisione e non cade quasi mai su un valore esatto in
/// virgola mobile: senza tolleranza l'ultima posizione sarebbe un punto interpolato a
/// frazioni di millimetro dalla destinazione, e il server ne ricaverebbe uno spostamento
/// residuo che non e' mai avvenuto.
const TOLLERANZA_ESTREMI_M: f64 = 0.001;

/// Sorgente da cui il client legge la posizione da inviare al server.
///
/// Il trait richiede [`Send`] perche' la sorgente vive nel task che invia le posizioni, e
/// tokio puo' spostare quel task fra i thread dell'esecutore.
pub trait PositionSource: Send {
    /// Posizione da inviare nell'istante `now`, oppure `None` a simulazione finita.
    ///
    /// `None` non e' un errore: significa che la sorgente ha esaurito quello che aveva da
    /// riprodurre. Una sorgente che ha ancora qualcosa da dire ma nulla di *nuovo* in
    /// questo istante ripete invece l'ultima posizione nota, che e' esattamente cio' che
    /// farebbe un GPS su un veicolo fermo.
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
/// `offset_secs` e' il ritardo in secondi rispetto al **tempo zero** della sorgente: il
/// primo istante passato a [`PositionSource::next_position`], non quello di costruzione,
/// perche' fra il caricamento del file e la prima posizione inviata passano la connessione
/// al server e l'autenticazione.
///
/// Ogni chiamata fa avanzare il percorso di al massimo un punto: se il chiamante torna in
/// ritardo e piu' righe sono nel frattempo scadute, escono una alla volta e nell'ordine
/// del file. Quando non c'e' nessun punto nuovo la sorgente ripete l'ultima coordinata
/// emessa, ed e' cosi' che un percorso puo' contenere una pausa.
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
    /// La lettura avviene subito e non alla prima chiamata: un file mancante o senza punti
    /// utilizzabili si scopre all'avvio del client, quando c'e' ancora qualcuno che legge
    /// l'errore, e non mezz'ora dopo sotto forma di veicolo che non si muove. E' sincrona
    /// e bloccante, quindi va chiamata in fase di avvio o dentro `spawn_blocking`.
    ///
    /// Le righe malformate non fanno fallire il caricamento: vengono scartate con un warn
    /// che ne riporta il numero di riga. Sono malformate quelle con meno di
    /// [`COLONNE_ATTESE`] colonne, con coordinate non numeriche o fuori intervallo e con
    /// offset non intero o negativo; le colonne oltre la terza si ignorano, le righe vuote
    /// si saltano e la prima riga e' sempre l'intestazione.
    ///
    /// # Errori
    ///
    /// - l'errore di `std::fs` se il file non esiste o non e' leggibile;
    /// - [`io::ErrorKind::InvalidData`] se dopo lo scarto non resta nessun punto: una
    ///   sorgente con zero posizioni e' un errore di configurazione, non una simulazione.
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
    /// Consegnato l'ultimo punto, con `loop_forever` il tempo zero si sposta su `now` e il
    /// percorso riparte (il periodo del ciclo e' cosi' l'offset dell'ultimo punto, stabile
    /// giro dopo giro); senza, la sorgente e' esaurita e restituisce sempre `None`.
    fn next_position(&mut self, now: DateTime<Utc>) -> Option<Coordinate> {
        if self.esaurita {
            return None;
        }

        let zero = *self.zero.get_or_insert(now);

        if let Some(punto) = self.punti.get(self.prossimo).copied()
            && now.signed_duration_since(zero) >= punto.offset
        {
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

        // Nessun punto nuovo e' scaduto: il veicolo e' fermo dove l'abbiamo lasciato.
        // Prima ancora del primo punto vale la sua coordinata.
        self.ultima
            .or_else(|| self.punti.first().map(|punto| punto.coordinate))
    }

    fn name(&self) -> &'static str {
        "file"
    }
}

/// Estrae i punti validi dal contenuto del CSV, scartando le righe malformate.
///
/// `percorso` compare solo nei messaggi di diagnostica.
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
/// per non riversare nei log un input arbitrariamente lungo: il numero di riga basta a
/// ritrovarla.
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
/// `[0, 360)` gradi, rifiutando gli spostamenti che porterebbero oltre `raggio_massimo`
/// metri dalla partenza.
///
/// Il generatore e' [`StdRng`] seminato a mano e non quello di sistema usato in `auth.rs`:
/// li' serviva imprevedibilita' crittografica, qui serve il contrario, cioe' che due
/// esecuzioni con lo stesso seme producano lo stesso cammino. `StdRng` non e' pero'
/// portabile fra versioni di `rand`: il seme fissa la sequenza a parita' di dipendenze, e
/// nessun test si appoggia a valori numerici attesi.
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
    /// produrre un veicolo fermo e un messaggio nel log, non una posizione priva di senso.
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
    /// Non restituisce mai `None`, e l'istante `now` non la influenza: il passo dipende
    /// dal numero di chiamate, non dal tempo trascorso.
    ///
    /// La distanza si estrae una volta sola, la direzione a ogni tentativo: se il punto
    /// cade oltre il raggio massimo si riprova, fino a [`TENTATIVI_MASSIMI`] volte, dopo
    /// le quali il veicolo resta dov'e' per questo giro.
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

/// Punto che si raggiunge partendo da `origine` e percorrendo `distanza_m` metri lungo la
/// direzione `direzione_gradi` (0 = nord, 90 = est).
///
/// E' la formula del punto di destinazione su sfera, l'inversa esatta dell'emisenoverso di
/// `haversine_meters`: applicando questa e poi misurando con quella si riottiene
/// `distanza_m`, ed e' su questo che si appoggia il controllo del raggio massimo di
/// [`RandomWalkSource`]. L'alternativa piatta sarebbe accurata quanto basta a corto raggio
/// ma degenera vicino ai poli e va gestita a mano sull'antimeridiano.
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

/// Direzione iniziale, in gradi (0 = nord, 90 = est), della geodetica da `origine` ad
/// `arrivo`.
///
/// Un'origine e un azimut iniziale individuano un unico cerchio massimo, quindi camminare
/// `d` metri con [`destinazione`] lungo questa direzione da' il punto che sta a `d` metri
/// lungo la geodetica: la direzione si calcola una volta sola alla costruzione della
/// rotta, anche se l'azimut *vero* cambia strada facendo.
fn rotta_iniziale(origine: Coordinate, arrivo: Coordinate) -> f64 {
    let lat_origine = origine.lat().to_radians();
    let lat_arrivo = arrivo.lat().to_radians();
    let delta_lon = (arrivo.lon() - origine.lon()).to_radians();

    let y = delta_lon.sin() * lat_arrivo.cos();
    let x = lat_origine.cos() * lat_arrivo.sin()
        - lat_origine.sin() * lat_arrivo.cos() * delta_lon.cos();

    // `atan2` da' un angolo in `(-180, 180]`: gli azimut si contano da nord in senso
    // orario, quindi si riporta in `[0, 360)`.
    y.atan2(x).to_degrees().rem_euclid(360.0)
}

/// Sosta programmata lungo il tragitto di [`RouteSource`].
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Pausa {
    /// Punto del tragitto in cui la sosta avviene: `0.0` la partenza, `1.0` l'arrivo.
    ///
    /// I valori fuori da `[0, 1]` vengono riportati all'estremo piu' vicino e quelli non
    /// finiti scartati, in entrambi i casi con un warn.
    pub a_frazione: f64,
    /// Quanto il veicolo resta fermo in quel punto.
    pub durata: Duration,
}

/// Un tratto della linea del tempo di [`RouteSource`].
///
/// Movimento e sosta hanno la stessa forma: in una sosta le due frazioni coincidono,
/// quindi interpolare fra loro da' sempre lo stesso punto e non serve distinguere i due
/// casi al momento di calcolare la posizione.
#[derive(Debug, Clone, Copy)]
struct Tappa {
    /// Inizio del tratto, in secondi dal tempo zero della sorgente.
    inizio_s: f64,
    /// Fine del tratto, in secondi dal tempo zero della sorgente.
    fine_s: f64,
    /// Frazione di tragitto gia' percorsa all'inizio del tratto.
    da_frazione: f64,
    /// Frazione di tragitto percorsa alla fine del tratto.
    a_frazione: f64,
}

/// Sorgente che percorre una rotta fra due punti a velocita' costante, con soste.
///
/// Come in [`FileSource`] il tempo zero e' il primo istante passato a
/// [`PositionSource::next_position`]. Il tempo del movimento e quello delle soste non si
/// sovrappongono: durante una pausa il tragitto non avanza, quindi la durata complessiva
/// e' la somma del tempo di movimento e di tutte le pause.
///
/// Finita la rotta la sorgente non restituisce `None` ma ripete la coordinata di arrivo:
/// a differenza di un percorso registrato che si esaurisce, una rotta conclusa e' un
/// veicolo che esiste ancora e sta fermo a destinazione, ed e' cosi' che il server lo vede
/// passare a fermo.
#[derive(Debug)]
pub struct RouteSource {
    /// Punto di partenza, restituito finche' il tragitto non e' cominciato.
    partenza: Coordinate,
    /// Punto di arrivo, restituito tale e quale a tragitto finito.
    arrivo: Coordinate,
    /// Lunghezza del tragitto in metri, misurata alla costruzione.
    distanza_totale_m: f64,
    /// Azimut iniziale verso l'arrivo, in gradi: uno solo per tutto il tragitto.
    direzione_gradi: f64,
    /// Tratti in ordine di tempo e senza buchi: ognuno comincia dove finisce il
    /// precedente. Vuoto solo se il tragitto ha durata nulla.
    tappe: Vec<Tappa>,
    /// Durata dell'intero tragitto, pause comprese, in secondi.
    durata_totale_s: f64,
    /// Vero quando la velocita' non e' utilizzabile: il veicolo non parte mai.
    fermo: bool,
    /// Istante da cui si contano i tempi, fissato alla prima chiamata.
    zero: Option<DateTime<Utc>>,
}

impl RouteSource {
    /// Prepara la rotta da `partenza` ad `arrivo` a `velocita_kmh`, con le soste indicate.
    ///
    /// Le pause vengono ordinate per frazione crescente e intercalate ai tratti di
    /// movimento, cosi' che a ogni chiamata basti cercare il tratto in corso. **Due pause
    /// sulla stessa frazione diventano una sola sosta** lunga quanto la somma: tenerle
    /// distinte darebbe fra loro un tratto di movimento di durata nulla, e le due varianti
    /// sarebbero comunque indistinguibili da fuori.
    ///
    /// I parametri assurdi vengono normalizzati con un warn, perche' arrivano dalla
    /// configurazione: una `velocita_kmh` non positiva o non finita lascia il veicolo
    /// fermo alla partenza per sempre; una [`Pausa::a_frazione`] fuori da `[0, 1]` viene
    /// riportata all'estremo piu' vicino, e una non finita viene scartata.
    pub fn new(
        partenza: Coordinate,
        arrivo: Coordinate,
        velocita_kmh: f64,
        pause: Vec<Pausa>,
    ) -> Self {
        let fermo = !velocita_kmh.is_finite() || velocita_kmh <= 0.0;
        if fermo {
            tracing::warn!(
                velocita_kmh,
                "velocita' non utilizzabile: il veicolo resta fermo alla partenza"
            );
        }

        let distanza_totale_m = georuggine_common::types::haversine_meters(partenza, arrivo);
        let durata_movimento_s = if fermo {
            0.0
        } else {
            distanza_totale_m / (velocita_kmh * 1_000.0 / 3_600.0)
        };

        let tappe = linea_del_tempo(durata_movimento_s, pause);
        let durata_totale_s = tappe.last().map_or(0.0, |tappa| tappa.fine_s);

        tracing::info!(
            distanza_m = distanza_totale_m,
            velocita_kmh,
            durata_totale_s,
            soste = tappe
                .iter()
                .filter(|t| t.da_frazione == t.a_frazione)
                .count(),
            "rotta preparata"
        );

        Self {
            partenza,
            arrivo,
            distanza_totale_m,
            direzione_gradi: rotta_iniziale(partenza, arrivo),
            tappe,
            durata_totale_s,
            fermo,
            zero: None,
        }
    }

    /// Coordinata del punto che sta alla frazione indicata del tragitto.
    ///
    /// I due estremi non passano dall'interpolazione: entro [`TOLLERANZA_ESTREMI_M`] si
    /// restituiscono le coordinate ricevute dal costruttore cosi' come sono, altrimenti un
    /// arrivo "quasi giusto" per pochi decimi di millimetro si propagherebbe nelle analisi
    /// del server.
    fn coordinata_a(&self, frazione: f64) -> Coordinate {
        let percorsi_m = frazione * self.distanza_totale_m;
        if percorsi_m <= TOLLERANZA_ESTREMI_M {
            return self.partenza;
        }
        if self.distanza_totale_m - percorsi_m <= TOLLERANZA_ESTREMI_M {
            return self.arrivo;
        }

        // La formula non produce coordinate fuori intervallo: se mai accadesse, il veicolo
        // resta dov'era invece di finire in un punto inventato.
        destinazione(self.partenza, percorsi_m, self.direzione_gradi).unwrap_or(self.partenza)
    }
}

impl PositionSource for RouteSource {
    /// Posizione lungo la rotta all'istante `now`, o l'arrivo se il tragitto e' finito.
    /// Non restituisce mai `None`.
    fn next_position(&mut self, now: DateTime<Utc>) -> Option<Coordinate> {
        if self.fermo {
            return Some(self.partenza);
        }

        let zero = *self.zero.get_or_insert(now);
        let trascorso_s = secondi_trascorsi(zero, now);
        if trascorso_s >= self.durata_totale_s {
            return Some(self.arrivo);
        }

        // I tratti sono pochi (due per pausa, piu' uno) e in ordine di tempo: la scansione
        // lineare costa meno di una ricerca dicotomica e si legge molto meglio.
        for tappa in &self.tappe {
            if trascorso_s < tappa.fine_s {
                let durata_s = tappa.fine_s - tappa.inizio_s;
                let avanzamento = if durata_s > 0.0 {
                    ((trascorso_s - tappa.inizio_s) / durata_s).clamp(0.0, 1.0)
                } else {
                    0.0
                };
                let frazione =
                    tappa.da_frazione + (tappa.a_frazione - tappa.da_frazione) * avanzamento;
                return Some(self.coordinata_a(frazione));
            }
        }

        Some(self.arrivo)
    }

    fn name(&self) -> &'static str {
        "route"
    }
}

/// Intercala i tratti di movimento alle soste, in ordine di tempo.
///
/// `durata_movimento_s` e' il tempo del solo movimento sull'intero tragitto: ogni tratto
/// ne riceve la quota corrispondente alla frazione che copre, cosi' la velocita' resta
/// costante e la somma dei tratti torna al totale.
fn linea_del_tempo(durata_movimento_s: f64, pause: Vec<Pausa>) -> Vec<Tappa> {
    let soste = normalizza_pause(pause);
    let mut tappe = Vec::with_capacity(soste.len() * 2 + 1);
    let mut cursore_s = 0.0;
    let mut frazione = 0.0;

    for (a_frazione, durata_sosta_s) in soste {
        if a_frazione > frazione {
            let durata_s = durata_movimento_s * (a_frazione - frazione);
            tappe.push(Tappa {
                inizio_s: cursore_s,
                fine_s: cursore_s + durata_s,
                da_frazione: frazione,
                a_frazione,
            });
            cursore_s += durata_s;
            frazione = a_frazione;
        }

        tappe.push(Tappa {
            inizio_s: cursore_s,
            fine_s: cursore_s + durata_sosta_s,
            da_frazione: frazione,
            a_frazione: frazione,
        });
        cursore_s += durata_sosta_s;
    }

    if frazione < 1.0 {
        tappe.push(Tappa {
            inizio_s: cursore_s,
            fine_s: cursore_s + durata_movimento_s * (1.0 - frazione),
            da_frazione: frazione,
            a_frazione: 1.0,
        });
    }

    tappe
}

/// Riporta le pause in `[0, 1]`, le ordina per frazione crescente e fonde quelle che
/// cadono nello stesso punto, restituendo coppie `(frazione, durata in secondi)`.
fn normalizza_pause(pause: Vec<Pausa>) -> Vec<(f64, f64)> {
    let mut soste: Vec<(f64, f64)> = Vec::with_capacity(pause.len());

    for pausa in pause {
        if !pausa.a_frazione.is_finite() {
            tracing::warn!(
                a_frazione = pausa.a_frazione,
                "pausa con frazione non finita, scartata"
            );
            continue;
        }

        let frazione = pausa.a_frazione.clamp(0.0, 1.0);
        if frazione != pausa.a_frazione {
            tracing::warn!(
                richiesta = pausa.a_frazione,
                usata = frazione,
                "frazione di pausa fuori da [0, 1], riportata all'estremo piu' vicino"
            );
        }
        soste.push((frazione, pausa.durata.as_secs_f64()));
    }

    // Le frazioni sono finite, quindi `total_cmp` e' qui un ordine totale, senza il caso
    // `None` di `partial_cmp` da gestire.
    soste.sort_by(|(una, _), (altra, _)| una.total_cmp(altra));

    let mut fuse: Vec<(f64, f64)> = Vec::with_capacity(soste.len());
    for (frazione, durata_s) in soste {
        match fuse.last_mut() {
            // Ordinate, le pause sullo stesso punto sono adiacenti: sommarle evita di
            // separarle con un tratto di movimento di durata nulla.
            Some((ultima, accumulata)) if *ultima == frazione => *accumulata += durata_s,
            _ => fuse.push((frazione, durata_s)),
        }
    }

    fuse
}

/// Secondi trascorsi da `zero` a `now`, mai negativi.
///
/// Un `now` precedente al tempo zero non dovrebbe arrivare, ma un orologio corretto
/// all'indietro puo' produrlo: vale come istante zero, cosi' il veicolo resta alla partenza
/// invece di indietreggiare fuori dal tragitto. Uno scarto talmente grande da non stare in
/// microsecondi diventa [`f64::INFINITY`], che ogni rotta legge come "ampiamente arrivato".
fn secondi_trascorsi(zero: DateTime<Utc>, now: DateTime<Utc>) -> f64 {
    match now.signed_duration_since(zero).num_microseconds() {
        Some(microsecondi) if microsecondi <= 0 => 0.0,
        Some(microsecondi) => microsecondi as f64 / 1e6,
        None => f64::INFINITY,
    }
}

/// Sorgente guidata da fuori: la posizione arriva su un canale, dall'operatore.
///
/// Il canale e' quello sincrono di [`std::sync::mpsc`] e non quello di tokio, perche'
/// [`PositionSource::next_position`] e' un metodo sincrono che puo' essere chiamato anche
/// fuori da un contesto asincrono; la lettura usa `try_recv` e non blocca mai, perche' una
/// sorgente che si mette in attesa fermerebbe l'invio periodico delle posizioni.
///
/// Ogni chiamata svuota il canale e tiene l'ultima coordinata arrivata: le posizioni
/// accumulate fra due chiamate non sono una coda da smaltire un elemento per volta ma
/// correzioni successive dello stesso operatore, di cui vale l'ultima. La sorgente non
/// finisce mai: un operatore che smette di mandare posizioni ha solo lasciato il veicolo
/// fermo dov'era.
#[derive(Debug)]
pub struct ManualSource {
    /// Ultima coordinata nota: quella iniziale finche' il canale non ne consegna un'altra.
    corrente: Coordinate,
    /// Capo ricevente del canale alimentato dall'operatore.
    comandi: Receiver<Coordinate>,
}

impl ManualSource {
    /// Costruisce la sorgente ferma su `iniziale`, insieme al capo con cui guidarla.
    ///
    /// Il canale nasce qui perche' i due capi hanno senso solo in coppia: restituirli
    /// insieme rende impossibile costruire una sorgente il cui mittente e' gia' stato
    /// perso. Lasciar cadere il [`Sender`] non e' comunque un errore.
    pub fn new(iniziale: Coordinate) -> (Self, Sender<Coordinate>) {
        let (trasmettitore, comandi) = mpsc::channel();
        (
            Self {
                corrente: iniziale,
                comandi,
            },
            trasmettitore,
        )
    }
}

impl PositionSource for ManualSource {
    /// Aggiorna la posizione con l'ultima coordinata ricevuta, o ripete quella nota.
    /// L'istante `now` non conta e il risultato non e' mai `None`.
    fn next_position(&mut self, _now: DateTime<Utc>) -> Option<Coordinate> {
        // Il ciclo termina sempre: `try_recv` fallisce non appena il canale e' vuoto o
        // senza mittenti, e nessuno puo' riempirlo piu' in fretta di quanto lo si svuota.
        while let Ok(coordinate) = self.comandi.try_recv() {
            self.corrente = coordinate;
        }

        Some(self.corrente)
    }

    fn name(&self) -> &'static str {
        "manual"
    }
}

/// Esito di [`SourceKind::build`]: la sorgente e, dove serve, il canale per guidarla.
///
/// Esiste perche' una sola variante, [`SourceKind::Manual`], deve restituire qualcosa in
/// piu' della sorgente: cosi' ogni variante si costruisce allo stesso modo e chi non ha un
/// canale lo dichiara con `None`.
pub struct SorgenteCostruita {
    /// Sorgente pronta all'uso, dietro il trait.
    pub sorgente: Box<dyn PositionSource>,
    /// Capo trasmittente di [`ManualSource`], `None` per ogni altra variante.
    pub comandi: Option<Sender<Coordinate>>,
}

/// Strategia di simulazione scelta dalla configurazione del client.
///
/// Le varianti descrivono i parametri, [`SourceKind::build`] costruisce l'oggetto. Chi
/// consuma le posizioni vede solo [`PositionSource`], quindi aggiungere una strategia non
/// lo tocca.
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
    /// Rotta a velocita' costante fra due punti, percorsa da [`RouteSource`].
    Route {
        /// Punto di partenza del tragitto.
        partenza: Coordinate,
        /// Punto di arrivo, dove il veicolo resta fermo a tragitto finito.
        arrivo: Coordinate,
        /// Velocita' di percorrenza in chilometri orari.
        velocita_kmh: f64,
        /// Soste programmate lungo il tragitto, in qualsiasi ordine.
        pause: Vec<Pausa>,
    },
    /// Posizione decisa dall'operatore su un canale, servita da [`ManualSource`].
    Manual {
        /// Coordinata su cui il veicolo si trova prima di ogni invio.
        iniziale: Coordinate,
    },
}

impl SourceKind {
    /// Costruisce la sorgente descritta da questa variante.
    pub fn build(&self) -> io::Result<SorgenteCostruita> {
        let costruita = match self {
            Self::File {
                percorso,
                loop_forever,
            } => SorgenteCostruita {
                sorgente: Box::new(FileSource::from_path(percorso, *loop_forever)?),
                comandi: None,
            },
            Self::RandomWalk {
                partenza,
                raggio_massimo_m,
                passo_massimo_m,
                seed,
            } => SorgenteCostruita {
                sorgente: Box::new(RandomWalkSource::new(
                    *partenza,
                    *raggio_massimo_m,
                    *passo_massimo_m,
                    *seed,
                )),
                comandi: None,
            },
            Self::Route {
                partenza,
                arrivo,
                velocita_kmh,
                pause,
            } => SorgenteCostruita {
                sorgente: Box::new(RouteSource::new(
                    *partenza,
                    *arrivo,
                    *velocita_kmh,
                    pause.clone(),
                )),
                comandi: None,
            },
            Self::Manual { iniziale } => {
                let (sorgente, comandi) = ManualSource::new(*iniziale);
                SorgenteCostruita {
                    sorgente: Box::new(sorgente),
                    comandi: Some(comandi),
                }
            }
        };

        Ok(costruita)
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

    /// Istante di riferimento dei test piu' `secondi`, anche frazionari.
    ///
    /// I tempi di [`RouteSource`] escono da una divisione fra distanza e velocita', quindi
    /// non cadono quasi mai su un secondo intero: la risoluzione al microsecondo basta a
    /// campionare gli istanti che interessano senza doverli arrotondare.
    fn af(secondi: f64) -> DateTime<Utc> {
        a(0) + TimeDelta::microseconds((secondi * 1e6) as i64)
    }

    /// Punto di partenza dei test sul cammino casuale: Piazza Castello, Torino.
    fn torino() -> Coordinate {
        coord(45.0703, 7.6869)
    }

    /// Arrivo dei test sulla rotta: Piazza Duomo, Milano, un centinaio di chilometri.
    fn milano() -> Coordinate {
        coord(45.4642, 9.1900)
    }

    /// Velocita' dei test sulla rotta: a 100 km/h il veicolo copre 27,8 m al secondo,
    /// abbastanza da rendere leggibili gli scarti misurati in metri.
    const VELOCITA_PROVA_KMH: f64 = 100.0;

    /// Secondi di solo movimento fra due coordinate a [`VELOCITA_PROVA_KMH`].
    ///
    /// Ripete il conto che fa [`RouteSource::new`]: se i due divergessero, i test sui
    /// tempi di arrivo non verificherebbero piu' nulla.
    fn durata_movimento_s(partenza: Coordinate, arrivo: Coordinate) -> f64 {
        haversine_meters(partenza, arrivo) / (VELOCITA_PROVA_KMH * 1_000.0 / 3_600.0)
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

    // --- RouteSource ---

    #[test]
    fn una_rotta_senza_pause_arriva_al_tempo_atteso() {
        let movimento_s = durata_movimento_s(torino(), milano());
        let mut rotta = RouteSource::new(torino(), milano(), VELOCITA_PROVA_KMH, Vec::new());

        assert_eq!(
            rotta.next_position(af(0.0)),
            Some(torino()),
            "al tempo zero il veicolo e' ancora alla partenza"
        );

        let poco_prima = rotta
            .next_position(af(movimento_s - 1.0))
            .expect("la rotta non finisce mai");
        assert_ne!(
            poco_prima,
            milano(),
            "un secondo prima del tempo previsto il veicolo e' ancora in viaggio"
        );
        let mancanti = haversine_meters(poco_prima, milano());
        assert!(
            (20.0..40.0).contains(&mancanti),
            "a un secondo dall'arrivo, a {VELOCITA_PROVA_KMH} km/h, mancano ~28 m: misurati {mancanti} m"
        );

        assert_eq!(
            rotta.next_position(af(movimento_s)),
            Some(milano()),
            "il tempo di arrivo e' distanza / velocita'"
        );
    }

    #[test]
    fn il_tempo_zero_della_rotta_e_la_prima_chiamata() {
        let movimento_s = durata_movimento_s(torino(), milano());
        let mut rotta = RouteSource::new(torino(), milano(), VELOCITA_PROVA_KMH, Vec::new());

        // Fra la costruzione e il primo uso passano la connessione e l'autenticazione:
        // il tragitto non deve scorrere durante l'attesa.
        assert_eq!(
            rotta.next_position(af(3_600.0)),
            Some(torino()),
            "il tragitto comincia alla prima chiamata, non alla costruzione"
        );
        assert_eq!(
            rotta.next_position(af(3_600.0 + movimento_s)),
            Some(milano()),
            "l'arrivo e' un tempo di movimento dopo la prima chiamata"
        );
    }

    #[test]
    fn una_pausa_a_meta_tragitto_congela_la_coordinata() {
        const PAUSA_S: f64 = 600.0;
        let movimento_s = durata_movimento_s(torino(), milano());
        let mut rotta = RouteSource::new(
            torino(),
            milano(),
            VELOCITA_PROVA_KMH,
            vec![Pausa {
                a_frazione: 0.5,
                durata: Duration::from_secs(600),
            }],
        );
        rotta.next_position(af(0.0));

        // Il campione di riferimento sta dentro la pausa e non sull'istante esatto in cui
        // comincia, che appartiene ancora al tratto di movimento.
        let inizio_pausa = movimento_s / 2.0;
        let in_sosta = rotta
            .next_position(af(inizio_pausa + 0.5))
            .expect("la rotta non finisce mai");

        let percorsi = haversine_meters(torino(), in_sosta);
        let meta = haversine_meters(torino(), milano()) / 2.0;
        assert!(
            (percorsi - meta).abs() < 1.0,
            "la pausa a 0.5 cade a meta' tragitto: attesi {meta} m, percorsi {percorsi} m"
        );

        for istante in [1.0, 60.0, 300.0, PAUSA_S - 0.001] {
            assert_eq!(
                rotta.next_position(af(inizio_pausa + istante)),
                Some(in_sosta),
                "durante la pausa la coordinata non cambia (a {istante} s dall'inizio)"
            );
        }

        assert_ne!(
            rotta.next_position(af(inizio_pausa + PAUSA_S + 1.0)),
            Some(in_sosta),
            "finita la pausa il veicolo riprende a muoversi"
        );
    }

    #[test]
    fn durante_le_pause_il_tempo_del_movimento_si_ferma() {
        let movimento_s = durata_movimento_s(torino(), milano());
        let mut rotta = RouteSource::new(
            torino(),
            milano(),
            VELOCITA_PROVA_KMH,
            vec![
                Pausa {
                    a_frazione: 0.25,
                    durata: Duration::from_secs(300),
                },
                Pausa {
                    a_frazione: 0.75,
                    durata: Duration::from_secs(180),
                },
            ],
        );
        rotta.next_position(af(0.0));

        assert_ne!(
            rotta.next_position(af(movimento_s)),
            Some(milano()),
            "trascorso il solo tempo di movimento il veicolo non e' ancora arrivato"
        );

        let totale_s = movimento_s + 480.0;
        assert_ne!(
            rotta.next_position(af(totale_s - 1.0)),
            Some(milano()),
            "un secondo prima del totale il veicolo e' ancora in viaggio"
        );
        assert_eq!(
            rotta.next_position(af(totale_s)),
            Some(milano()),
            "il tragitto dura il movimento piu' la durata di tutte le pause"
        );
    }

    #[test]
    fn le_pause_vengono_ordinate_per_frazione_crescente() {
        let movimento_s = durata_movimento_s(torino(), milano());
        let pause = vec![
            Pausa {
                a_frazione: 0.75,
                durata: Duration::from_secs(300),
            },
            Pausa {
                a_frazione: 0.25,
                durata: Duration::from_secs(300),
            },
        ];
        let mut disordinate =
            RouteSource::new(torino(), milano(), VELOCITA_PROVA_KMH, pause.clone());
        let mut ordinate = RouteSource::new(
            torino(),
            milano(),
            VELOCITA_PROVA_KMH,
            pause.into_iter().rev().collect(),
        );

        for passo in 0..40 {
            let istante = af(f64::from(passo) * movimento_s / 20.0);
            assert_eq!(
                disordinate.next_position(istante),
                ordinate.next_position(istante),
                "l'ordine in cui le pause sono elencate non cambia il tragitto (passo {passo})"
            );
        }
    }

    #[test]
    fn due_pause_sulla_stessa_frazione_diventano_una_sosta_piu_lunga() {
        let movimento_s = durata_movimento_s(torino(), milano());
        let mut rotta = RouteSource::new(
            torino(),
            milano(),
            VELOCITA_PROVA_KMH,
            vec![
                Pausa {
                    a_frazione: 0.5,
                    durata: Duration::from_secs(120),
                },
                Pausa {
                    a_frazione: 0.5,
                    durata: Duration::from_secs(180),
                },
            ],
        );
        rotta.next_position(af(0.0));

        let inizio_pausa = movimento_s / 2.0;
        let in_sosta = rotta
            .next_position(af(inizio_pausa + 1.0))
            .expect("la rotta non finisce mai");
        assert_eq!(
            rotta.next_position(af(inizio_pausa + 299.0)),
            Some(in_sosta),
            "le due pause nello stesso punto valgono una sosta di 300 s"
        );
        assert_ne!(
            rotta.next_position(af(inizio_pausa + 301.0)),
            Some(in_sosta),
            "scaduta la sosta fusa il veicolo riparte"
        );
        assert_eq!(
            rotta.next_position(af(movimento_s + 300.0)),
            Some(milano()),
            "il tragitto si allunga della somma delle due pause"
        );
    }

    #[test]
    fn dopo_l_arrivo_la_rotta_ripete_l_arrivo_e_non_finisce_mai() {
        let mut rotta = RouteSource::new(torino(), milano(), VELOCITA_PROVA_KMH, Vec::new());
        rotta.next_position(af(0.0));

        // Una rotta conclusa resta un veicolo che esiste e sta fermo a destinazione:
        // a differenza di FileSource non ciclico non restituisce mai `None`.
        for ora in 2..26 {
            assert_eq!(
                rotta.next_position(af(3_600.0 * f64::from(ora))),
                Some(milano()),
                "dopo l'arrivo la rotta ripete la destinazione (ora {ora})"
            );
        }
    }

    #[test]
    fn velocita_non_valida_lascia_il_veicolo_alla_partenza() {
        for velocita_kmh in [0.0, -10.0, f64::NAN, f64::INFINITY] {
            let mut rotta = RouteSource::new(
                torino(),
                milano(),
                velocita_kmh,
                vec![Pausa {
                    a_frazione: 0.5,
                    durata: Duration::from_secs(60),
                }],
            );

            for istante in [0.0, 1.0, 10_000.0, 1e9] {
                assert_eq!(
                    rotta.next_position(af(istante)),
                    Some(torino()),
                    "velocita' {velocita_kmh}: il veicolo resta fermo alla partenza (istante {istante})"
                );
            }
        }
    }

    #[test]
    fn una_pausa_fuori_intervallo_viene_riportata_agli_estremi() {
        let movimento_s = durata_movimento_s(torino(), milano());
        let mut rotta = RouteSource::new(
            torino(),
            milano(),
            VELOCITA_PROVA_KMH,
            vec![
                Pausa {
                    a_frazione: -1.0,
                    durata: Duration::from_secs(60),
                },
                Pausa {
                    a_frazione: 2.0,
                    durata: Duration::from_secs(60),
                },
            ],
        );
        rotta.next_position(af(0.0));

        assert_eq!(
            rotta.next_position(af(30.0)),
            Some(torino()),
            "la frazione negativa diventa una sosta alla partenza"
        );
        assert_ne!(
            rotta.next_position(af(59.0 + movimento_s)),
            Some(milano()),
            "il movimento comincia solo dopo la sosta iniziale"
        );
        assert_eq!(
            rotta.next_position(af(60.0 + movimento_s)),
            Some(milano()),
            "la frazione oltre uno diventa una sosta all'arrivo"
        );
        assert_eq!(
            rotta.next_position(af(200.0 + movimento_s)),
            Some(milano()),
            "che e' indistinguibile dal veicolo fermo a destinazione"
        );
    }

    #[test]
    fn una_pausa_con_frazione_non_finita_viene_scartata() {
        let movimento_s = durata_movimento_s(torino(), milano());
        let mut rotta = RouteSource::new(
            torino(),
            milano(),
            VELOCITA_PROVA_KMH,
            vec![Pausa {
                a_frazione: f64::NAN,
                durata: Duration::from_secs(600),
            }],
        );
        rotta.next_position(af(0.0));

        assert_eq!(
            rotta.next_position(af(movimento_s)),
            Some(milano()),
            "una pausa con frazione non finita non allunga il tragitto"
        );
    }

    #[test]
    fn la_posizione_intermedia_sta_sulla_geodetica_non_sulla_media_dei_gradi() {
        // Rotta lunga e ad alta latitudine: e' il caso in cui la media aritmetica di
        // latitudine e longitudine sbaglia di parecchi chilometri, perche' la geodetica
        // piega verso il polo mentre la media resta sul parallelo.
        let partenza = coord(60.0, 0.0);
        let arrivo = coord(60.0, 60.0);
        let movimento_s = durata_movimento_s(partenza, arrivo);
        let mut rotta = RouteSource::new(partenza, arrivo, VELOCITA_PROVA_KMH, Vec::new());
        rotta.next_position(af(0.0));

        let meta = rotta
            .next_position(af(movimento_s / 2.0))
            .expect("la rotta non finisce mai");

        let mezza_distanza = haversine_meters(partenza, arrivo) / 2.0;
        assert!(
            (haversine_meters(partenza, meta) - mezza_distanza).abs() < 1.0
                && (haversine_meters(meta, arrivo) - mezza_distanza).abs() < 1.0,
            "a meta' tempo il veicolo e' equidistante dagli estremi: {meta:?}"
        );
        assert!(
            meta.lat() > 60.5,
            "la media aritmetica darebbe latitudine 60: la geodetica passa a nord, {}",
            meta.lat()
        );
    }

    // --- ManualSource ---

    #[test]
    fn prima_di_ogni_invio_ripete_la_coordinata_iniziale() {
        let (mut sorgente, _comandi) = ManualSource::new(torino());

        for chiamata in 0..10 {
            assert_eq!(
                sorgente.next_position(a(chiamata)),
                Some(torino()),
                "il veicolo esiste fin dall'inizio, sulla coordinata iniziale (chiamata {chiamata})"
            );
        }
    }

    #[test]
    fn dopo_un_invio_la_nuova_coordinata_resta_finche_non_ne_arriva_un_altra() {
        let (mut sorgente, comandi) = ManualSource::new(torino());

        comandi.send(milano()).expect("il ricevitore e' vivo");
        assert_eq!(sorgente.next_position(a(0)), Some(milano()));
        assert_eq!(
            sorgente.next_position(a(1)),
            Some(milano()),
            "senza nuovi invii la coordinata si ripete"
        );

        let roma = coord(41.9028, 12.4964);
        comandi.send(roma).expect("il ricevitore e' vivo");
        assert_eq!(sorgente.next_position(a(2)), Some(roma));
        assert_eq!(sorgente.next_position(a(3)), Some(roma));
    }

    #[test]
    fn di_piu_coordinate_in_coda_vale_solo_l_ultima() {
        let (mut sorgente, comandi) = ManualSource::new(torino());
        let ultima = coord(41.9028, 12.4964);

        // Nessuna chiamata a next_position fra un invio e l'altro: a differenza di
        // FileSource la coda non si smaltisce un elemento per volta, l'operatore ha
        // corretto la posizione due volte e vale l'ultima parola.
        comandi
            .send(coord(43.0, 7.0))
            .expect("il ricevitore e' vivo");
        comandi.send(milano()).expect("il ricevitore e' vivo");
        comandi.send(ultima).expect("il ricevitore e' vivo");

        assert_eq!(
            sorgente.next_position(a(0)),
            Some(ultima),
            "il canale viene svuotato: vale l'ultima coordinata disponibile"
        );
        assert_eq!(
            sorgente.next_position(a(1)),
            Some(ultima),
            "e non resta nulla di arretrato da consegnare"
        );
    }

    #[test]
    fn il_mittente_caduto_non_ferma_la_sorgente() {
        let (mut sorgente, comandi) = ManualSource::new(torino());

        comandi.send(milano()).expect("il ricevitore e' vivo");
        drop(comandi);

        assert_eq!(
            sorgente.next_position(a(0)),
            Some(milano()),
            "le coordinate gia' inviate arrivano anche a mittente caduto"
        );
        for chiamata in 1..10 {
            assert_eq!(
                sorgente.next_position(a(chiamata)),
                Some(milano()),
                "senza mittente la sorgente ripete l'ultima nota (chiamata {chiamata})"
            );
        }
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
        assert_eq!(da_file.sorgente.name(), "file");
        assert!(
            da_file.comandi.is_none(),
            "solo la sorgente manuale ha un canale"
        );

        let casuale = SourceKind::RandomWalk {
            partenza: torino(),
            raggio_massimo_m: 500.0,
            passo_massimo_m: 20.0,
            seed: 42,
        }
        .build()
        .expect("il cammino casuale non puo' fallire");
        assert_eq!(casuale.sorgente.name(), "random-walk");
        assert!(casuale.comandi.is_none());
    }

    #[test]
    fn la_factory_costruisce_la_sorgente_manuale_col_suo_canale() {
        let mut manuale = SourceKind::Manual { iniziale: torino() }
            .build()
            .expect("la sorgente manuale non puo' fallire");

        assert_eq!(manuale.sorgente.name(), "manual");
        assert_eq!(
            manuale.sorgente.next_position(a(0)),
            Some(torino()),
            "la coordinata iniziale e' quella della configurazione"
        );

        let comandi = manuale
            .comandi
            .take()
            .expect("la variante manuale restituisce il capo trasmittente");
        comandi.send(milano()).expect("il ricevitore e' vivo");
        assert_eq!(
            manuale.sorgente.next_position(a(1)),
            Some(milano()),
            "il canale restituito alimenta davvero la sorgente costruita"
        );
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
