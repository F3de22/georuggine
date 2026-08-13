//! Log periodico del tempo di CPU consumato dal processo (requisito R15).
//!
//! Il modulo [`cpu_time`](crate::cpu_time) sa leggere il contatore del sistema, ma una
//! lettura sola non dice nulla: il valore cresce sempre, e quello che interessa a chi
//! guarda il server e' quanto e' cresciuto nell'ultimo intervallo. [`CpuLogger`] tiene
//! percio' la lettura precedente e scrive una riga ogni [`CpuLogger::intervallo`], con il
//! valore assoluto, la differenza rispetto al giro prima e la percentuale che quella
//! differenza rappresenta sul tempo di orologio davvero trascorso.
//!
//! # Le due misure del tempo, e perche' sono diverse
//!
//! Il timestamp della riga arriva dal [`Clock`] condiviso col resto del server, come
//! vuole l'invariante del progetto: e' un fatto da datare, e va datato come tutti gli
//! altri. L'intervallo su cui si calcola la percentuale viene invece da [`Instant`], che
//! e' monotono: un orologio di calendario puo' saltare (correzione NTP, cambio d'ora) o,
//! nei test, essere un [`FakeClock`] fermo, e in entrambi i casi la divisione avrebbe al
//! denominatore un tempo trascorso falso o nullo.
//!
//! [`FakeClock`]: georuggine_common::clock::FakeClock
//!
//! # Nessun guasto del log ferma il server
//!
//! Un log e' un'osservazione, non una funzione del servizio: se il disco e' pieno o la
//! cartella non e' scrivibile, il task registra l'errore con `tracing::error!` e riprova
//! al giro successivo. Non propaga l'errore, non va in panico e non chiude il processo.

use std::fs::{self, OpenOptions};
use std::io::{self, Write as _};
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::{DateTime, SecondsFormat, Utc};
use georuggine_common::clock::Clock;
use tokio::sync::broadcast;
use tokio::task;
use tokio::time::MissedTickBehavior;

use crate::cpu_time::{CpuUsage, process_cpu_usage};
use crate::net::ConnectionRegistry;

/// Nome del file di log dentro la cartella configurata.
const NOME_FILE: &str = "cpu.log";

/// Intestazione delle colonne, scritta solo alla creazione del file.
///
/// Le colonne sono separate da tabulazione e non da virgola: i valori non contengono mai
/// tabulazioni, quindi non serve nessuna forma di quoting, e il file resta leggibile sia
/// a occhio sia con `cut -f`.
const INTESTAZIONE: &str = "timestamp\tcpu_utente_ms\tcpu_sistema_ms\tcpu_totale_ms\t\
                            delta_ms\tpercentuale_cpu\tutenti_connessi\n";

/// Lettura del tempo di CPU, con l'istante monotono in cui e' stata presa.
///
/// I due valori stanno insieme perche' si usano insieme: la percentuale e' il rapporto
/// fra la crescita del primo e quella del secondo, e tenerli separati permetterebbe di
/// aggiornarne uno solo.
#[derive(Debug, Clone, Copy)]
struct Rilevazione {
    /// Tempo di CPU complessivo letto in quel momento.
    totale: Duration,
    /// Istante monotono della lettura.
    istante: Instant,
}

/// Scrive periodicamente su file il tempo di CPU consumato dal server.
///
/// Requisito R15 della traccia. Si costruisce con [`CpuLogger::new`] e si avvia con
/// [`CpuLogger::avvia`], tipicamente dentro un `tokio::spawn`.
pub struct CpuLogger {
    /// Cartella che contiene `cpu.log`; viene creata se non esiste.
    log_dir: PathBuf,
    /// Tempo fra due righe di log.
    intervallo: Duration,
    /// Sorgente dell'ora con cui si data ogni riga.
    clock: Arc<dyn Clock>,
}

impl CpuLogger {
    /// Costruisce un logger che scrive in `log_dir` ogni `intervallo`.
    ///
    /// La cartella non viene toccata qui: nasce alla prima scrittura, dentro
    /// `spawn_blocking`, perche' crearla e' I/O bloccante e questo costruttore puo'
    /// essere chiamato da un contesto asincrono.
    pub fn new(log_dir: PathBuf, intervallo: Duration, clock: Arc<dyn Clock>) -> Self {
        Self {
            log_dir,
            intervallo,
            clock,
        }
    }

    /// Scrive una riga di log a ogni intervallo, finche' non arriva lo spegnimento.
    ///
    /// Prende `self` per valore, e non a prestito, perche' il chiamante lo passa a
    /// `tokio::spawn`, che pretende un futuro `'static`: con `&self` servirebbe comunque
    /// un [`Arc`] tenuto in vita dal chiamante.
    ///
    /// La forma e' quella di `batti_gli_stati` in [`net`](crate::net): un `select!` fra il
    /// timer e il canale di spegnimento. La differenza sta nella prima scadenza, che
    /// `tokio::time::interval` fa maturare subito: `batti_gli_stati` la consuma e la butta
    /// via, qui invece la si tiene, perche' con un intervallo di due minuti un file vuoto
    /// per due minuti sembra un logger rotto. La prima riga esce quindi all'avvio, con
    /// delta e percentuale a zero: non c'e' nessuna lettura precedente da confrontare.
    ///
    /// Allo spegnimento scrive un'ultima riga prima di uscire, cosi' il file ha un punto
    /// finale e l'ultimo tratto di attivita' del server non resta fuori dal conteggio.
    pub async fn avvia(
        self,
        registry: Arc<ConnectionRegistry>,
        mut shutdown: broadcast::Receiver<()>,
    ) {
        let mut battito = tokio::time::interval(self.intervallo);
        // Se il task resta indietro (macchina sospesa, esecutore congestionato) le
        // scadenze perse non si recuperano: il comportamento predefinito le farebbe
        // maturare tutte insieme, e il file si riempirebbe di righe distanti fra loro
        // microsecondi, con percentuali calcolate su intervalli quasi nulli. R15 chiede
        // una fotografia ogni due minuti, non il recupero di quelle mancate.
        battito.set_missed_tick_behavior(MissedTickBehavior::Delay);

        let mut precedente: Option<Rilevazione> = None;

        loop {
            tokio::select! {
                // Anche l'errore del ricevitore (mittente caduto, segnali saltati) e' un
                // motivo per smettere: nessuno mandera' piu' nulla su quel canale.
                _ = shutdown.recv() => {
                    self.registra(&registry, &mut precedente).await;
                    tracing::info!("spegnimento: il log del tempo di CPU si chiude");
                    return;
                }
                _ = battito.tick() => {
                    self.registra(&registry, &mut precedente).await;
                }
            }
        }
    }

    /// Prende una lettura, la trasforma in una riga e la accoda al file.
    ///
    /// Aggiorna `precedente` con la lettura appena presa anche quando la scrittura
    /// fallisce: il `delta_ms` della riga successiva e' definito rispetto all'ultima
    /// *lettura*, non all'ultima riga andata a buon fine, e ancorarlo a quest'ultima
    /// farebbe comparire, dopo una serie di guasti, un delta che copre un intervallo
    /// diverso da quello su cui e' calcolata la percentuale.
    ///
    /// Una lettura che fallisce salta invece la riga: senza il tempo di CPU non c'e'
    /// niente da scrivere, e inventare uno zero direbbe il falso.
    async fn registra(&self, registry: &ConnectionRegistry, precedente: &mut Option<Rilevazione>) {
        let uso = match process_cpu_usage() {
            Ok(uso) => uso,
            Err(errore) => {
                tracing::error!(%errore, "tempo di CPU non leggibile: riga di log saltata");
                return;
            }
        };
        let adesso = Rilevazione {
            totale: uso.total(),
            istante: Instant::now(),
        };

        // Alla prima riga non c'e' un intervallo precedente: delta e percentuale valgono
        // zero, che e' l'unica cosa vera che si possa dire di un confronto che manca.
        let (delta, trascorso) = match precedente {
            Some(prima) => (
                adesso.totale.saturating_sub(prima.totale),
                adesso.istante.saturating_duration_since(prima.istante),
            ),
            None => (Duration::ZERO, Duration::ZERO),
        };
        *precedente = Some(adesso);

        let riga = formatta_riga(
            self.clock.now(),
            uso,
            delta,
            percentuale_cpu(delta, trascorso),
            registry.connected_users().await.len(),
        );

        // La scrittura su file e' bloccante come ogni altro I/O del progetto: va su un
        // thread da lavoro, altrimenti fermerebbe un thread dell'esecutore.
        let percorso = self.log_dir.clone();
        match task::spawn_blocking(move || accoda_riga(&percorso, &riga)).await {
            Ok(Ok(())) => {}
            Ok(Err(errore)) => {
                tracing::error!(
                    cartella = %self.log_dir.display(),
                    %errore,
                    "riga del log di CPU non scritta: si riprova al prossimo intervallo"
                );
            }
            Err(errore) => {
                tracing::error!(%errore, "attivita' di scrittura del log di CPU non completata");
            }
        }
    }
}

/// Compone la riga di log, terminata da un a capo.
///
/// I tempi sono in millisecondi interi: il contatore del sistema ha risoluzione al
/// microsecondo, ma su intervalli di due minuti la terza cifra decimale di un
/// millisecondo non aggiunge nulla e allunga la riga.
fn formatta_riga(
    quando: DateTime<Utc>,
    uso: CpuUsage,
    delta: Duration,
    percentuale: f64,
    utenti: usize,
) -> String {
    format!(
        "{}\t{}\t{}\t{}\t{}\t{:.2}\t{}\n",
        quando.to_rfc3339_opts(SecondsFormat::Secs, true),
        uso.user.as_millis(),
        uso.system.as_millis(),
        uso.total().as_millis(),
        delta.as_millis(),
        percentuale,
        utenti,
    )
}

/// Percentuale di CPU usata nell'intervallo: `delta / trascorso * 100`.
///
/// Il valore puo' superare 100 in modo del tutto legittimo, perche' il server e' multi
/// thread e in un secondo di orologio puo' consumare piu' di un secondo di CPU: il tetto
/// e' quindi `100 * numero di core`, non 100. Sopra quel tetto il rapporto non descrive
/// piu' niente di reale — ci si arriva solo con un intervallo misurato male, per esempio
/// se la macchina e' stata sospesa fra due letture — e il valore viene limitato al tetto
/// invece di finire nel file cosi' com'e'.
///
/// Non restituisce mai NaN, infinito o un valore negativo: `delta` e `trascorso` sono
/// [`Duration`], quindi non negativi per costruzione; l'intervallo nullo della prima riga
/// e' trattato a parte, perche' li' la divisione darebbe NaN o infinito; e cio' che resta
/// passa comunque da un controllo di finitezza prima del confronto col tetto.
fn percentuale_cpu(delta: Duration, trascorso: Duration) -> f64 {
    if trascorso.is_zero() {
        return 0.0;
    }

    let percentuale = delta.as_secs_f64() / trascorso.as_secs_f64() * 100.0;
    if percentuale.is_finite() {
        percentuale.clamp(0.0, tetto_percentuale())
    } else {
        0.0
    }
}

/// Valore massimo che [`percentuale_cpu`] puo' restituire: `100 * numero di core`.
///
/// Se il numero di core non e' interrogabile si assume una macchina a un core, cioe' il
/// tetto piu' basso: il valore riportato resta prudente invece di lasciar passare
/// percentuali che nessuno puo' verificare.
fn tetto_percentuale() -> f64 {
    let core = std::thread::available_parallelism().map_or(1, NonZeroUsize::get);
    100.0 * core as f64
}

/// Accoda una riga a `cpu.log`, creando cartella, file e intestazione se mancano.
///
/// E' l'unica funzione bloccante del modulo, e per questo il chiamante la esegue dentro
/// `spawn_blocking`.
///
/// L'intestazione si scrive solo quando il file nasce qui, e la distinzione arriva dal
/// sistema operativo invece che da un controllo di esistenza: si prova prima ad aprire
/// con `create_new`, che fallisce con [`io::ErrorKind::AlreadyExists`] se il file c'e'
/// gia'. Un `Path::exists` seguito da un'apertura lascerebbe una finestra fra i due, e
/// due esecuzioni contemporanee potrebbero scrivere entrambe l'intestazione.
///
/// # Errori
///
/// L'errore del sistema operativo se la cartella non si puo' creare, se il file non si
/// puo' aprire in aggiunta o se la scrittura non riesce.
fn accoda_riga(log_dir: &Path, riga: &str) -> io::Result<()> {
    fs::create_dir_all(log_dir)?;
    let percorso = log_dir.join(NOME_FILE);

    match OpenOptions::new()
        .append(true)
        .create_new(true)
        .open(&percorso)
    {
        // Intestazione e prima riga in una scrittura sola: cosi' non esiste un istante in
        // cui il file contiene la sola intestazione senza dati.
        Ok(mut file) => file.write_all(format!("{INTESTAZIONE}{riga}").as_bytes()),
        Err(errore) if errore.kind() == io::ErrorKind::AlreadyExists => {
            let mut file = OpenOptions::new()
                .append(true)
                .create(true)
                .open(&percorso)?;
            file.write_all(riga.as_bytes())
        }
        Err(errore) => Err(errore),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::time::{SystemTime, UNIX_EPOCH};

    use georuggine_common::clock::SystemClock;
    use tokio::time::timeout;

    /// Intervallo dei test: abbastanza corto da non rallentare la suite.
    const INTERVALLO_BREVE: Duration = Duration::from_millis(20);

    /// Attesa massima concessa a un'attesa su file prima di dichiarare fallito il test.
    ///
    /// E' un tetto, non una pausa: le funzioni che lo usano tornano appena la condizione
    /// e' vera. Serve solo a far fallire il test con un messaggio chiaro invece di
    /// lasciarlo appeso se il logger non scrive nulla.
    const ATTESA_MASSIMA: Duration = Duration::from_secs(10);

    /// Passo con cui si ricontrolla il file mentre si aspetta.
    const PASSO_ATTESA: Duration = Duration::from_millis(5);

    /// Cartella di prova nella directory temporanea, unica per esecuzione.
    ///
    /// Il percorso non esiste ancora: e' il logger a doverlo creare, ed e' proprio cio'
    /// che i test verificano.
    fn cartella_temporanea(nome: &str) -> PathBuf {
        let unico = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        std::env::temp_dir().join(format!(
            "georuggine-cpulog-{nome}-{}-{unico}",
            std::process::id()
        ))
    }

    /// Costruisce un logger di prova sulla cartella indicata.
    fn logger(log_dir: &Path) -> CpuLogger {
        CpuLogger::new(
            log_dir.to_path_buf(),
            INTERVALLO_BREVE,
            Arc::new(SystemClock),
        )
    }

    /// Righe attualmente presenti nel file, vuoto se il file non c'e' ancora.
    fn righe(log_dir: &Path) -> Vec<String> {
        match fs::read_to_string(log_dir.join(NOME_FILE)) {
            Ok(contenuto) => contenuto.lines().map(str::to_owned).collect(),
            Err(_) => Vec::new(),
        }
    }

    /// Aspetta che il file contenga almeno `quante` righe in tutto, e le restituisce.
    ///
    /// La sincronizzazione e' a condizione e non a tempo: il test non indovina quanti
    /// millisecondi servono per tre righe, ricontrolla il file finche' non ce ne sono
    /// abbastanza. [`ATTESA_MASSIMA`] evita che un logger fermo appenda il test per
    /// sempre, e superarla e' un fallimento con un messaggio esplicito.
    async fn attendi_righe(log_dir: &Path, quante: usize) -> Vec<String> {
        let attesa = async {
            loop {
                let presenti = righe(log_dir);
                if presenti.len() >= quante {
                    return presenti;
                }
                tokio::time::sleep(PASSO_ATTESA).await;
            }
        };

        timeout(ATTESA_MASSIMA, attesa)
            .await
            .unwrap_or_else(|_| panic!("attese {quante} righe in {}", log_dir.display()))
    }

    /// Colonne di una riga di dati, separate da tabulazione.
    fn colonne(riga: &str) -> Vec<&str> {
        riga.split('\t').collect()
    }

    /// Tiene occupata la CPU per almeno `durata`, senza mai sospendere il thread.
    ///
    /// Come in `cpu_time`: uno `sleep` farebbe passare il tempo di orologio ma non quello
    /// di calcolo, e il delta resterebbe a zero. Il risultato passa da
    /// [`std::hint::black_box`] perche' altrimenti l'ottimizzatore eliminerebbe il ciclo.
    fn brucia_cpu(durata: Duration) {
        let inizio = Instant::now();
        let mut accumulatore: u64 = 0;
        while inizio.elapsed() < durata {
            for i in 0..10_000_u64 {
                accumulatore = accumulatore.wrapping_add(i.wrapping_mul(i));
            }
        }
        std::hint::black_box(accumulatore);
    }

    // --- percentuale_cpu, senza toccare il disco ---

    #[test]
    fn la_percentuale_del_primo_intervallo_e_zero() {
        assert_eq!(percentuale_cpu(Duration::ZERO, Duration::ZERO), 0.0);
    }

    #[test]
    fn mezzo_secondo_di_cpu_in_un_secondo_fa_cinquanta_per_cento() {
        let percentuale = percentuale_cpu(Duration::from_millis(500), Duration::from_secs(1));
        assert!(
            (percentuale - 50.0).abs() < 1e-9,
            "attesa 50%, ottenuto {percentuale}"
        );
    }

    #[test]
    fn oltre_il_tetto_si_satura_al_numero_di_core() {
        // Un anno di CPU in un microsecondo non e' misurabile: e' il caso dell'intervallo
        // sballato, e il valore va limitato invece di finire nel file.
        let percentuale =
            percentuale_cpu(Duration::from_secs(31_536_000), Duration::from_micros(1));
        assert!(percentuale.is_finite(), "percentuale non finita");
        assert_eq!(percentuale, tetto_percentuale());
    }

    #[test]
    fn la_percentuale_non_e_mai_negativa_ne_nan() {
        for (delta, trascorso) in [
            (Duration::ZERO, Duration::ZERO),
            (Duration::ZERO, Duration::from_secs(1)),
            (Duration::MAX, Duration::from_nanos(1)),
            (Duration::from_millis(1), Duration::MAX),
        ] {
            let percentuale = percentuale_cpu(delta, trascorso);
            assert!(percentuale.is_finite(), "percentuale non finita: {delta:?}");
            assert!(percentuale >= 0.0, "percentuale negativa: {percentuale}");
        }
    }

    #[test]
    fn il_tetto_vale_almeno_cento() {
        assert!(tetto_percentuale() >= 100.0);
    }

    // --- formato della riga ---

    #[test]
    fn la_riga_ha_sette_colonne_come_l_intestazione() {
        let riga = formatta_riga(
            DateTime::from_timestamp(1_704_067_200, 0).expect("istante valido"),
            CpuUsage {
                user: Duration::from_millis(1_500),
                system: Duration::from_millis(250),
            },
            Duration::from_millis(100),
            12.5,
            3,
        );

        assert!(riga.ends_with('\n'), "riga senza a capo: {riga:?}");
        let intestazione = colonne(INTESTAZIONE.trim_end());
        let valori = colonne(riga.trim_end());
        assert_eq!(valori.len(), intestazione.len());
        assert_eq!(
            valori,
            vec![
                "2024-01-01T00:00:00Z",
                "1500",
                "250",
                "1750",
                "100",
                "12.50",
                "3"
            ]
        );
    }

    // --- il task sul file ---

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn crea_il_file_con_l_intestazione_alla_prima_esecuzione() {
        let cartella = cartella_temporanea("creazione");
        let (spegnimento, _) = broadcast::channel(1);
        let task = tokio::spawn(
            logger(&cartella).avvia(Arc::new(ConnectionRegistry::new()), spegnimento.subscribe()),
        );

        let righe = attendi_righe(&cartella, 2).await;
        assert_eq!(righe[0], INTESTAZIONE.trim_end());

        let _ = spegnimento.send(());
        let _ = task.await;
        let _ = fs::remove_dir_all(&cartella);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn scrive_almeno_tre_righe_di_dati_valide() {
        let cartella = cartella_temporanea("tre-righe");
        let (spegnimento, _) = broadcast::channel(1);
        let task = tokio::spawn(
            logger(&cartella).avvia(Arc::new(ConnectionRegistry::new()), spegnimento.subscribe()),
        );

        // Il ciclo di calcolo gira su un thread da lavoro: bloccando qui il thread del
        // test, con due worker il logger continua comunque a battere. Serve a far
        // crescere davvero il contatore fra due letture, come nei test di `cpu_time`.
        let _ = task::spawn_blocking(|| brucia_cpu(Duration::from_millis(150))).await;

        let righe = attendi_righe(&cartella, 4).await;
        let _ = spegnimento.send(());
        let _ = task.await;

        assert_eq!(righe[0], INTESTAZIONE.trim_end());
        assert!(righe.len() >= 4, "attese intestazione e tre righe di dati");

        let mut precedente: Option<u128> = None;
        let mut delta_massimo = 0_u128;
        for riga in &righe[1..] {
            let colonne = colonne(riga);
            assert_eq!(colonne.len(), 7, "colonne inattese in {riga:?}");

            let totale: u128 = colonne[3].parse().expect("cpu_totale_ms e' un intero");
            let delta: u128 = colonne[4].parse().expect("delta_ms e' un intero");
            let percentuale: f64 = colonne[5].parse().expect("percentuale_cpu e' un numero");

            assert!(
                percentuale.is_finite(),
                "percentuale non finita in {riga:?}"
            );
            assert!(percentuale >= 0.0, "percentuale negativa in {riga:?}");
            // Il registro dei test e' vuoto: nessun veicolo collegato.
            assert_eq!(colonne[6], "0", "utenti connessi inattesi in {riga:?}");

            // `delta` e' senza segno gia' nel tipo: quello che si verifica qui e' che il
            // totale non torni indietro e che il delta racconti la stessa differenza. Il
            // confronto tollera un millisecondo perche' i tre valori sono troncati a
            // millisecondo ognuno per conto proprio: due totali che distano 0,2 ms
            // possono comparire come 10 e 11 con delta 0.
            match precedente {
                None => assert_eq!(delta, 0, "la prima riga non ha una lettura precedente"),
                Some(prima) => {
                    assert!(
                        totale >= prima,
                        "tempo di CPU decrescente: {prima} poi {totale}"
                    );
                    let differenza = totale - prima;
                    assert!(
                        delta.abs_diff(differenza) <= 1,
                        "delta {delta} incoerente con la differenza {differenza} in {riga:?}"
                    );
                }
            }

            precedente = Some(totale);
            delta_massimo = delta_massimo.max(delta);
        }

        assert!(
            delta_massimo > 0,
            "nessun incremento di CPU rilevato dopo il ciclo di calcolo"
        );

        let _ = fs::remove_dir_all(&cartella);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn non_riscrive_l_intestazione_su_un_file_gia_popolato() {
        let cartella = cartella_temporanea("intestazione-unica");
        fs::create_dir_all(&cartella).expect("cartella di prova creata");
        fs::write(
            cartella.join(NOME_FILE),
            format!("{INTESTAZIONE}2024-01-01T00:00:00Z\t10\t5\t15\t0\t0.00\t0\n"),
        )
        .expect("file di prova scritto");

        let (spegnimento, _) = broadcast::channel(1);
        let task = tokio::spawn(
            logger(&cartella).avvia(Arc::new(ConnectionRegistry::new()), spegnimento.subscribe()),
        );

        // Il file parte con due righe (intestazione e una riga di dati): se ne aspettano
        // almeno altrettante scritte dal logger.
        let righe = attendi_righe(&cartella, 4).await;
        let _ = spegnimento.send(());
        let _ = task.await;

        let intestazioni = righe
            .iter()
            .filter(|riga| riga.as_str() == INTESTAZIONE.trim_end())
            .count();
        assert_eq!(intestazioni, 1, "intestazione ripetuta: {righe:?}");
        assert_eq!(righe[0], INTESTAZIONE.trim_end());

        let _ = fs::remove_dir_all(&cartella);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn lo_spegnimento_ferma_il_task_e_lascia_una_riga_finale() {
        // L'intervallo e' lungo rispetto alla durata del test: dopo la riga iniziale il
        // logger non ne scrive altre da se', quindi la riga in piu' che si conta alla
        // fine puo' essere solo quella dello spegnimento.
        let cartella = cartella_temporanea("spegnimento");
        let logger = CpuLogger::new(
            cartella.clone(),
            Duration::from_secs(3_600),
            Arc::new(SystemClock),
        );
        let (spegnimento, _) = broadcast::channel(1);
        let task = tokio::spawn(
            logger.avvia(Arc::new(ConnectionRegistry::new()), spegnimento.subscribe()),
        );

        let prima = attendi_righe(&cartella, 2).await.len();

        assert!(spegnimento.send(()).is_ok(), "nessun ricevitore in ascolto");
        // Nessuna attesa a tempo: il task scrive la riga finale prima di terminare,
        // quindi quando la sua `JoinHandle` si risolve la scrittura e' gia' avvenuta.
        // Il tetto serve solo a far fallire il test se il task non terminasse affatto.
        timeout(ATTESA_MASSIMA, task)
            .await
            .expect("il task termina dopo lo spegnimento")
            .expect("il task non va in panico");

        let dopo = righe(&cartella).len();
        assert_eq!(dopo, prima + 1, "attesa una sola riga finale");

        let _ = fs::remove_dir_all(&cartella);
    }
}
