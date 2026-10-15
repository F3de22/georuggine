//! Log periodico del tempo di CPU consumato dal processo.
//!
//! Ogni riga riporta il valore assoluto, la differenza dalla lettura precedente e la
//! percentuale sul tempo trascorso, misurato con [`Instant`] perche' monotono. Un errore
//! di scrittura non ferma il server: si riprova all'intervallo successivo.

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

/// Intestazione delle colonne, separate da tabulazione.
const INTESTAZIONE: &str = "timestamp\tcpu_utente_ms\tcpu_sistema_ms\tcpu_totale_ms\t\
                            delta_ms\tpercentuale_cpu\tutenti_connessi\n";

/// Lettura del tempo di CPU con l'istante monotono in cui e' stata presa.
#[derive(Debug, Clone, Copy)]
struct Rilevazione {
    /// Tempo di CPU complessivo letto in quel momento.
    totale: Duration,
    /// Istante monotono della lettura.
    istante: Instant,
}

/// Scrive periodicamente su file il tempo di CPU consumato dal server.
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
    pub fn new(log_dir: PathBuf, intervallo: Duration, clock: Arc<dyn Clock>) -> Self {
        Self {
            log_dir,
            intervallo,
            clock,
        }
    }

    /// Scrive una riga all'avvio, una a ogni intervallo e una finale allo spegnimento.
    pub async fn avvia(
        self,
        registry: Arc<ConnectionRegistry>,
        mut shutdown: broadcast::Receiver<()>,
    ) {
        let mut battito = tokio::time::interval(self.intervallo);
        // Le scadenze perse non si recuperano: righe ravvicinate darebbero percentuali
        // calcolate su intervalli quasi nulli.
        battito.set_missed_tick_behavior(MissedTickBehavior::Delay);

        let mut precedente: Option<Rilevazione> = None;

        loop {
            tokio::select! {
                // Anche un errore del ricevitore significa che non arriveranno altri segnali.
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

    /// Legge il tempo di CPU e accoda una riga al file.
    ///
    /// Se la lettura fallisce la riga viene saltata; `precedente` si aggiorna anche se
    /// fallisce la scrittura, cosi' delta e percentuale restano sullo stesso intervallo.
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

        // Alla prima riga delta e percentuale valgono zero.
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

        // La scrittura su file e' bloccante: va su un thread da lavoro.
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

/// Compone la riga di log, con i tempi in millisecondi.
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

/// Percentuale di CPU nell'intervallo, limitata a `100 * numero di core`.
///
/// Restituisce 0 per un intervallo nullo e non produce mai NaN o valori negativi.
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

/// Tetto di [`percentuale_cpu`]: `100 * numero di core`, con un core se non rilevabile.
fn tetto_percentuale() -> f64 {
    let core = std::thread::available_parallelism().map_or(1, NonZeroUsize::get);
    100.0 * core as f64
}

/// Accoda una riga a `cpu.log`, creando cartella, file e intestazione se mancano.
///
/// L'apertura con `create_new` stabilisce in modo atomico se il file e' nuovo e va
/// scritta l'intestazione.
///
/// # Errori
///
/// L'errore del sistema operativo se cartella o file non si possono creare o scrivere.
fn accoda_riga(log_dir: &Path, riga: &str) -> io::Result<()> {
    fs::create_dir_all(log_dir)?;
    let percorso = log_dir.join(NOME_FILE);

    match OpenOptions::new()
        .append(true)
        .create_new(true)
        .open(&percorso)
    {
        // Intestazione e prima riga in un'unica scrittura.
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

    /// Attesa massima delle condizioni verificate dai test.
    const ATTESA_MASSIMA: Duration = Duration::from_secs(10);

    /// Passo con cui si ricontrolla il file mentre si aspetta.
    const PASSO_ATTESA: Duration = Duration::from_millis(5);

    /// Cartella di prova ancora inesistente, che il logger deve creare.
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

    /// Attende che il file contenga almeno `quante` righe e le restituisce.
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

    fn colonne(riga: &str) -> Vec<&str> {
        riga.split('\t').collect()
    }

    /// Tiene occupata la CPU per almeno `durata`.
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

    #[test]
    fn la_percentuale_e_sempre_finita_non_negativa_e_sotto_il_tetto() {
        assert!(tetto_percentuale() >= 100.0);
        assert_eq!(percentuale_cpu(Duration::ZERO, Duration::ZERO), 0.0);

        let meta = percentuale_cpu(Duration::from_millis(500), Duration::from_secs(1));
        assert!((meta - 50.0).abs() < 1e-9, "attesa 50%, ottenuto {meta}");

        // Intervallo misurato male: il valore va limitato al tetto.
        assert_eq!(
            percentuale_cpu(Duration::from_secs(31_536_000), Duration::from_micros(1)),
            tetto_percentuale()
        );

        for (delta, trascorso) in [
            (Duration::ZERO, Duration::from_secs(1)),
            (Duration::MAX, Duration::from_nanos(1)),
            (Duration::from_millis(1), Duration::MAX),
        ] {
            let percentuale = percentuale_cpu(delta, trascorso);
            assert!(percentuale.is_finite(), "non finita per {delta:?}");
            assert!(percentuale >= 0.0, "negativa: {percentuale}");
        }
    }

    #[test]
    fn la_riga_ha_le_stesse_colonne_dell_intestazione() {
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
        assert_eq!(
            colonne(riga.trim_end()).len(),
            colonne(INTESTAZIONE.trim_end()).len()
        );
        assert_eq!(
            colonne(riga.trim_end()),
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn scrive_intestazione_e_righe_di_dati_coerenti() {
        let cartella = cartella_temporanea("righe");
        let (spegnimento, _) = broadcast::channel(1);
        let task = tokio::spawn(
            logger(&cartella).avvia(Arc::new(ConnectionRegistry::new()), spegnimento.subscribe()),
        );

        // Il calcolo gira su un altro thread, cosi' il logger continua a scrivere.
        let _ = task::spawn_blocking(|| brucia_cpu(Duration::from_millis(150))).await;

        let righe = attendi_righe(&cartella, 4).await;
        let _ = spegnimento.send(());
        let _ = task.await;

        assert_eq!(righe[0], INTESTAZIONE.trim_end());

        let mut precedente: Option<u128> = None;
        let mut delta_massimo = 0_u128;
        for riga in &righe[1..] {
            let colonne = colonne(riga);
            assert_eq!(colonne.len(), 7, "colonne inattese in {riga:?}");

            let totale: u128 = colonne[3].parse().expect("cpu_totale_ms e' un intero");
            let delta: u128 = colonne[4].parse().expect("delta_ms e' un intero");
            let percentuale: f64 = colonne[5].parse().expect("percentuale_cpu e' un numero");

            assert!(percentuale.is_finite() && percentuale >= 0.0, "in {riga:?}");
            // Il registro dei test e' vuoto: nessun veicolo collegato.
            assert_eq!(colonne[6], "0", "utenti connessi inattesi in {riga:?}");

            // Totale non decrescente e delta coerente, con un millisecondo di tolleranza
            // per gli arrotondamenti.
            match precedente {
                None => assert_eq!(delta, 0, "la prima riga non ha una lettura precedente"),
                Some(prima) => {
                    assert!(totale >= prima, "CPU decrescente: {prima} poi {totale}");
                    let differenza = totale - prima;
                    assert!(
                        delta.abs_diff(differenza) <= 1,
                        "delta {delta} incoerente con la differenza {differenza}"
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

        let righe = attendi_righe(&cartella, 4).await;
        let _ = spegnimento.send(());
        let _ = task.await;

        assert_eq!(
            righe
                .iter()
                .filter(|riga| riga.as_str() == INTESTAZIONE.trim_end())
                .count(),
            1,
            "intestazione ripetuta: {righe:?}"
        );

        let _ = fs::remove_dir_all(&cartella);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn lo_spegnimento_ferma_il_task_e_lascia_una_riga_finale() {
        // Con un intervallo lungo, la riga in piu' puo' essere solo quella finale.
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
        // Quando il task termina la riga finale e' gia' scritta.
        timeout(ATTESA_MASSIMA, task)
            .await
            .expect("il task termina dopo lo spegnimento")
            .expect("il task non va in panico");

        assert_eq!(righe(&cartella).len(), prima + 1, "attesa una riga finale");

        let _ = fs::remove_dir_all(&cartella);
    }
}
