//! Tempo di CPU consumato dal processo, letto dal sistema operativo.
//!
//! Il tempo di CPU e' cosa diversa dal tempo di orologio: un processo fermo su una
//! `accept` consuma secondi di orologio ma non di CPU. Serve percio' un contatore per
//! processo che la libreria standard non espone, e va chiesto direttamente al sistema:
//! `getrusage(RUSAGE_SELF)` su Unix, `GetProcessTimes` su Windows. Entrambe sono
//! chiamate FFI, e questo modulo e' l'unico punto del progetto in cui il `unsafe` che
//! richiedono e' ammesso; ogni blocco porta il proprio commento `// SAFETY:`.
//!
//! I contatori del sistema sono interi con segno (`timeval`) o coppie di parole da 32
//! bit (`FILETIME`), mentre [`Duration`] e' senza segno: la conversione e' isolata in
//! una funzione per piattaforma, che satura invece di traboccare e mappa a
//! [`Duration::ZERO`] i valori negativi, che il sistema non dovrebbe mai produrre.
//!
//! Sulle piattaforme che non sono ne' Unix ne' Windows non esiste una fonte
//! equivalente: [`process_cpu_usage`] restituisce un errore
//! [`io::ErrorKind::Unsupported`], mai un panico.

use std::io;
use std::time::Duration;

/// Tempo di CPU consumato dal processo dall'avvio, diviso per modalita' di esecuzione.
///
/// I due campi sono separati perche' misurano lavori diversi: `user` e' il tempo speso
/// nel codice del processo, `system` quello speso dal kernel nelle chiamate di sistema
/// fatte per suo conto (I/O di rete e su file). Chi vuole solo il costo complessivo usa
/// [`CpuUsage::total`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct CpuUsage {
    /// Tempo di CPU passato in spazio utente.
    pub user: Duration,
    /// Tempo di CPU passato in spazio kernel per conto del processo.
    pub system: Duration,
}

impl CpuUsage {
    /// Tempo di CPU complessivo: somma di [`CpuUsage::user`] e [`CpuUsage::system`].
    ///
    /// La somma satura a [`Duration::MAX`] invece di traboccare: l'operatore `+` fra
    /// `Duration` va in panico in caso di overflow, e il progetto non ammette panici
    /// sui percorsi di produzione. Con valori reali la saturazione non si verifica, per
    /// raggiungerla servirebbero circa 584 miliardi di anni di CPU.
    pub fn total(&self) -> Duration {
        self.user.saturating_add(self.system)
    }
}

/// Legge il tempo di CPU consumato finora dal processo corrente.
///
/// # Errori
///
/// Restituisce l'errore del sistema operativo se la chiamata sottostante fallisce, e un
/// errore [`io::ErrorKind::Unsupported`] sulle piattaforme che non sono ne' Unix ne'
/// Windows.
#[cfg(unix)]
pub fn process_cpu_usage() -> io::Result<CpuUsage> {
    use std::mem::MaybeUninit;

    // Non serve azzerare la struct: `getrusage` la riempie per intero, e leggerla prima
    // che sia stata scritta e' impedito dal tipo, non dalla disciplina di chi scrive.
    let mut consumo = MaybeUninit::<libc::rusage>::uninit();

    // SAFETY: `RUSAGE_SELF` e' la costante prevista dalla libc per il primo parametro.
    // `consumo.as_mut_ptr()` e' un puntatore a una `MaybeUninit<rusage>` allocata sullo
    // stack di questa funzione: e' non nullo, allineato come `rusage` e grande quanto
    // `rusage`, quindi la libc puo' scriverci dentro l'intera struttura. Il prestito
    // mutabile esclusivo dura per tutta la chiamata, che e' sincrona, e nessun altro
    // riferimento alla stessa memoria e' vivo: non c'e' aliasing. `getrusage` non
    // conserva il puntatore oltre il ritorno. Passare memoria non inizializzata e'
    // corretto perche' la funzione scrive senza leggere il contenuto precedente.
    let esito = unsafe { libc::getrusage(libc::RUSAGE_SELF, consumo.as_mut_ptr()) };

    if esito != 0 {
        return Err(io::Error::last_os_error());
    }

    // SAFETY: `getrusage` ha restituito 0, quindi per contratto ha inizializzato tutti
    // i campi della `rusage` puntata; la memoria e' percio' un valore valido del tipo e
    // `assume_init` puo' estrarlo. Il ramo di errore e' gia' uscito sopra, quindi qui
    // non si puo' arrivare con la struttura ancora non inizializzata.
    let consumo = unsafe { consumo.assume_init() };

    Ok(CpuUsage {
        user: da_timeval(consumo.ru_utime),
        system: da_timeval(consumo.ru_stime),
    })
}

/// Converte un `timeval` (secondi piu' microsecondi, con segno) in [`Duration`].
///
/// I valori negativi diventano [`Duration::ZERO`]: `Duration` non li rappresenta, e un
/// cast come `as u64` li trasformerebbe in durate enormi per wrapping silenzioso. La
/// somma satura invece di traboccare, cosi' nemmeno `time_t::MAX` porta a un panico.
#[cfg(unix)]
fn da_timeval(valore: libc::timeval) -> Duration {
    let secondi = u64::try_from(valore.tv_sec).unwrap_or(0);
    let microsecondi = u64::try_from(valore.tv_usec).unwrap_or(0);

    Duration::from_secs(secondi).saturating_add(Duration::from_micros(microsecondi))
}

/// Legge il tempo di CPU consumato finora dal processo corrente.
///
/// # Errori
///
/// Restituisce l'errore del sistema operativo se la chiamata sottostante fallisce, e un
/// errore [`io::ErrorKind::Unsupported`] sulle piattaforme che non sono ne' Unix ne'
/// Windows.
#[cfg(windows)]
pub fn process_cpu_usage() -> io::Result<CpuUsage> {
    use windows_sys::Win32::Foundation::FILETIME;
    use windows_sys::Win32::System::Threading::{GetCurrentProcess, GetProcessTimes};

    // `GetProcessTimes` scrive tutti e quattro i valori: creazione e uscita non
    // servono, ma non si possono omettere perche' l'API non accetta puntatori nulli.
    let mut creazione = FILETIME::default();
    let mut uscita = FILETIME::default();
    let mut kernel = FILETIME::default();
    let mut utente = FILETIME::default();

    // SAFETY: `GetCurrentProcess` non prende parametri e non tocca memoria del
    // chiamante, quindi non ha precondizioni da rispettare; il blocco `unsafe` serve
    // solo perche' e' una chiamata FFI. Restituisce lo pseudo-handle del processo
    // corrente, un valore costante valido per tutta la vita del processo e che non va
    // chiuso: non puo' fallire e non c'e' esito da controllare.
    let processo = unsafe { GetCurrentProcess() };

    // SAFETY: `processo` e' lo pseudo-handle appena ottenuto, sempre valido e con i
    // diritti necessari, perche' il processo corrente puo' sempre interrogare i propri
    // tempi. I quattro puntatori nascono da altrettante variabili locali `FILETIME`
    // distinte, gia' inizializzate a zero: sono non nulli, allineati, scrivibili, non
    // aliasati fra loro, e restano vivi per tutta la chiamata perche' vivono fino a
    // fine funzione mentre la chiamata e' sincrona. L'API scrive esattamente un
    // `FILETIME` per puntatore e non li conserva oltre il ritorno.
    let esito = unsafe {
        GetProcessTimes(
            processo,
            &mut creazione,
            &mut uscita,
            &mut kernel,
            &mut utente,
        )
    };

    if esito == 0 {
        return Err(io::Error::last_os_error());
    }

    Ok(CpuUsage {
        user: da_filetime(utente),
        system: da_filetime(kernel),
    })
}

/// Converte un `FILETIME` in [`Duration`].
///
/// Come contatore di durata un `FILETIME` e' un intero a 64 bit spezzato in due parole
/// da 32, in unita' da 100 nanosecondi. Si ricompone il valore e lo si divide in
/// secondi piu' resto invece di passare da `Duration::from_nanos(unita * 100)`, che per
/// valori grandi traboccherebbe: `u64::MAX * 100` non entra in 64 bit.
#[cfg(windows)]
fn da_filetime(valore: windows_sys::Win32::Foundation::FILETIME) -> Duration {
    /// Unita' da 100 ns contenute in un secondo.
    const UNITA_PER_SECONDO: u64 = 10_000_000;
    /// Nanosecondi in una unita' di `FILETIME`.
    const NANOSECONDI_PER_UNITA: u64 = 100;

    let unita = (u64::from(valore.dwHighDateTime) << 32) | u64::from(valore.dwLowDateTime);
    let resto = (unita % UNITA_PER_SECONDO) * NANOSECONDI_PER_UNITA;

    Duration::from_secs(unita / UNITA_PER_SECONDO).saturating_add(Duration::from_nanos(resto))
}

/// Legge il tempo di CPU consumato finora dal processo corrente.
///
/// # Errori
///
/// Restituisce l'errore del sistema operativo se la chiamata sottostante fallisce, e un
/// errore [`io::ErrorKind::Unsupported`] sulle piattaforme che non sono ne' Unix ne'
/// Windows.
#[cfg(not(any(unix, windows)))]
pub fn process_cpu_usage() -> io::Result<CpuUsage> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "misura del tempo di CPU non disponibile su questa piattaforma: \
         servono getrusage (Unix) o GetProcessTimes (Windows)",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::time::Instant;

    /// Tiene occupata la CPU per almeno `durata`, senza mai sospendere il thread.
    ///
    /// Serve a far crescere il contatore di tempo di CPU: uno `sleep` non andrebbe bene
    /// perche' fa passare il tempo di orologio ma non quello di calcolo. Il risultato
    /// passa da [`std::hint::black_box`] perche' altrimenti l'ottimizzatore potrebbe
    /// eliminare il ciclo, che non ha altri effetti osservabili.
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

    // --- CpuUsage ---

    #[test]
    fn total_e_la_somma_di_user_e_system() {
        let uso = CpuUsage {
            user: Duration::from_millis(1_500),
            system: Duration::from_millis(250),
        };
        assert_eq!(uso.total(), Duration::from_millis(1_750));
    }

    #[test]
    fn total_satura_invece_di_traboccare() {
        let uso = CpuUsage {
            user: Duration::MAX,
            system: Duration::from_secs(1),
        };
        assert_eq!(uso.total(), Duration::MAX);
    }

    // --- process_cpu_usage sulla piattaforma corrente ---

    #[cfg(any(unix, windows))]
    #[test]
    fn una_lettura_sulla_piattaforma_corrente_ritorna_ok() {
        let esito = process_cpu_usage();
        assert!(esito.is_ok(), "lettura fallita: {:?}", esito.err());
    }

    #[cfg(any(unix, windows))]
    #[test]
    fn il_tempo_di_cpu_cresce_dopo_un_ciclo_di_calcolo() {
        let prima = process_cpu_usage().expect("prima lettura riuscita");
        brucia_cpu(Duration::from_millis(150));
        let dopo = process_cpu_usage().expect("seconda lettura riuscita");

        assert!(
            dopo.total() > prima.total(),
            "il tempo di CPU non e' cresciuto: prima {:?}, dopo {:?}",
            prima.total(),
            dopo.total()
        );
    }

    #[cfg(any(unix, windows))]
    #[test]
    fn due_letture_consecutive_non_tornano_indietro() {
        let prima = process_cpu_usage().expect("prima lettura riuscita");
        let dopo = process_cpu_usage().expect("seconda lettura riuscita");

        assert!(dopo.user >= prima.user, "user decrescente");
        assert!(dopo.system >= prima.system, "system decrescente");
    }

    // --- conversione dei tipi di sistema ---
    //
    // I contatori del sistema operativo sono interi con segno (Unix) o coppie di
    // parole a 32 bit (Windows), mentre `Duration` e' senza segno: qui si verifica che
    // i valori limite non traboccino e che quelli negativi non diventino durate enormi
    // per wrapping silenzioso.

    #[cfg(unix)]
    mod conversione_unix {
        use super::*;

        /// Costruisce un `timeval` con i tipi esatti della piattaforma.
        fn timeval(sec: libc::time_t, usec: libc::suseconds_t) -> libc::timeval {
            libc::timeval {
                tv_sec: sec,
                tv_usec: usec,
            }
        }

        #[test]
        fn somma_secondi_e_microsecondi() {
            assert_eq!(
                da_timeval(timeval(3, 500_000)),
                Duration::new(3, 500_000_000)
            );
        }

        #[test]
        fn il_valore_massimo_non_trabocca() {
            let secondi = u64::try_from(libc::time_t::MAX).expect("time_t::MAX e' positivo");
            let atteso = Duration::from_secs(secondi)
                .checked_add(Duration::from_micros(999_999))
                .expect("la somma resta nell'intervallo di Duration");

            assert_eq!(da_timeval(timeval(libc::time_t::MAX, 999_999)), atteso);
        }

        #[test]
        fn i_valori_negativi_diventano_zero_e_non_durate_enormi() {
            assert_eq!(da_timeval(timeval(-1, -1)), Duration::ZERO);
            assert_eq!(
                da_timeval(timeval(libc::time_t::MIN, libc::suseconds_t::MIN)),
                Duration::ZERO
            );
        }
    }

    #[cfg(windows)]
    mod conversione_windows {
        use super::*;

        use windows_sys::Win32::Foundation::FILETIME;

        /// Costruisce un `FILETIME` dalle sue due meta' da 32 bit.
        fn filetime(alta: u32, bassa: u32) -> FILETIME {
            FILETIME {
                dwLowDateTime: bassa,
                dwHighDateTime: alta,
            }
        }

        #[test]
        fn dieci_milioni_di_unita_fanno_un_secondo() {
            assert_eq!(da_filetime(filetime(0, 10_000_000)), Duration::from_secs(1));
        }

        #[test]
        fn la_parola_alta_pesa_due_alla_trentaduesima() {
            // 2^32 unita' da 100 ns = 429,4967296 secondi.
            assert_eq!(da_filetime(filetime(1, 0)), Duration::new(429, 496_729_600));
        }

        #[test]
        fn il_valore_massimo_non_trabocca() {
            // u64::MAX unita' da 100 ns: 1_844_674_407_370 s e 955_161_500 ns.
            assert_eq!(
                da_filetime(filetime(u32::MAX, u32::MAX)),
                Duration::new(1_844_674_407_370, 955_161_500)
            );
        }
    }
}
