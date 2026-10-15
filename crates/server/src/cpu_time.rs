//! Tempo di CPU consumato dal processo: `getrusage(RUSAGE_SELF)` su Unix,
//! `GetProcessTimes` su Windows.
//!
//! E' l'unico modulo del progetto con codice `unsafe`, necessario per le chiamate FFI.
//! Le conversioni verso [`Duration`] saturano e portano a zero i valori negativi.

use std::io;
use std::time::Duration;

/// Tempo di CPU consumato dal processo dall'avvio, diviso per modalita' di esecuzione.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct CpuUsage {
    /// Tempo di CPU passato nel codice del processo.
    pub user: Duration,
    /// Tempo di CPU passato nel kernel per conto del processo.
    pub system: Duration,
}

impl CpuUsage {
    /// Tempo di CPU complessivo; la somma satura invece di andare in panico.
    pub fn total(&self) -> Duration {
        self.user.saturating_add(self.system)
    }
}

/// Legge il tempo di CPU consumato finora dal processo.
///
/// # Errori
///
/// L'errore del sistema operativo se la chiamata fallisce.
#[cfg(unix)]
pub fn process_cpu_usage() -> io::Result<CpuUsage> {
    use std::mem::MaybeUninit;

    let mut consumo = MaybeUninit::<libc::rusage>::uninit();

    // SAFETY: il puntatore indica una `MaybeUninit<rusage>` locale, valida, allineata e
    // non aliasata per tutta la chiamata; `getrusage` la scrive per intero e non conserva
    // il puntatore.
    let esito = unsafe { libc::getrusage(libc::RUSAGE_SELF, consumo.as_mut_ptr()) };

    if esito != 0 {
        return Err(io::Error::last_os_error());
    }

    // SAFETY: `getrusage` ha restituito 0, quindi ha inizializzato tutta la struttura.
    let consumo = unsafe { consumo.assume_init() };

    Ok(CpuUsage {
        user: da_timeval(consumo.ru_utime),
        system: da_timeval(consumo.ru_stime),
    })
}

/// Converte un `timeval` in [`Duration`]; i valori negativi diventano zero.
#[cfg(unix)]
fn da_timeval(valore: libc::timeval) -> Duration {
    let secondi = u64::try_from(valore.tv_sec).unwrap_or(0);
    let microsecondi = u64::try_from(valore.tv_usec).unwrap_or(0);

    Duration::from_secs(secondi).saturating_add(Duration::from_micros(microsecondi))
}

/// Legge il tempo di CPU consumato finora dal processo.
///
/// # Errori
///
/// L'errore del sistema operativo se la chiamata fallisce.
#[cfg(windows)]
pub fn process_cpu_usage() -> io::Result<CpuUsage> {
    use windows_sys::Win32::Foundation::FILETIME;
    use windows_sys::Win32::System::Threading::{GetCurrentProcess, GetProcessTimes};

    // Creazione e uscita non servono, ma l'API non accetta puntatori nulli.
    let mut creazione = FILETIME::default();
    let mut uscita = FILETIME::default();
    let mut kernel = FILETIME::default();
    let mut utente = FILETIME::default();

    // SAFETY: `GetCurrentProcess` non ha precondizioni e restituisce uno pseudo-handle
    // sempre valido, da non chiudere.
    let processo = unsafe { GetCurrentProcess() };

    // SAFETY: l'handle e' valido e i quattro puntatori indicano `FILETIME` locali
    // distinti, inizializzati e vivi per tutta la chiamata sincrona.
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

/// Converte un `FILETIME` (unita' da 100 ns su due parole da 32 bit) in [`Duration`],
/// passando per secondi e resto per non traboccare.
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

/// Piattaforme diverse da Unix e Windows: non supportate.
///
/// # Errori
///
/// Sempre [`io::ErrorKind::Unsupported`].
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

    /// Tiene occupata la CPU per almeno `durata`; `black_box` impedisce
    /// all'ottimizzatore di eliminare il ciclo.
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
    fn total_somma_i_due_campi_e_satura() {
        assert_eq!(
            CpuUsage {
                user: Duration::from_millis(1_500),
                system: Duration::from_millis(250),
            }
            .total(),
            Duration::from_millis(1_750)
        );
        assert_eq!(
            CpuUsage {
                user: Duration::MAX,
                system: Duration::from_secs(1),
            }
            .total(),
            Duration::MAX
        );
    }

    #[cfg(any(unix, windows))]
    #[test]
    fn il_tempo_di_cpu_cresce_e_non_torna_mai_indietro() {
        let prima = process_cpu_usage().expect("prima lettura riuscita");
        brucia_cpu(Duration::from_millis(150));
        let dopo = process_cpu_usage().expect("seconda lettura riuscita");

        assert!(dopo.user >= prima.user, "user decrescente");
        assert!(dopo.system >= prima.system, "system decrescente");
        assert!(
            dopo.total() > prima.total(),
            "il tempo di CPU non e' cresciuto: prima {:?}, dopo {:?}",
            prima.total(),
            dopo.total()
        );
    }

    // I valori limite non devono traboccare e quelli negativi non devono diventare
    // durate enormi.

    #[cfg(unix)]
    #[test]
    fn la_conversione_di_un_timeval_regge_i_valori_limite() {
        let timeval = |sec, usec| libc::timeval {
            tv_sec: sec,
            tv_usec: usec,
        };

        assert_eq!(
            da_timeval(timeval(3, 500_000)),
            Duration::new(3, 500_000_000)
        );

        let secondi = u64::try_from(libc::time_t::MAX).expect("time_t::MAX e' positivo");
        assert_eq!(
            da_timeval(timeval(libc::time_t::MAX, 999_999)),
            Duration::from_secs(secondi)
                .checked_add(Duration::from_micros(999_999))
                .expect("la somma resta nell'intervallo di Duration")
        );

        assert_eq!(da_timeval(timeval(-1, -1)), Duration::ZERO);
        assert_eq!(
            da_timeval(timeval(libc::time_t::MIN, libc::suseconds_t::MIN)),
            Duration::ZERO
        );
    }

    #[cfg(windows)]
    #[test]
    fn la_conversione_di_un_filetime_regge_i_valori_limite() {
        use windows_sys::Win32::Foundation::FILETIME;

        let filetime = |alta, bassa| FILETIME {
            dwLowDateTime: bassa,
            dwHighDateTime: alta,
        };

        assert_eq!(da_filetime(filetime(0, 10_000_000)), Duration::from_secs(1));
        // 2^32 unita' da 100 ns = 429,4967296 s: la parola alta pesa 2^32.
        assert_eq!(da_filetime(filetime(1, 0)), Duration::new(429, 496_729_600));
        // u64::MAX unita' da 100 ns, che `from_nanos(unita * 100)` non rappresenterebbe.
        assert_eq!(
            da_filetime(filetime(u32::MAX, u32::MAX)),
            Duration::new(1_844_674_407_370, 955_161_500)
        );
    }
}
