//! Astrazione del tempo: la logica di dominio legge l'ora da un [`Clock`] e non da
//! `Utc::now()`, cosi' i test possono far scorrere il tempo con [`FakeClock`].

use std::sync::{Mutex, MutexGuard, PoisonError};

use chrono::{DateTime, TimeDelta, Utc};

/// Sorgente dell'ora corrente, condivisa fra i task come `Arc<dyn Clock>`.
pub trait Clock: Send + Sync + 'static {
    /// Istante corrente in UTC.
    fn now(&self) -> DateTime<Utc>;
}

/// Orologio di produzione: legge l'ora dal sistema operativo.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

/// Orologio simulato: il tempo avanza solo con [`FakeClock::advance`].
///
/// Non e' sotto `#[cfg(test)]` perche' lo usano anche i test end-to-end.
#[derive(Debug)]
pub struct FakeClock {
    corrente: Mutex<DateTime<Utc>>,
}

impl FakeClock {
    /// Costruisce un orologio fermo sull'istante indicato.
    pub fn new(iniziale: DateTime<Utc>) -> Self {
        Self {
            corrente: Mutex::new(iniziale),
        }
    }

    /// Sposta il tempo di `delta`, anche all'indietro, saturando agli estremi.
    pub fn advance(&self, delta: TimeDelta) {
        let mut corrente = self.blocca();
        *corrente = corrente.checked_add_signed(delta).unwrap_or({
            if delta < TimeDelta::zero() {
                DateTime::<Utc>::MIN_UTC
            } else {
                DateTime::<Utc>::MAX_UTC
            }
        });
    }

    /// Un mutex avvelenato non lascia lo stato a meta': la sezione critica e' una sola
    /// assegnazione.
    fn blocca(&self) -> MutexGuard<'_, DateTime<Utc>> {
        self.corrente.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl Clock for FakeClock {
    fn now(&self) -> DateTime<Utc> {
        *self.blocca()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    fn istante_base() -> DateTime<Utc> {
        DateTime::from_timestamp(1_704_067_200, 0).expect("istante valido")
    }

    #[test]
    fn advance_sposta_il_tempo_e_gli_spostamenti_si_sommano() {
        let orologio = FakeClock::new(istante_base());

        orologio.advance(TimeDelta::seconds(30));
        orologio.advance(TimeDelta::seconds(45));
        assert_eq!(orologio.now(), istante_base() + TimeDelta::seconds(75));

        orologio.advance(TimeDelta::seconds(-75));
        assert_eq!(orologio.now(), istante_base());
    }

    #[test]
    fn advance_oltre_i_limiti_satura_invece_di_traboccare() {
        let orologio = FakeClock::new(istante_base());

        orologio.advance(TimeDelta::MAX);
        assert_eq!(orologio.now(), DateTime::<Utc>::MAX_UTC);
    }

    #[test]
    fn fake_clock_e_condivisibile_fra_thread() {
        let orologio = Arc::new(FakeClock::new(istante_base()));

        let scrittore = Arc::clone(&orologio);
        let handle = std::thread::spawn(move || {
            for _ in 0..100 {
                scrittore.advance(TimeDelta::seconds(1));
            }
        });
        handle.join().expect("il thread di test non va in panico");

        assert_eq!(orologio.now(), istante_base() + TimeDelta::seconds(100));
    }

    #[test]
    fn system_clock_non_torna_indietro() {
        let orologio = SystemClock;

        let prima = orologio.now();
        let dopo = orologio.now();
        assert!(dopo >= prima, "letture decrescenti: {prima} poi {dopo}");
    }
}
