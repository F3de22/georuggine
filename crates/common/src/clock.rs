//! Astrazione del tempo: la logica di dominio legge l'ora da un [`Clock`], mai da
//! `Utc::now()`.
//!
//! Chiamare `Utc::now()` dentro la logica di dominio la lega all'orologio di sistema, e
//! un comportamento che dipende dal tempo diventa verificabile solo aspettando davvero.
//! Il caso concreto e' il requisito R5: un veicolo che non si sposta per tre minuti passa
//! a `UserState::Still`. Con `Utc::now()` il test di quella transizione dovrebbe dormire
//! tre minuti; ricevendo il tempo da un `Clock` basta iniettare un [`FakeClock`] e
//! chiamare `advance(TimeDelta::seconds(180))`, che costa microsecondi ed e'
//! deterministico, senza sospensioni ne' esiti che cambiano da un'esecuzione all'altra.
//!
//! In produzione si passa [`SystemClock`]; nei test [`FakeClock`], che non e' dietro
//! `#[cfg(test)]` proprio perche' serve anche ai test di integrazione degli altri crate.

use std::sync::{Mutex, MutexGuard, PoisonError};

use chrono::{DateTime, TimeDelta, Utc};

/// Sorgente dell'ora corrente.
///
/// I bound `Send + Sync + 'static` sono sul trait e non sui singoli usi perche' un
/// orologio viene condiviso fra i task del server, tipicamente come
/// `Arc<dyn Clock>`: chiederli qui una volta evita di ripeterli a ogni struttura che
/// tiene un orologio.
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

/// Orologio simulato: il tempo avanza solo quando lo si sposta a mano.
///
/// E' condivisibile fra thread e task (`Send + Sync`) perche' i test di integrazione lo
/// passano al server dietro `Arc` e lo muovono da un task mentre altri leggono
/// [`Clock::now`]: per questo lo stato sta in un [`Mutex`] e non in una `RefCell`, che
/// non e' `Sync`. La sezione critica e' una copia di [`DateTime`] e non contiene punti di
/// sospensione, quindi il guard non attraversa mai un `.await`.
///
/// Non e' compilato dietro `#[cfg(test)]`: gli altri crate del workspace lo usano nei
/// propri test di integrazione, che vedono `georuggine-common` come dipendenza normale.
#[derive(Debug)]
pub struct FakeClock {
    /// Istante restituito da [`Clock::now`] fino alla prossima modifica.
    corrente: Mutex<DateTime<Utc>>,
}

impl FakeClock {
    /// Costruisce un orologio fermo sull'istante indicato.
    pub fn new(iniziale: DateTime<Utc>) -> Self {
        Self {
            corrente: Mutex::new(iniziale),
        }
    }

    /// Sposta il tempo di `delta`, in avanti o (con `delta` negativo) all'indietro.
    ///
    /// Gli spostamenti si accumulano: due `advance` consecutivi equivalgono a uno solo
    /// della somma dei due delta. Se il risultato uscirebbe dall'intervallo
    /// rappresentabile da [`DateTime<Utc>`] il valore satura agli estremi, invece di
    /// traboccare come farebbe l'operatore `+`, che qui andrebbe in panico.
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

    /// Porta il tempo esattamente a `t`, qualunque fosse il valore precedente.
    pub fn set(&self, t: DateTime<Utc>) {
        *self.blocca() = t;
    }

    /// Prende il lock sullo stato, riprendendo il valore anche se il mutex e' avvelenato.
    ///
    /// L'avvelenamento segnala che un altro thread e' andato in panico tenendo il lock.
    /// Qui la sezione critica e' una singola assegnazione, quindi non puo' lasciare lo
    /// stato a meta': il valore resta valido e si continua con
    /// [`PoisonError::into_inner`], evitando l'`unwrap` che il progetto vieta.
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

    /// Istante di riferimento dei test: 2024-01-01T00:00:00Z.
    fn istante_base() -> DateTime<Utc> {
        DateTime::from_timestamp(1_704_067_200, 0).expect("istante valido")
    }

    /// Accetta solo tipi che soddisfano i bound del trait [`Clock`].
    ///
    /// Il controllo e' a tempo di compilazione: se `FakeClock` perdesse `Send` o `Sync`
    /// (per esempio passando a `RefCell`), questa chiamata non compilerebbe piu'.
    fn richiede_send_sync<T: Send + Sync + 'static>(_valore: &T) {}

    // --- FakeClock ---

    #[test]
    fn fake_clock_ritorna_il_tempo_iniziale() {
        let orologio = FakeClock::new(istante_base());
        assert_eq!(orologio.now(), istante_base());
    }

    #[test]
    fn advance_sposta_il_tempo_della_quantita_esatta() {
        let orologio = FakeClock::new(istante_base());
        orologio.advance(TimeDelta::seconds(180));
        assert_eq!(orologio.now(), istante_base() + TimeDelta::seconds(180));
    }

    #[test]
    fn due_advance_consecutivi_si_sommano() {
        let orologio = FakeClock::new(istante_base());
        orologio.advance(TimeDelta::seconds(30));
        orologio.advance(TimeDelta::seconds(45));
        assert_eq!(orologio.now(), istante_base() + TimeDelta::seconds(75));
    }

    #[test]
    fn advance_con_delta_negativo_torna_indietro() {
        let orologio = FakeClock::new(istante_base());
        orologio.advance(TimeDelta::seconds(-10));
        assert_eq!(orologio.now(), istante_base() - TimeDelta::seconds(10));
    }

    #[test]
    fn set_sposta_il_tempo_al_valore_indicato() {
        let orologio = FakeClock::new(istante_base());
        orologio.advance(TimeDelta::seconds(3_600));

        let bersaglio = istante_base() - TimeDelta::days(7);
        orologio.set(bersaglio);
        assert_eq!(orologio.now(), bersaglio);
    }

    #[test]
    fn advance_oltre_i_limiti_satura_invece_di_traboccare() {
        let orologio = FakeClock::new(istante_base());
        orologio.advance(TimeDelta::MAX);
        assert_eq!(orologio.now(), DateTime::<Utc>::MAX_UTC);

        orologio.set(istante_base());
        orologio.advance(TimeDelta::MIN);
        assert_eq!(orologio.now(), DateTime::<Utc>::MIN_UTC);
    }

    #[test]
    fn fake_clock_e_send_e_sync() {
        let orologio = FakeClock::new(istante_base());
        richiede_send_sync(&orologio);

        // Uso reale atteso: condiviso fra piu' task dietro `Arc<dyn Clock>`.
        let condiviso: Arc<dyn Clock> = Arc::new(orologio);
        richiede_send_sync(&condiviso);
        assert_eq!(condiviso.now(), istante_base());
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

    // --- SystemClock ---

    #[test]
    fn system_clock_non_torna_indietro() {
        let orologio = SystemClock;
        richiede_send_sync(&orologio);

        let prima = orologio.now();
        let dopo = orologio.now();
        assert!(
            dopo >= prima,
            "letture consecutive decrescenti: {prima} poi {dopo}"
        );
    }
}
