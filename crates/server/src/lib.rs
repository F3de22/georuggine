//! Libreria del server Georuggine.
//!
//! Il binario in `src/main.rs` e' un guscio sottile: la logica vive qui, cosi' i test
//! (unitari e di integrazione) possono raggiungerla senza passare dal processo.
//!
//! Valgono le invarianti del progetto: errori con `thiserror`, niente
//! `unwrap`/`expect`/`panic!` sui percorsi di produzione e nessun `unsafe` fuori dal
//! modulo [`cpu_time`].

pub mod auth;
pub mod cpu_time;
pub mod storage;
