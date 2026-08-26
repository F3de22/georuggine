//! Libreria del client Georuggine.
//!
//! Come per il server, il binario in `src/main.rs` e' un guscio sottile: la logica vive
//! qui, cosi' i test la raggiungono senza passare dal processo.
//!
//! Valgono le invarianti del progetto: errori con `thiserror`, niente
//! `unwrap`/`expect`/`panic!` sui percorsi di produzione e nessun `unsafe`.

pub mod core;
pub mod sources;
pub mod tui;
