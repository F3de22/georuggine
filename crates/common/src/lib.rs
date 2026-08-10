//! Codice condiviso fra il server e il client Georuggine.
//!
//! Qui vivono i tipi di dominio, il protocollo di rete con il relativo codec e le
//! astrazioni trasversali (fra cui il trait `Clock`, che la logica di dominio usa al
//! posto di `Utc::now()`).
//!
//! Questo crate e' una libreria: gli errori si modellano con `thiserror`, mai con
//! `anyhow`, e non deve contenere `unwrap`/`expect`/`panic!` sui percorsi di
//! produzione.

pub mod types;
