//! Codice condiviso fra il server e il client Georuggine: tipi di dominio, protocollo
//! di rete con il relativo codec e il trait `Clock`, che la logica di dominio usa al
//! posto di `Utc::now()`.
//!
//! Essendo una libreria, gli errori si modellano con `thiserror` e non con `anyhow`, e
//! non ci sono `unwrap`/`expect`/`panic!` sui percorsi di produzione.

pub mod clock;
pub mod codec;
pub mod protocol;
pub mod types;
