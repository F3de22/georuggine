//! Codice condiviso fra server e client: tipi di dominio, protocollo, codec di rete e
//! il trait `Clock`, che permette ai test di controllare il tempo.

pub mod clock;
pub mod codec;
pub mod protocol;
pub mod types;
