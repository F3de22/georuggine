//! Scenario end-to-end: caduta della connessione e rientro del veicolo.
//!
//! Il client non deve arrendersi a una connessione che cade: riprova con il backoff e
//! riapre la sessione da solo. Nel frattempo continua a interrogare la sorgente di
//! posizione e accumula in una coda locale cio' che non puo' inviare, che parte in ordine
//! non appena la connessione torna.
//!
//! La caduta si provoca togliendo la connessione dal [`ConnectionRegistry`] del server:
//! il registro e' il proprietario del canale di scrittura, quindi la voce che sparisce
//! chiude davvero il socket, esattamente come nella sostituzione di una sessione. E' il
//! modo piu' diretto di far vedere al client una caduta senza spegnere il server, che
//! costringerebbe a rilegare la stessa porta.
//!
//! [`ConnectionRegistry`]: georuggine_server::net::ConnectionRegistry

mod support;

use std::time::Duration;

use georuggine_client::core::{ClientCommand, ClientEvent, Tempi};
use georuggine_common::types::Coordinate;
use tokio::time::sleep;

use support::{
    PASSWORD, TestClient, TestServer, a_cento_metri, a_cinquanta_metri, attendi_connessi,
    coordinata,
};

/// Intervallo fra due interrogazioni della sorgente mentre la connessione e' caduta.
const INTERVALLO_POSIZIONE: Duration = Duration::from_millis(300);

/// Attesa prima del rientro, scelta perche' ci stiano dentro piu' interrogazioni della
/// sorgente: e' la finestra in cui la coda locale si riempie.
const RITARDO_DI_RIENTRO: Duration = Duration::from_millis(1_200);

/// Punto circa duecento metri a nord della partenza, terzo e ultimo della coda.
fn a_duecento_metri() -> Coordinate {
    coordinata(45.0703 + 0.001_8, 7.6869)
}

/// Tempi di un client che accumula posizioni mentre e' scollegato.
///
/// Il rientro e' volutamente lento rispetto al ticker: senza quel margine la connessione
/// tornerebbe prima che la sorgente sia stata interrogata, e non ci sarebbe nessuna coda
/// da svuotare.
fn tempi_con_coda() -> Tempi {
    Tempi {
        intervallo_posizione: INTERVALLO_POSIZIONE,
        backoff: [RITARDO_DI_RIENTRO; 4],
        backoff_massimo: RITARDO_DI_RIENTRO,
    }
}

#[tokio::test]
async fn il_veicolo_rientra_dopo_una_caduta() {
    let server = TestServer::avvia().await;
    let id = server.registra("veicolo_01").await;
    let mut cliente = TestClient::accedi(&server, "veicolo_01").await;

    server.registro.unregister(id).await;

    let motivo = cliente.attendi_caduta().await;
    assert!(
        !motivo.is_empty(),
        "la caduta deve arrivare con un motivo da mostrare"
    );
    // Nessuno rifa' l'accesso da fuori: il task di rete si riconnette e riapre la
    // sessione con le credenziali che ha gia'.
    cliente.attendi_connessione().await;
    attendi_connessi(&server, &[id]).await;

    cliente.chiudi().await;
    server.spegni().await;
}

#[tokio::test]
async fn il_veicolo_rientra_anche_dopo_piu_cadute() {
    let server = TestServer::avvia().await;
    let id = server.registra("veicolo_01").await;
    let mut cliente = TestClient::accedi(&server, "veicolo_01").await;

    // Il backoff riparte da capo a ogni sessione riaperta: una seconda caduta si tratta
    // come la prima, non come il proseguimento della progressione.
    for _ in 0..2 {
        server.registro.unregister(id).await;
        cliente.attendi_caduta().await;
        cliente.attendi_connessione().await;
        attendi_connessi(&server, &[id]).await;
    }

    cliente.chiudi().await;
    server.spegni().await;
}

#[tokio::test]
async fn le_posizioni_prodotte_da_scollegato_partono_al_rientro() {
    let server = TestServer::avvia().await;
    let id = server.registra("veicolo_01").await;
    let mut cliente = TestClient::avvia(&server, "veicolo_01", PASSWORD, false, tempi_con_coda());
    cliente.attendi_connessione().await;

    server.registro.unregister(id).await;
    cliente.attendi_caduta().await;

    // Il veicolo continua a muoversi mentre nessuno lo ascolta. Fra un punto e l'altro
    // passa piu' di un intervallo, cosi' ognuno viene davvero interrogato e finisce in
    // coda: la sorgente manuale tiene solo l'ultima coordinata ricevuta.
    for punto in [a_cinquanta_metri(), a_cento_metri()] {
        cliente
            .comanda(ClientCommand::SetManualPosition(punto))
            .await;
        sleep(INTERVALLO_POSIZIONE + Duration::from_millis(50)).await;
    }
    cliente
        .comanda(ClientCommand::SetManualPosition(a_duecento_metri()))
        .await;

    cliente.attendi_connessione().await;

    // I primi due punti sono la prova che la coda esiste: la sorgente non li restituisce
    // piu', quindi possono venire solo da cio' che era stato accumulato. L'ordine e'
    // quello di produzione.
    for atteso in [a_cinquanta_metri(), a_cento_metri(), a_duecento_metri()] {
        cliente
            .attendi(
                "l'invio di una posizione accumulata",
                |evento| matches!(evento, ClientEvent::PositionSent(dove) if *dove == atteso),
            )
            .await;
    }

    // La conferma di invio dice che la posizione e' partita dal client, non che il server
    // l'abbia gia' scritta: il registro che si svuota e' invece un fatto che avviene dopo
    // il `Logout`, e quindi dopo tutto cio' che lo precedeva sulla stessa connessione.
    cliente.chiudi().await;
    attendi_connessi(&server, &[]).await;

    let mut percorso: Vec<Coordinate> = server
        .posizioni(id)
        .iter()
        .map(|record| record.coordinate)
        .collect();
    percorso.dedup();
    // Non si confronta il percorso per intero: se il ticker fa in tempo a scattare prima
    // della caduta, davanti c'e' anche la coordinata di partenza, che qui non dice nulla.
    assert!(
        percorso.ends_with(&[a_cinquanta_metri(), a_cento_metri(), a_duecento_metri()]),
        "lo storico non ha ricevuto in ordine le posizioni accumulate: {percorso:?}"
    );

    server.spegni().await;
}
