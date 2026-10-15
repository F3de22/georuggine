//! Scenario end-to-end: caduta della connessione e rientro del veicolo con le posizioni
//! accumulate.
//!
//! La caduta si provoca togliendo la connessione dal registro del server, che chiude il
//! socket senza dover spegnere il server.

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

/// Attesa prima del rientro, abbastanza lunga da accumulare piu' posizioni.
const RITARDO_DI_RIENTRO: Duration = Duration::from_millis(1_200);

/// Punto circa duecento metri a nord della partenza, terzo e ultimo della coda.
fn a_duecento_metri() -> Coordinate {
    coordinata(45.0703 + 0.001_8, 7.6869)
}

/// Tempi di un client che accumula posizioni mentre e' scollegato: rientro lento rispetto
/// all'invio.
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
    // Il task di rete si riconnette da solo con le stesse credenziali.
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

    // Il backoff riparte da capo a ogni sessione riaperta.
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

    // Fra un punto e l'altro passa piu' di un intervallo, cosi' ognuno finisce in coda.
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

    // I primi due punti possono arrivare solo dalla coda: la sorgente ormai restituisce
    // l'ultimo.
    for atteso in [a_cinquanta_metri(), a_cento_metri(), a_duecento_metri()] {
        cliente
            .attendi(
                "l'invio di una posizione accumulata",
                |evento| matches!(evento, ClientEvent::PositionSent(dove, _) if *dove == atteso),
            )
            .await;
    }

    // Il registro si svuota dopo il Logout, quindi dopo tutte le posizioni.
    cliente.chiudi().await;
    attendi_connessi(&server, &[]).await;

    let mut percorso: Vec<Coordinate> = server
        .posizioni(id)
        .iter()
        .map(|record| record.coordinate)
        .collect();
    percorso.dedup();
    // All'inizio puo' esserci anche la posizione di partenza.
    assert!(
        percorso.ends_with(&[a_cinquanta_metri(), a_cento_metri(), a_duecento_metri()]),
        "lo storico non ha ricevuto in ordine le posizioni accumulate: {percorso:?}"
    );

    server.spegni().await;
}
