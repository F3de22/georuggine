//! Scenario end-to-end: passaggio in movimento, ritorno a fermo dopo tre minuti e stato
//! di chi si scollega. L'orologio finto condiviso fa passare i tre minuti all'istante.

mod support;

use chrono::TimeDelta;
use georuggine_common::clock::Clock as _;
use georuggine_common::protocol::{AckKind, ClientMessage, ServerMessage};
use georuggine_common::types::{STILL_AFTER_SECS, UserState};

use support::{
    ClientGrezzo, TestClient, TestServer, a_cinquanta_metri, a_un_metro, attendi_stato, partenza,
};

/// Un secondo oltre la soglia di ritorno a fermo.
fn oltre_la_soglia_di_fermo() -> TimeDelta {
    TimeDelta::seconds(i64::try_from(STILL_AFTER_SECS).expect("la soglia entra in i64") + 1)
}

#[tokio::test]
async fn uno_spostamento_oltre_la_soglia_porta_in_movimento() {
    let server = TestServer::avvia().await;
    let id = server.registra("veicolo_01").await;
    let mut cliente = TestClient::accedi_e_invia(&server, "veicolo_01").await;

    // La prima posizione della sessione non porta in movimento.
    cliente.invia_posizione(partenza()).await;
    cliente.invia_posizione(a_cinquanta_metri()).await;

    assert_eq!(cliente.attendi_cambio_di_stato().await, UserState::Moving);
    assert_eq!(server.stati.current_state(id), Some(UserState::Moving));

    cliente.chiudi().await;
    server.spegni().await;
}

#[tokio::test]
async fn uno_spostamento_sotto_la_soglia_non_cambia_stato() {
    let server = TestServer::avvia().await;
    let id = server.registra("veicolo_01").await;

    // Client grezzo: se il Pong arriva prima di un StateChanged, il cambio di stato non
    // c'e' stato.
    let mut veicolo = ClientGrezzo::connetti(&server).await;
    veicolo.saluta_e_accedi("veicolo_01").await;

    for punto in [partenza(), a_un_metro()] {
        veicolo
            .invia(ClientMessage::Position {
                coordinate: punto,
                recorded_at: server.orologio.now(),
            })
            .await;
        assert_eq!(
            veicolo.ricevi().await,
            ServerMessage::Ack {
                request: AckKind::Position
            }
        );
    }

    veicolo.invia(ClientMessage::Ping).await;
    assert_eq!(veicolo.ricevi().await, ServerMessage::Pong);
    assert_eq!(server.stati.current_state(id), Some(UserState::Still));

    server.spegni().await;
}

#[tokio::test]
async fn tre_minuti_senza_spostamenti_riportano_a_fermo() {
    let server = TestServer::avvia().await;
    let id = server.registra("veicolo_01").await;
    let mut cliente = TestClient::accedi_e_invia(&server, "veicolo_01").await;

    cliente.invia_posizione(partenza()).await;
    cliente.invia_posizione(a_cinquanta_metri()).await;
    assert_eq!(cliente.attendi_cambio_di_stato().await, UserState::Moving);

    // Senza nuovi spostamenti e' il task periodico a riportare il veicolo a fermo.
    server.avanza(oltre_la_soglia_di_fermo());

    assert_eq!(cliente.attendi_cambio_di_stato().await, UserState::Still);
    assert_eq!(server.stati.current_state(id), Some(UserState::Still));

    cliente.chiudi().await;
    server.spegni().await;
}

#[tokio::test]
async fn la_chiusura_della_sessione_segna_il_veicolo_disconnesso() {
    let server = TestServer::avvia().await;
    let id = server.registra("veicolo_01").await;
    let cliente = TestClient::accedi(&server, "veicolo_01").await;
    assert_eq!(server.stati.current_state(id), Some(UserState::Still));

    cliente.chiudi().await;

    attendi_stato(&server, id, UserState::Disconnected).await;
    server.spegni().await;
}
