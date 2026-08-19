//! Scenario end-to-end: la macchina a stati fermo/movimento vista dal veicolo.
//!
//! Copre il passaggio in movimento quando la posizione si sposta oltre la soglia (R4) e
//! il ritorno a fermo dopo tre minuti senza spostamenti (R5), piu' lo stato che il server
//! attribuisce a chi si scollega. I tre minuti non si aspettano davvero: sono tempo di
//! dominio, e l'orologio finto condiviso fra client e server li attraversa in un istante.

mod support;

use chrono::TimeDelta;
use georuggine_common::clock::Clock as _;
use georuggine_common::protocol::{AckKind, ClientMessage, ServerMessage};
use georuggine_common::types::{STILL_AFTER_SECS, UserState};

use support::{
    ClientGrezzo, TestClient, TestServer, a_cinquanta_metri, a_un_metro, attendi_stato, partenza,
};

/// Un secondo oltre la soglia di ritorno a fermo, nel tipo di chrono.
fn oltre_la_soglia_di_fermo() -> TimeDelta {
    TimeDelta::seconds(i64::try_from(STILL_AFTER_SECS).expect("la soglia entra in i64") + 1)
}

#[tokio::test]
async fn uno_spostamento_oltre_la_soglia_porta_in_movimento() {
    let server = TestServer::avvia().await;
    let id = server.registra("veicolo_01").await;
    let mut cliente = TestClient::accedi_e_invia(&server, "veicolo_01").await;

    // La prima posizione della sessione non porta mai in movimento: non c'e' un punto
    // precedente con cui misurare uno spostamento.
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

    // Qui serve un client grezzo: l'assenza di un messaggio non si dimostra aspettando,
    // ma mandando subito dopo qualcosa la cui risposta e' certa. Il canale conserva
    // l'ordine, quindi se il `Pong` arriva prima di un `StateChanged` quel cambio di
    // stato non e' stato prodotto.
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

    // Il veicolo continua a mandare la stessa coordinata, quindi nessuna posizione
    // rimette in movimento l'orologio dell'ultimo spostamento: passata la soglia, e' il
    // task periodico del server ad accorgersene e ad avvisare il diretto interessato.
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
