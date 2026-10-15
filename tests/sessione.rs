//! Scenario end-to-end: ciclo di vita di una sessione.
//!
//! Un secondo accesso con lo stesso utente chiude la sessione precedente, che riceve
//! [`SESSION_REPLACED_MESSAGE`] e non si riconnette. Coperti anche logout e spegnimento
//! del server.

mod support;

use georuggine_common::protocol::SESSION_REPLACED_MESSAGE;
use georuggine_common::types::UserState;

use support::{TestClient, TestServer, attendi_connessi, attendi_stato};

#[tokio::test]
async fn una_seconda_connessione_dello_stesso_utente_sostituisce_la_prima() {
    let server = TestServer::avvia().await;
    let id = server.registra("veicolo_01").await;
    let mut primo = TestClient::accedi(&server, "veicolo_01").await;

    // Lo stesso account si collega da un'altra parte.
    let mut secondo = TestClient::accedi(&server, "veicolo_01").await;

    // Il primo client riceve il motivo e non si riconnette.
    assert_eq!(primo.attendi_caduta().await, SESSION_REPLACED_MESSAGE);
    primo.attendi_fine().await;

    // Nel registro resta solo la connessione nuova, raggiungibile con un diretto.
    attendi_connessi(&server, &[id]).await;
    let consegnato = server
        .messaggistica
        .direct(id, "rientra in sede")
        .await
        .expect("il messaggio viene accettato");
    assert!(consegnato, "la sessione nuova deve essere raggiungibile");
    assert_eq!(secondo.attendi_testo(true).await, "rientra in sede");

    primo.chiudi().await;
    secondo.chiudi().await;
    server.spegni().await;
}

#[tokio::test]
async fn il_logout_libera_la_sessione() {
    let server = TestServer::avvia().await;
    let id = server.registra("veicolo_01").await;
    let cliente = TestClient::accedi(&server, "veicolo_01").await;
    attendi_connessi(&server, &[id]).await;

    cliente.chiudi().await;

    // Il logout libera subito la sessione.
    attendi_connessi(&server, &[]).await;
    attendi_stato(&server, id, UserState::Disconnected).await;

    // Liberata la sessione, lo stesso utente puo' rientrare.
    let di_nuovo = TestClient::accedi(&server, "veicolo_01").await;
    attendi_connessi(&server, &[id]).await;

    di_nuovo.chiudi().await;
    server.spegni().await;
}

#[tokio::test]
async fn due_veicoli_diversi_restano_collegati_insieme() {
    let server = TestServer::avvia().await;
    let primo_id = server.registra("veicolo_01").await;
    let secondo_id = server.registra("veicolo_02").await;

    let primo = TestClient::accedi(&server, "veicolo_01").await;
    let secondo = TestClient::accedi(&server, "veicolo_02").await;

    // Una sessione per utente, non una per server.
    attendi_connessi(&server, &[primo_id, secondo_id]).await;

    primo.chiudi().await;
    attendi_connessi(&server, &[secondo_id]).await;

    secondo.chiudi().await;
    server.spegni().await;
}

#[tokio::test]
async fn lo_spegnimento_del_server_chiude_le_sessioni_aperte() {
    let server = TestServer::avvia().await;
    server.registra("veicolo_01").await;
    let mut cliente = TestClient::accedi(&server, "veicolo_01").await;

    // Lo spegnimento chiude anche le connessioni gia' aperte.
    server.spegni().await;

    let motivo = cliente.attendi_caduta().await;
    assert!(
        !motivo.is_empty(),
        "anche una chiusura decisa dal server arriva con un motivo da mostrare"
    );

    // Il client riprova: un server spento non si distingue da una caduta di rete.
    cliente.chiudi().await;
}
