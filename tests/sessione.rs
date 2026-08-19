//! Scenario end-to-end: ciclo di vita di una sessione.
//!
//! Il server ammette una sola sessione per utente: se lo stesso account si presenta da
//! un'altra parte, la connessione precedente viene chiusa. La chiusura non e' pero' muta,
//! ed e' questo il punto delicato: il client sostituito riceve prima un errore con il
//! testo [`SESSION_REPLACED_MESSAGE`], lo riconosce e **non** si riconnette. Senza quel
//! discriminante vedrebbe solo la fine del flusso, non distinguibile da una caduta di
//! rete, e i due client si espellerebbero a vicenda senza fine.
//!
//! Lo scenario copre anche le altre due uscite: il logout, che libera la sessione, e lo
//! spegnimento del server, che chiude quelle aperte.

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

    // Il primo apprende il motivo della chiusura e smette: se si riconnettesse
    // espellerebbe la connessione che ha preso il suo posto, in un ciclo senza fine.
    assert_eq!(primo.attendi_caduta().await, SESSION_REPLACED_MESSAGE);
    primo.attendi_fine().await;

    // Nel registro resta una sola voce, ed e' quella della connessione nuova: un
    // messaggio diretto la raggiunge.
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

    // Il logout sgancia la sessione e toglie la voce dal registro, senza aspettare la
    // scadenza di inattivita'; la macchina a stati ne prende atto.
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

    // Il limite e' una sessione per utente, non una connessione per server.
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

    // Lo spegnimento non riguarda solo l'accettazione di nuove connessioni: anche quelle
    // gia' aperte ricevono il segnale e si chiudono.
    server.spegni().await;

    let motivo = cliente.attendi_caduta().await;
    assert!(
        !motivo.is_empty(),
        "anche una chiusura decisa dal server arriva con un motivo da mostrare"
    );

    // Da qui in poi il client riprova con il backoff, perche' una caduta di rete non e'
    // distinguibile da un server spento: e' l'utente a decidere quando smettere.
    cliente.chiudi().await;
}
