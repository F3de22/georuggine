//! Scenario end-to-end: broadcast a tutta la flotta, messaggi diretti anche a utenti
//! scollegati e testi dei veicoli verso il server.

mod support;

use std::time::Duration;

use georuggine_client::core::{ClientCommand, ClientEvent};
use georuggine_common::types::UserId;
use georuggine_server::messaging::MAX_CARATTERI;
use georuggine_server::storage::{MessageDirection, Repository as _};
use tokio::time::{Instant, sleep};

use support::{ATTESA, MessaggioSpiato, TestClient, TestServer, attendi_messaggio};

/// Intervallo fra due controlli di una condizione.
const PASSO_DI_ATTESA: Duration = Duration::from_millis(10);

#[tokio::test]
async fn il_testo_di_un_veicolo_arriva_al_server() {
    let server = TestServer::avvia().await;
    let id = server.registra("veicolo_01").await;
    let cliente = TestClient::accedi(&server, "veicolo_01").await;

    cliente
        .comanda(ClientCommand::SendText("in arrivo al deposito".to_owned()))
        .await;

    // Il client riceve solo un Ack: si verifica la riga salvata.
    attendi_messaggio(&server, MessageDirection::Inbound, "in arrivo al deposito").await;
    assert!(
        server.archivio.messaggi().contains(&MessaggioSpiato {
            destinatario: Some(id),
            verso: MessageDirection::Inbound,
            corpo: "in arrivo al deposito".to_owned(),
        }),
        "il testo non risulta attribuito a chi lo ha mandato"
    );

    cliente.chiudi().await;
    server.spegni().await;
}

#[tokio::test]
async fn un_testo_troppo_lungo_viene_rifiutato_senza_chiudere_la_sessione() {
    let server = TestServer::avvia().await;
    let id = server.registra("veicolo_01").await;
    let mut cliente = TestClient::accedi(&server, "veicolo_01").await;

    cliente
        .comanda(ClientCommand::SendText("a".repeat(MAX_CARATTERI + 1)))
        .await;

    let errore = cliente.attendi_errore().await;
    assert!(
        errore.contains(&MAX_CARATTERI.to_string()),
        "l'errore non dice qual e' il limite: {errore}"
    );
    // Il rifiuto riguarda la singola richiesta: un testo valido subito dopo passa.
    cliente
        .comanda(ClientCommand::SendText("riprovo".to_owned()))
        .await;
    attendi_messaggio(&server, MessageDirection::Inbound, "riprovo").await;
    assert_eq!(server.registro.connected_users().await, vec![id]);

    cliente.chiudi().await;
    server.spegni().await;
}

#[tokio::test]
async fn un_avviso_raggiunge_tutta_la_flotta() {
    let server = TestServer::avvia().await;
    server.registra("veicolo_01").await;
    server.registra("veicolo_02").await;
    let mut primo = TestClient::accedi(&server, "veicolo_01").await;
    let mut secondo = TestClient::accedi(&server, "veicolo_02").await;

    let raggiunti = server
        .messaggistica
        .broadcast("manutenzione alle 18")
        .await
        .expect("l'avviso viene accettato");

    assert_eq!(raggiunti, 2, "l'avviso non ha raggiunto entrambi i veicoli");
    assert_eq!(primo.attendi_testo(false).await, "manutenzione alle 18");
    assert_eq!(secondo.attendi_testo(false).await, "manutenzione alle 18");

    primo.chiudi().await;
    secondo.chiudi().await;
    server.spegni().await;
}

#[tokio::test]
async fn un_messaggio_diretto_arriva_al_solo_destinatario() {
    let server = TestServer::avvia().await;
    let id = server.registra("veicolo_01").await;
    server.registra("veicolo_02").await;
    let mut destinatario = TestClient::accedi(&server, "veicolo_01").await;
    let mut estraneo = TestClient::accedi(&server, "veicolo_02").await;

    let consegnato = server
        .messaggistica
        .direct(id, "rientra in sede")
        .await
        .expect("il messaggio viene accettato");

    assert!(consegnato, "il destinatario e' collegato");
    assert_eq!(destinatario.attendi_testo(true).await, "rientra in sede");

    // Un broadcast subito dopo dimostra che l'altro veicolo non ha ricevuto il diretto:
    // il canale conserva l'ordine.
    server
        .messaggistica
        .broadcast("manutenzione alle 18")
        .await
        .expect("l'avviso viene accettato");
    let primo_testo = estraneo
        .attendi("un testo qualunque", |evento| {
            matches!(evento, ClientEvent::ServerText { .. })
        })
        .await;
    assert_eq!(
        primo_testo,
        ClientEvent::ServerText {
            body: "manutenzione alle 18".to_owned(),
            direct: false,
        }
    );

    destinatario.chiudi().await;
    estraneo.chiudi().await;
    server.spegni().await;
}

#[tokio::test]
async fn i_messaggi_arrivati_da_scollegato_si_ricevono_al_ritorno() {
    let server = TestServer::avvia().await;
    let id = server.registra("veicolo_01").await;

    // Il veicolo non si e' mai collegato.
    let consegnato = server
        .messaggistica
        .direct(id, "passa dall'officina")
        .await
        .expect("il messaggio viene accettato");
    assert!(!consegnato, "nessuno e' collegato con quell'utente");
    assert_eq!(
        server
            .archivio
            .pending_messages(id)
            .expect("lettura dei messaggi in attesa riuscita")
            .len(),
        1
    );

    // All'accesso il server consegna i messaggi in attesa.
    let mut cliente = TestClient::accedi(&server, "veicolo_01").await;
    assert_eq!(cliente.attendi_testo(true).await, "passa dall'officina");
    attendi_attesa_vuota(&server, id).await;

    cliente.chiudi().await;
    server.spegni().await;
}

/// Attende che l'utente non abbia piu' messaggi in attesa: la marcatura segue l'invio.
async fn attendi_attesa_vuota(server: &TestServer, utente: UserId) {
    let scadenza = Instant::now() + ATTESA;

    while Instant::now() < scadenza {
        let in_attesa = server
            .archivio
            .pending_messages(utente)
            .expect("lettura dei messaggi in attesa riuscita");
        if in_attesa.is_empty() {
            return;
        }
        sleep(PASSO_DI_ATTESA).await;
    }

    panic!("i messaggi consegnati risultano ancora in attesa dopo {ATTESA:?}");
}
