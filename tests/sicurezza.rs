//! Scenario end-to-end: richieste che il server rifiuta, inviate da un client grezzo.
//!
//! Una violazione della sequenza chiude la connessione; il rifiuto di una singola
//! richiesta la lascia aperta.

mod support;

use chrono::TimeDelta;
use georuggine_common::clock::Clock as _;
use georuggine_common::protocol::{
    AckKind, ClientMessage, ErrorCode, PROTOCOL_VERSION, ServerMessage,
};
use georuggine_common::types::Username;
use georuggine_server::storage::Repository as _;

use support::{
    ClientGrezzo, PASSWORD, PASSWORD_DEBOLE, TestClient, TestServer, partenza, tempi_fermi, utente,
};

/// Posizioni massime inviate prima di concludere che il limite di frequenza non esiste.
const POSIZIONI_MASSIME: u32 = 200;

#[tokio::test]
async fn una_versione_di_protocollo_sconosciuta_chiude_la_connessione() {
    let server = TestServer::avvia().await;
    let mut estraneo = ClientGrezzo::connetti(&server).await;

    estraneo
        .invia(ClientMessage::Hello {
            protocol_version: PROTOCOL_VERSION + 1,
        })
        .await;

    match estraneo.ricevi().await {
        ServerMessage::Error { code, .. } => assert_eq!(code, ErrorCode::UnsupportedVersion),
        altro => panic!("risposta inattesa a una versione sconosciuta: {altro:?}"),
    }
    // Versione non supportata: il server chiude.
    estraneo.attendi_chiusura().await;

    server.spegni().await;
}

#[tokio::test]
async fn la_connessione_si_apre_con_hello_e_non_con_altro() {
    let server = TestServer::avvia().await;
    server.registra("veicolo_01").await;
    let mut estraneo = ClientGrezzo::connetti(&server).await;

    // Credenziali corrette ma prima dell'handshake: sequenza violata.
    estraneo
        .invia(ClientMessage::Login {
            username: utente("veicolo_01"),
            password: PASSWORD.to_owned(),
        })
        .await;

    match estraneo.ricevi().await {
        ServerMessage::Error { code, .. } => assert_eq!(code, ErrorCode::NotAuthenticated),
        altro => panic!("risposta inattesa a un primo messaggio diverso da Hello: {altro:?}"),
    }
    estraneo.attendi_chiusura().await;

    server.spegni().await;
}

#[tokio::test]
async fn una_richiesta_prima_dell_accesso_e_rifiutata_senza_chiudere() {
    let server = TestServer::avvia().await;
    server.registra("veicolo_01").await;
    let mut veicolo = ClientGrezzo::connetti(&server).await;
    veicolo.saluta().await;

    veicolo
        .invia(ClientMessage::Position {
            coordinate: partenza(),
            recorded_at: server.orologio.now(),
        })
        .await;

    match veicolo.ricevi().await {
        ServerMessage::Error { code, .. } => assert_eq!(code, ErrorCode::NotAuthenticated),
        altro => panic!("risposta inattesa a una richiesta senza sessione: {altro:?}"),
    }
    // La connessione resta aperta in attesa dell'accesso.
    veicolo
        .invia(ClientMessage::Login {
            username: utente("veicolo_01"),
            password: PASSWORD.to_owned(),
        })
        .await;
    assert!(matches!(
        veicolo.ricevi().await,
        ServerMessage::SessionToken { .. }
    ));

    server.spegni().await;
}

#[tokio::test]
async fn una_password_sbagliata_non_apre_la_sessione_e_non_si_ripete() {
    let server = TestServer::avvia().await;
    server.registra("veicolo_01").await;

    let mut cliente = TestClient::avvia(&server, "veicolo_01", "sbagliata99", false, tempi_fermi());

    let errore = cliente.attendi_errore().await;
    assert_eq!(errore, ErrorCode::InvalidCredentials.to_string());
    // Credenziali rifiutate: il task di rete termina senza riprovare.
    cliente.attendi_fine().await;
    assert_eq!(server.registro.connected_users().await, Vec::new());

    server.spegni().await;
}

#[tokio::test]
async fn uno_username_gia_in_uso_non_si_registra() {
    let server = TestServer::avvia().await;
    server.registra("veicolo_01").await;

    let mut intruso = TestClient::avvia(&server, "veicolo_01", PASSWORD, true, tempi_fermi());

    let errore = intruso.attendi_errore().await;
    assert_eq!(errore, ErrorCode::UsernameTaken.to_string());
    intruso.attendi_fine().await;

    server.spegni().await;
}

#[tokio::test]
async fn una_password_troppo_debole_non_si_registra() {
    let server = TestServer::avvia().await;

    let mut cliente =
        TestClient::avvia(&server, "veicolo_01", PASSWORD_DEBOLE, true, tempi_fermi());

    let errore = cliente.attendi_errore().await;
    assert!(
        errore.contains("password troppo debole"),
        "l'errore non spiega perche' la password non va bene: {errore}"
    );
    cliente.attendi_fine().await;
    // La validazione precede ogni accesso al database.
    assert!(
        server
            .archivio
            .find_user_by_username(&Username::new("veicolo_01").expect("username valido"))
            .expect("lettura dell'utente riuscita")
            .is_none()
    );

    server.spegni().await;
}

#[tokio::test]
async fn una_posizione_datata_nel_futuro_e_rifiutata_senza_chiudere() {
    let server = TestServer::avvia().await;
    server.registra("veicolo_01").await;
    let mut veicolo = ClientGrezzo::connetti(&server).await;
    veicolo.saluta_e_accedi("veicolo_01").await;

    // Dieci minuti di anticipo superano la tolleranza sulla deriva dell'orologio.
    veicolo
        .invia(ClientMessage::Position {
            coordinate: partenza(),
            recorded_at: server.orologio.now() + TimeDelta::seconds(600),
        })
        .await;

    match veicolo.ricevi().await {
        ServerMessage::Error { code, .. } => assert_eq!(code, ErrorCode::MalformedMessage),
        altro => panic!("risposta inattesa a una posizione nel futuro: {altro:?}"),
    }
    // Rifiuto della singola richiesta: la sessione resta aperta.
    veicolo.invia(ClientMessage::Ping).await;
    assert_eq!(veicolo.ricevi().await, ServerMessage::Pong);
    assert!(
        server.posizioni(server.id_utente("veicolo_01")).is_empty(),
        "una posizione rifiutata non deve finire nello storico"
    );

    server.spegni().await;
}

#[tokio::test]
async fn troppe_posizioni_in_poco_tempo_vengono_limitate() {
    let server = TestServer::avvia().await;
    server.registra("veicolo_01").await;
    let mut veicolo = ClientGrezzo::connetti(&server).await;
    veicolo.saluta_e_accedi("veicolo_01").await;

    // Con l'orologio fermo la finestra non si riapre; posizioni identiche per non
    // avere cambi di stato fra le risposte.
    let mut confermate: u32 = 0;
    let codice = loop {
        veicolo
            .invia(ClientMessage::Position {
                coordinate: partenza(),
                recorded_at: server.orologio.now(),
            })
            .await;

        match veicolo.ricevi().await {
            ServerMessage::Ack {
                request: AckKind::Position,
            } => {
                confermate += 1;
                assert!(
                    confermate < POSIZIONI_MASSIME,
                    "{confermate} posizioni accettate di fila: il limite di frequenza non scatta"
                );
            }
            ServerMessage::Error { code, .. } => break code,
            altro => panic!("risposta inattesa a una posizione: {altro:?}"),
        }
    };

    assert_eq!(codice, ErrorCode::RateLimited);
    assert!(
        confermate > 0,
        "il limite non deve scattare prima della prima posizione"
    );
    // Non chiude la connessione.
    veicolo.invia(ClientMessage::Ping).await;
    assert_eq!(veicolo.ricevi().await, ServerMessage::Pong);

    server.spegni().await;
}
