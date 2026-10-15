//! Scenario end-to-end: registrazione, accesso, invio delle posizioni e riepilogo, fra
//! client e server veri su TCP.

mod support;

use chrono::TimeDelta;
use georuggine_client::core::ClientCommand;
use georuggine_common::clock::Clock as _;
use georuggine_common::protocol::{AckKind, AnalysisRange, ClientMessage, ServerMessage};
use georuggine_common::types::{Coordinate, STILL_AFTER_SECS, haversine_meters};

use support::{
    ClientGrezzo, TestClient, TestServer, a_cento_metri, a_cinquanta_metri, attendi_connessi,
    partenza,
};

/// Tolleranza sulle distanze, in metri.
const TOLLERANZA_M: f64 = 0.5;

#[tokio::test]
async fn un_veicolo_nuovo_si_registra_e_apre_la_sessione() {
    let server = TestServer::avvia().await;

    let cliente = TestClient::registrandosi(&server, "veicolo_01").await;

    // Sessione aperta: l'utente esiste ed e' nel registro delle connessioni.
    let id = server.id_utente("veicolo_01");
    attendi_connessi(&server, &[id]).await;

    cliente.chiudi().await;
    server.spegni().await;
}

#[tokio::test]
async fn un_veicolo_registrato_riapre_la_sessione_e_la_chiude() {
    let server = TestServer::avvia().await;
    let id = server.registra("veicolo_01").await;

    let cliente = TestClient::accedi(&server, "veicolo_01").await;
    attendi_connessi(&server, &[id]).await;

    // Il logout toglie il veicolo dal registro delle connessioni.
    cliente.chiudi().await;
    attendi_connessi(&server, &[]).await;

    server.spegni().await;
}

#[tokio::test]
async fn le_posizioni_inviate_finiscono_nello_storico() {
    let server = TestServer::avvia().await;
    let id = server.registra("veicolo_01").await;
    let mut cliente = TestClient::accedi_e_invia(&server, "veicolo_01").await;

    // Un minuto fra un punto e l'altro: riapre anche la finestra del limite di frequenza.
    cliente.invia_posizione(partenza()).await;
    server.avanza(TimeDelta::seconds(60));
    cliente.invia_posizione(a_cinquanta_metri()).await;
    server.avanza(TimeDelta::seconds(60));
    cliente.invia_posizione(a_cento_metri()).await;

    // Il registro si svuota dopo il Logout, quindi dopo che tutte le posizioni sono
    // state elaborate.
    cliente.chiudi().await;
    attendi_connessi(&server, &[]).await;

    let storico = server.posizioni(id);
    assert!(
        storico.iter().all(|record| record.user_id == id),
        "lo storico contiene posizioni di un altro utente: {storico:?}"
    );

    // Il client ripete l'ultima coordinata a ogni invio: si verifica solo che i tre
    // punti ci siano, in ordine.
    let mut percorso: Vec<Coordinate> = storico.iter().map(|record| record.coordinate).collect();
    percorso.dedup();
    assert_eq!(
        percorso,
        vec![partenza(), a_cinquanta_metri(), a_cento_metri()]
    );

    server.spegni().await;
}

#[tokio::test]
async fn il_riepilogo_dei_movimenti_arriva_al_veicolo_che_lo_chiede() {
    let server = TestServer::avvia().await;
    server.registra("veicolo_01").await;
    let inizio = server.orologio.now();

    // Il percorso lo invia un client grezzo, per avere istanti esatti: un minuto di
    // marcia e tre minuti di sosta.
    let mut veicolo = ClientGrezzo::connetti(&server).await;
    veicolo.saluta_e_accedi("veicolo_01").await;
    invia_e_conferma(&mut veicolo, partenza(), &server).await;
    server.avanza(TimeDelta::seconds(60));
    invia_e_conferma(&mut veicolo, a_cinquanta_metri(), &server).await;
    server.avanza(TimeDelta::seconds(
        i64::try_from(STILL_AFTER_SECS).expect("la soglia entra in i64"),
    ));
    invia_e_conferma(&mut veicolo, a_cinquanta_metri(), &server).await;

    // Il veicolo e' in movimento oltre la soglia: il task periodico puo' accodare il
    // ritorno a fermo, che qui va scartato.
    veicolo.scarta_ritorno_a_fermo().await;

    veicolo.invia(ClientMessage::Logout).await;
    assert_eq!(
        veicolo.ricevi().await,
        ServerMessage::Ack {
            request: AckKind::Logout
        }
    );
    veicolo.attendi_chiusura().await;

    // L'analisi la chiede il client vero.
    let fine = server.orologio.now();
    let mut cliente = TestClient::accedi(&server, "veicolo_01").await;
    cliente
        .comanda(ClientCommand::RequestAnalysis(AnalysisRange::Custom {
            from: inizio,
            to: fine,
        }))
        .await;
    let riepilogo = cliente.attendi_analisi().await;

    let distanza_attesa = haversine_meters(partenza(), a_cinquanta_metri());
    assert!(
        (riepilogo.total_distance_meters - distanza_attesa).abs() < TOLLERANZA_M,
        "distanza {} invece di circa {distanza_attesa}",
        riepilogo.total_distance_meters
    );
    assert_eq!(riepilogo.moving_duration_secs, 60);
    assert_eq!(riepilogo.pause_duration_secs, STILL_AFTER_SECS);
    // La sosta dura esattamente la soglia, inclusa.
    assert_eq!(riepilogo.stop_count, 1);
    assert!(
        (riepilogo.average_speed_kmh - distanza_attesa / 60.0 * 3.6).abs() < 0.1,
        "velocita' media {} non coerente con distanza e tempo in movimento",
        riepilogo.average_speed_kmh
    );
    assert_eq!(
        riepilogo
            .route
            .iter()
            .map(|record| record.coordinate)
            .collect::<Vec<_>>(),
        vec![partenza(), a_cinquanta_metri(), a_cinquanta_metri()]
    );

    cliente.chiudi().await;
    server.spegni().await;
}

/// Invia una posizione all'ora corrente del dominio e attende l'Ack, scartando un
/// eventuale cambio di stato.
async fn invia_e_conferma(veicolo: &mut ClientGrezzo, dove: Coordinate, server: &TestServer) {
    veicolo
        .invia(ClientMessage::Position {
            coordinate: dove,
            recorded_at: server.orologio.now(),
        })
        .await;

    loop {
        match veicolo.ricevi().await {
            ServerMessage::Ack {
                request: AckKind::Position,
            } => return,
            ServerMessage::StateChanged { .. } => {}
            altro => panic!("risposta inattesa a una posizione: {altro:?}"),
        }
    }
}
