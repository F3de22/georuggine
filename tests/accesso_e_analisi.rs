//! Scenario end-to-end: registrazione, accesso, invio delle posizioni e riepilogo.
//!
//! Copre il percorso principale della traccia dal punto di vista di un veicolo che entra
//! in servizio: si registra (R1), riapre la sessione con le proprie credenziali (R2),
//! manda le posizioni che il server persiste (R3) e chiede il riepilogo dei propri
//! movimenti (R10). Tutto passa da un socket TCP fra il client vero e il server vero.

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

/// Tolleranza sulle distanze in metri: la formula del semiverso e la coordinata scelta
/// dal test non cadono su un valore tondo, ma la differenza e' ben sotto il metro.
const TOLLERANZA_M: f64 = 0.5;

#[tokio::test]
async fn un_veicolo_nuovo_si_registra_e_apre_la_sessione() {
    let server = TestServer::avvia().await;

    // `registrandosi` manda `Register` sul filo: l'utente non esiste prima di questa riga.
    let cliente = TestClient::registrandosi(&server, "veicolo_01").await;

    // La sessione e' aperta, quindi il server ha creato l'utente e ne ha registrato la
    // connessione: sono le due meta' del requisito R1.
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

    // Il logout non e' solo la chiusura del socket: il server sgancia la sessione e
    // toglie il veicolo dal registro delle connessioni.
    cliente.chiudi().await;
    attendi_connessi(&server, &[]).await;

    server.spegni().await;
}

#[tokio::test]
async fn le_posizioni_inviate_finiscono_nello_storico() {
    let server = TestServer::avvia().await;
    let id = server.registra("veicolo_01").await;
    let mut cliente = TestClient::accedi_e_invia(&server, "veicolo_01").await;

    // Fra un punto e il successivo passa un minuto di tempo di dominio: e' anche cio' che
    // riapre la finestra del limitatore di frequenza, che il ticker riempirebbe di
    // duplicati mentre l'orologio sta fermo.
    cliente.invia_posizione(partenza()).await;
    server.avanza(TimeDelta::seconds(60));
    cliente.invia_posizione(a_cinquanta_metri()).await;
    server.avanza(TimeDelta::seconds(60));
    cliente.invia_posizione(a_cento_metri()).await;

    // La conferma di invio dice che la posizione e' partita dal client, non che il server
    // l'abbia gia' scritta: il registro che si svuota e' invece un fatto che avviene dopo
    // il `Logout`, e quindi dopo tutto cio' che lo precedeva sulla stessa connessione.
    cliente.chiudi().await;
    attendi_connessi(&server, &[]).await;

    let storico = server.posizioni(id);
    assert!(
        storico.iter().all(|record| record.user_id == id),
        "lo storico contiene posizioni di un altro utente: {storico:?}"
    );

    // Il ticker ripete l'ultima coordinata nota finche' non ne arriva una nuova, quindi
    // lo storico contiene anche dei duplicati consecutivi: cio' che il requisito R3
    // chiede e' che i tre punti ci siano tutti e in quest'ordine.
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

    // Il percorso lo pianta un client grezzo, un punto per volta: e' l'unico modo di
    // avere istanti esatti su cui verificare le durate del riepilogo, perche' il ticker
    // del client vero ripete l'ultima coordinata a ogni scatto e quei duplicati
    // sposterebbero i confini dei segmenti. Il tragitto e' un minuto di marcia seguito da
    // una sosta di tre minuti.
    let mut veicolo = ClientGrezzo::connetti(&server).await;
    veicolo.saluta_e_accedi("veicolo_01").await;
    invia_e_conferma(&mut veicolo, partenza(), &server).await;
    server.avanza(TimeDelta::seconds(60));
    invia_e_conferma(&mut veicolo, a_cinquanta_metri(), &server).await;
    server.avanza(TimeDelta::seconds(
        i64::try_from(STILL_AFTER_SECS).expect("la soglia entra in i64"),
    ));
    invia_e_conferma(&mut veicolo, a_cinquanta_metri(), &server).await;

    // La terza posizione lascia il veicolo in movimento oltre la soglia del requisito R5:
    // da qui il battito della macchina a stati e' nel suo diritto di annunciare il ritorno
    // a fermo, e lo accoda dove il test si aspetta l'`Ack` del logout. Non e' un difetto
    // del server, ma un messaggio legittimo che questo scenario non sta verificando:
    // quello di stati e' `stati`. Lo si toglie di mezzo qui, dove la coda non puo'
    // contenere altro perche' nessuna richiesta e' in volo.
    veicolo.scarta_ritorno_a_fermo().await;

    veicolo.invia(ClientMessage::Logout).await;
    assert_eq!(
        veicolo.ricevi().await,
        ServerMessage::Ack {
            request: AckKind::Logout
        }
    );
    veicolo.attendi_chiusura().await;

    // La richiesta la fa invece il client vero, che e' chi la fara' in servizio.
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
    // La sosta dura esattamente la soglia del requisito R5, che e' inclusa.
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

/// Manda una posizione datata all'ora corrente del dominio e attende la conferma.
///
/// Un eventuale [`ServerMessage::StateChanged`] viene scartato: segue l'`Ack` quando la
/// posizione fa passare il veicolo in movimento, non e' una risposta alla richiesta, e di
/// stati parla lo scenario `stati`.
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
