//! Messaggistica del server: avvisi alla flotta, messaggi diretti e testo in arrivo.
//!
//! I tre versi passano tutti da [`MessageHub`], che fa sempre le stesse due cose nello
//! stesso ordine: **prima persiste, poi eventualmente invia**. L'ordine non e'
//! indifferente, perche' [`Repository::pending_messages`] legge dallo storico ed e' da li'
//! che un veicolo ritrova, alla connessione successiva, i messaggi arrivati mentre era
//! spento: un invio non scritto prima sparirebbe se il destinatario non fosse
//! raggiungibile.
//!
//! I metodi del [`Repository`] sono sincroni, quindi ogni scrittura passa da
//! `tokio::task::spawn_blocking`.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use georuggine_common::clock::Clock;
use georuggine_common::protocol::ServerMessage;
use georuggine_common::types::UserId;
use thiserror::Error;
use tokio::task;

use crate::net::ConnectionRegistry;
use crate::storage::{MessageDirection, MessageId, Repository, StorageError};

/// Caratteri che un messaggio puo' avere al massimo.
///
/// Si conta in caratteri e non in byte, cosi' un testo accentato non vale il doppio di
/// uno in ASCII puro. Il tetto evita che un corpo smisurato finisca nel database e nella
/// coda di scrittura di ogni connessione aperta.
pub const MAX_CARATTERI: usize = 1_024;

/// Errore restituito dalle operazioni di messaggistica.
#[derive(Debug, Error)]
pub enum MessagingError {
    /// Il corpo del messaggio supera [`MAX_CARATTERI`].
    #[error("messaggio di {lunghezza} caratteri: il massimo e' {massimo}")]
    TooLong {
        /// Caratteri che il messaggio ha davvero.
        lunghezza: usize,
        /// Caratteri ammessi, cioe' [`MAX_CARATTERI`].
        massimo: usize,
    },
    /// Errore proveniente dalla persistenza.
    #[error("errore di persistenza: {0}")]
    Storage(#[from] StorageError),
    /// L'attivita' di `spawn_blocking` che esegue la scrittura non e' arrivata in fondo.
    ///
    /// E' una variante a se' e non un riuso di [`MessagingError::Storage`] perche' il
    /// messaggio dica la causa vera invece di attribuire al database un guasto che non e'
    /// suo.
    #[error("attivita' in background non completata: {0}")]
    Task(String),
}

/// Punto unico da cui passano i messaggi del server.
///
/// Ha un solo esemplare, condiviso da tutte le connessioni come le altre dipendenze.
pub struct MessageHub {
    /// Persistenza dei messaggi, anche di quelli consegnati subito.
    repo: Arc<dyn Repository>,
    /// Connessioni aperte verso cui recapitare.
    registry: Arc<ConnectionRegistry>,
    /// Sorgente dell'ora: l'hub non chiama mai `Utc::now()` da se'.
    clock: Arc<dyn Clock>,
}

impl MessageHub {
    /// Costruisce un hub sulle dipendenze condivise del server.
    ///
    /// L'orologio va scelto uguale a quello della persistenza e del livello di rete: gli
    /// istanti scritti a storico e quelli annunciati ai client devono venire dalla stessa
    /// sorgente.
    pub fn new(
        repo: Arc<dyn Repository>,
        registry: Arc<ConnectionRegistry>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            repo,
            registry,
            clock,
        }
    }

    /// Manda un avviso a tutta la flotta, restituendo quanti destinatari ha raggiunto.
    ///
    /// Il messaggio va a storico una volta sola, senza destinatario: non e' in attesa per
    /// nessuno, e chi era spento non lo ritrova al ritorno. Un destinatario che legge
    /// troppo piano viene saltato invece di far attendere gli altri, quindi il conteggio
    /// puo' essere minore del numero di connessioni aperte.
    ///
    /// # Errori
    ///
    /// [`MessagingError::TooLong`] se il corpo supera [`MAX_CARATTERI`], e in quel caso
    /// non viene ne' scritto ne' inviato; [`MessagingError::Storage`] o
    /// [`MessagingError::Task`] se la scrittura fallisce.
    pub async fn broadcast(&self, body: &str) -> Result<usize, MessagingError> {
        let (_, sent_at) = self
            .persisti(None, MessageDirection::Broadcast, body)
            .await?;

        Ok(self
            .registry
            .broadcast_all(ServerMessage::Broadcast {
                body: body.to_owned(),
                sent_at,
            })
            .await)
    }

    /// Manda un messaggio a un solo veicolo; vero se lo ha raggiunto subito.
    ///
    /// Il falso non e' un fallimento: significa che il destinatario non ha una
    /// connessione aperta e che il messaggio, gia' scritto, lo aspetta in
    /// [`Repository::pending_messages`].
    ///
    /// # Errori
    ///
    /// [`MessagingError::TooLong`] se il corpo supera [`MAX_CARATTERI`], e in quel caso
    /// non viene ne' scritto ne' inviato; [`MessagingError::Storage`] o
    /// [`MessagingError::Task`] se la scrittura fallisce.
    pub async fn direct(&self, user_id: UserId, body: &str) -> Result<bool, MessagingError> {
        let (id, sent_at) = self
            .persisti(Some(user_id), MessageDirection::Direct, body)
            .await?;

        let consegnato = self
            .registry
            .send_to(
                user_id,
                ServerMessage::Direct {
                    body: body.to_owned(),
                    sent_at,
                },
            )
            .await;

        if consegnato {
            // Si marca il solo messaggio appena scritto, non tutta l'attesa dell'utente:
            // quelli che lo precedono possono non essere mai stati recapitati.
            self.marca_consegnato(id).await?;
        }

        Ok(consegnato)
    }

    /// Prende in carico il testo che un veicolo ha mandato al server.
    ///
    /// Non produce nessun invio: la risposta al client la decide il livello di rete con
    /// un `Ack`.
    ///
    /// # Errori
    ///
    /// [`MessagingError::TooLong`] se il corpo supera [`MAX_CARATTERI`];
    /// [`MessagingError::Storage`] o [`MessagingError::Task`] se la scrittura fallisce.
    pub async fn from_user(&self, user_id: UserId, body: &str) -> Result<(), MessagingError> {
        self.persisti(Some(user_id), MessageDirection::Inbound, body)
            .await?;

        Ok(())
    }

    /// Valida il corpo e lo scrive a storico, restituendo id e istante assegnati.
    ///
    /// L'istante torna al chiamante invece di essere riletto dall'orologio al momento
    /// dell'invio, cosi' la riga nel database e il messaggio che arriva al client portano
    /// lo stesso `sent_at`. La validazione precede lettura dell'ora e scrittura, quindi
    /// un messaggio rifiutato non lascia nessuna traccia.
    async fn persisti(
        &self,
        user_id: Option<UserId>,
        direction: MessageDirection,
        body: &str,
    ) -> Result<(MessageId, DateTime<Utc>), MessagingError> {
        valida_lunghezza(body)?;

        let adesso = self.clock.now();
        // I dati vengono duplicati perche' `spawn_blocking` sposta la chiusura su un
        // altro thread e richiede quindi che non contenga prestiti.
        let repo = Arc::clone(&self.repo);
        let corpo = body.to_owned();
        let id =
            task::spawn_blocking(move || repo.insert_message(user_id, direction, &corpo, adesso))
                .await
                .map_err(|errore| {
                    MessagingError::Task(format!("attivita' di scrittura interrotta: {errore}"))
                })??;

        Ok((id, adesso))
    }

    /// Segna consegnato il messaggio indicato.
    ///
    /// # Errori
    ///
    /// [`MessagingError::Storage`] o [`MessagingError::Task`] se la marcatura fallisce.
    /// L'errore risale anche se il messaggio era gia' partito: senza la marcatura
    /// verrebbe riproposto al prossimo accesso, e chi lo ha mandato ha diritto di saperlo.
    async fn marca_consegnato(&self, id: MessageId) -> Result<(), MessagingError> {
        let repo = Arc::clone(&self.repo);
        task::spawn_blocking(move || repo.mark_delivered(id))
            .await
            .map_err(|errore| {
                MessagingError::Task(format!("attivita' di marcatura interrotta: {errore}"))
            })??;

        Ok(())
    }
}

/// Verifica che il corpo stia dentro [`MAX_CARATTERI`].
///
/// # Errori
///
/// [`MessagingError::TooLong`] con la lunghezza vera e il massimo ammesso, cosi' il
/// livello di rete puo' riferirli al client senza ricontarli.
fn valida_lunghezza(body: &str) -> Result<(), MessagingError> {
    let lunghezza = body.chars().count();

    if lunghezza > MAX_CARATTERI {
        Err(MessagingError::TooLong {
            lunghezza,
            massimo: MAX_CARATTERI,
        })
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Mutex;

    use georuggine_common::clock::FakeClock;
    use georuggine_common::types::{PositionRecord, Username};
    use tokio::sync::mpsc;

    use crate::storage::{PendingMessage, SqliteRepository, StoredUser};

    /// Messaggi che una coda di prova puo' accodare prima di riempirsi.
    const CAPACITA: usize = 4;

    fn istante_base() -> DateTime<Utc> {
        DateTime::from_timestamp(1_704_067_200, 0).expect("istante valido")
    }

    /// Scrittura di un messaggio, come la vede [`RepositorySpia`].
    #[derive(Debug, Clone, PartialEq, Eq)]
    struct Scrittura {
        user_id: Option<UserId>,
        direction: MessageDirection,
        body: String,
        sent_at: DateTime<Utc>,
    }

    /// Repository che delega a quello vero annotando i messaggi scritti.
    ///
    /// Serve perche' nessun metodo di [`Repository`] rilegge la direzione di un messaggio
    /// ne' i messaggi senza destinatario: senza la spia, "persiste con direzione Inbound"
    /// e "il broadcast finisce comunque a storico" non sarebbero verificabili.
    struct RepositorySpia {
        interno: SqliteRepository,
        scritture: Mutex<Vec<Scrittura>>,
    }

    impl RepositorySpia {
        fn nuova(orologio: Arc<dyn Clock>) -> Self {
            Self {
                interno: SqliteRepository::new(":memory:", orologio)
                    .expect("apertura in memoria riuscita"),
                scritture: Mutex::new(Vec::new()),
            }
        }

        fn scritture(&self) -> Vec<Scrittura> {
            self.scritture
                .lock()
                .expect("il lock della spia non e' avvelenato")
                .clone()
        }
    }

    impl Repository for RepositorySpia {
        fn create_user(
            &self,
            username: &Username,
            password_hash: &str,
        ) -> Result<UserId, StorageError> {
            self.interno.create_user(username, password_hash)
        }

        fn find_user_by_username(
            &self,
            username: &Username,
        ) -> Result<Option<StoredUser>, StorageError> {
            self.interno.find_user_by_username(username)
        }

        fn insert_position(&self, record: &PositionRecord) -> Result<(), StorageError> {
            self.interno.insert_position(record)
        }

        fn positions_in_range(
            &self,
            user_id: UserId,
            from: DateTime<Utc>,
            to: DateTime<Utc>,
        ) -> Result<Vec<PositionRecord>, StorageError> {
            self.interno.positions_in_range(user_id, from, to)
        }

        fn insert_message(
            &self,
            user_id: Option<UserId>,
            direction: MessageDirection,
            body: &str,
            at: DateTime<Utc>,
        ) -> Result<MessageId, StorageError> {
            self.scritture
                .lock()
                .expect("il lock della spia non e' avvelenato")
                .push(Scrittura {
                    user_id,
                    direction,
                    body: body.to_owned(),
                    sent_at: at,
                });

            self.interno.insert_message(user_id, direction, body, at)
        }

        fn pending_messages(&self, user_id: UserId) -> Result<Vec<PendingMessage>, StorageError> {
            self.interno.pending_messages(user_id)
        }

        fn mark_delivered(&self, id: MessageId) -> Result<(), StorageError> {
            self.interno.mark_delivered(id)
        }

        fn clear_pending_messages(&self, user_id: UserId) -> Result<(), StorageError> {
            self.interno.clear_pending_messages(user_id)
        }

        fn list_users(&self) -> Result<Vec<StoredUser>, StorageError> {
            self.interno.list_users()
        }
    }

    /// Hub sotto esame e le sue dipendenze, ispezionabili dal test.
    struct Banco {
        hub: MessageHub,
        repo: Arc<RepositorySpia>,
        registry: Arc<ConnectionRegistry>,
    }

    /// Monta un hub su un repository spia e su un orologio fermo su [`istante_base`].
    fn banco() -> Banco {
        let orologio: Arc<dyn Clock> = Arc::new(FakeClock::new(istante_base()));
        let repo = Arc::new(RepositorySpia::nuova(Arc::clone(&orologio)));
        let registry = Arc::new(ConnectionRegistry::new());
        let hub = MessageHub::new(
            Arc::clone(&repo) as Arc<dyn Repository>,
            Arc::clone(&registry),
            orologio,
        );

        Banco {
            hub,
            repo,
            registry,
        }
    }

    /// Iscrive una connessione finta e restituisce la coda su cui il test la legge.
    async fn connetti(banco: &Banco, user_id: UserId) -> mpsc::Receiver<ServerMessage> {
        let (mittente, coda) = mpsc::channel(CAPACITA);
        banco.registry.register(user_id, mittente).await;
        coda
    }

    /// Vero se sulla coda non c'e' nulla da leggere.
    fn e_vuota(coda: &mut mpsc::Receiver<ServerMessage>) -> bool {
        matches!(coda.try_recv(), Err(mpsc::error::TryRecvError::Empty))
    }

    #[tokio::test]
    async fn broadcast_persiste_una_volta_sola_e_raggiunge_i_connessi() {
        let banco = banco();
        let mut primo = connetti(&banco, UserId(1)).await;
        let mut secondo = connetti(&banco, UserId(2)).await;

        let raggiunti = banco
            .hub
            .broadcast("manutenzione alle 18")
            .await
            .expect("il broadcast riesce");

        assert_eq!(raggiunti, 2, "entrambe le connessioni vanno contate");
        let atteso = ServerMessage::Broadcast {
            body: "manutenzione alle 18".to_owned(),
            sent_at: istante_base(),
        };
        assert_eq!(primo.recv().await, Some(atteso.clone()));
        assert_eq!(secondo.recv().await, Some(atteso));
        assert_eq!(
            banco.repo.scritture(),
            vec![Scrittura {
                user_id: None,
                direction: MessageDirection::Broadcast,
                body: "manutenzione alle 18".to_owned(),
                sent_at: istante_base(),
            }],
            "il broadcast va a storico una volta sola, senza destinatario"
        );
    }

    #[tokio::test]
    async fn broadcast_senza_connessi_persiste_comunque() {
        let banco = banco();

        let raggiunti = banco
            .hub
            .broadcast("chiusura turno")
            .await
            .expect("il broadcast riesce anche a flotta ferma");

        assert_eq!(raggiunti, 0);
        assert_eq!(banco.repo.scritture().len(), 1);
    }

    #[tokio::test]
    async fn direct_a_un_connesso_e_consegnato_subito_e_non_resta_in_attesa() {
        let banco = banco();
        let mut coda = connetti(&banco, UserId(1)).await;

        let consegnato = banco
            .hub
            .direct(UserId(1), "torna in sede")
            .await
            .expect("l'invio diretto riesce");

        assert!(consegnato, "un utente connesso riceve subito");
        assert_eq!(
            coda.recv().await,
            Some(ServerMessage::Direct {
                body: "torna in sede".to_owned(),
                sent_at: istante_base(),
            })
        );
        assert_eq!(
            banco.repo.scritture(),
            vec![Scrittura {
                user_id: Some(UserId(1)),
                direction: MessageDirection::Direct,
                body: "torna in sede".to_owned(),
                sent_at: istante_base(),
            }]
        );
        assert!(
            banco
                .repo
                .pending_messages(UserId(1))
                .expect("lettura riuscita")
                .is_empty(),
            "un messaggio gia' consegnato non deve tornare al prossimo accesso"
        );
    }

    #[tokio::test]
    async fn direct_a_un_assente_resta_in_attesa_e_non_va_ad_altri() {
        let banco = banco();
        // Un altro utente e' connesso: senza di lui il test non distinguerebbe un hub che
        // sceglie il destinatario sbagliato da uno che non consegna a nessuno.
        let mut altrui = connetti(&banco, UserId(1)).await;

        let consegnato = banco
            .hub
            .direct(UserId(2), "rientra in sede")
            .await
            .expect("l'invio riesce anche senza destinatario collegato");

        assert!(!consegnato, "l'utente non e' connesso");
        assert_eq!(
            banco
                .repo
                .pending_messages(UserId(2))
                .expect("lettura riuscita"),
            vec![PendingMessage {
                body: "rientra in sede".to_owned(),
                sent_at: istante_base(),
            }]
        );
        assert!(
            e_vuota(&mut altrui),
            "il messaggio non deve finire a un altro utente"
        );
    }

    #[tokio::test]
    async fn la_consegna_immediata_non_marca_i_messaggi_gia_in_attesa() {
        let banco = banco();

        // Il primo messaggio arriva a utente scollegato e resta in attesa.
        assert!(
            !banco
                .hub
                .direct(UserId(1), "rientra in sede")
                .await
                .expect("l'invio diretto riesce")
        );

        // L'utente si collega e riceve un secondo messaggio: la marcatura di questo non
        // deve dare per consegnato il primo, che nessuno gli ha ancora recapitato.
        let mut coda = connetti(&banco, UserId(1)).await;
        assert!(
            banco
                .hub
                .direct(UserId(1), "torna in sede")
                .await
                .expect("l'invio diretto riesce")
        );
        assert!(coda.recv().await.is_some());

        assert_eq!(
            banco
                .repo
                .pending_messages(UserId(1))
                .expect("lettura riuscita"),
            vec![PendingMessage {
                body: "rientra in sede".to_owned(),
                sent_at: istante_base(),
            }],
            "il messaggio mai recapitato deve restare in attesa"
        );
    }

    #[tokio::test]
    async fn from_user_persiste_senza_inviare_nulla() {
        let banco = banco();
        let mut coda = connetti(&banco, UserId(1)).await;

        banco
            .hub
            .from_user(UserId(1), "in arrivo al deposito")
            .await
            .expect("la presa in carico riesce");

        assert_eq!(
            banco.repo.scritture(),
            vec![Scrittura {
                user_id: Some(UserId(1)),
                direction: MessageDirection::Inbound,
                body: "in arrivo al deposito".to_owned(),
                sent_at: istante_base(),
            }]
        );
        assert!(
            e_vuota(&mut coda),
            "il testo va dal client al server: non genera nessun invio in uscita"
        );
    }

    #[tokio::test]
    async fn il_limite_e_incluso_si_conta_in_caratteri_e_vale_per_tutti_i_metodi() {
        let banco = banco();
        let mut coda = connetti(&banco, UserId(1)).await;
        let lungo = "a".repeat(2_000);

        for esito in [
            banco.hub.broadcast(&lungo).await.err(),
            banco.hub.direct(UserId(1), &lungo).await.err(),
            banco.hub.from_user(UserId(1), &lungo).await.err(),
        ] {
            assert!(
                matches!(
                    esito,
                    Some(MessagingError::TooLong {
                        lunghezza: 2_000,
                        massimo: MAX_CARATTERI,
                    })
                ),
                "atteso TooLong, ottenuto {esito:?}"
            );
        }
        assert!(
            banco.repo.scritture().is_empty(),
            "un messaggio rifiutato non deve essere persistito"
        );
        assert!(e_vuota(&mut coda), "un messaggio rifiutato non parte");

        // 'a' accentata e' un carattere solo ma due byte in UTF-8: un limite contato sui
        // byte rifiuterebbe questo messaggio, che di caratteri ne ha esattamente il
        // massimo.
        let accentato = "\u{e0}".repeat(MAX_CARATTERI);
        assert_eq!(accentato.len(), MAX_CARATTERI * 2, "il testo e' multibyte");
        banco
            .hub
            .from_user(UserId(1), &accentato)
            .await
            .expect("il limite e' incluso e si conta in caratteri");
        assert_eq!(banco.repo.scritture().len(), 1);
    }

    #[test]
    fn messaging_error_si_mostra_in_italiano() {
        assert_eq!(
            MessagingError::TooLong {
                lunghezza: 2_000,
                massimo: 1_024,
            }
            .to_string(),
            "messaggio di 2000 caratteri: il massimo e' 1024"
        );
        assert_eq!(
            MessagingError::Storage(StorageError::UserNotFound).to_string(),
            "errore di persistenza: utente inesistente"
        );
    }
}
