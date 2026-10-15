//! Console amministrativa: i comandi che l'operatore scrive sullo standard input.
//!
//! La lettura di stdin non sta qui: il modulo si divide in [`analizza_comando`], una
//! funzione pura che traduce una riga in un [`Comando`], e [`esegui`], che quel comando
//! lo applica alle dipendenze del server. Le due cose le mette insieme il binario. La
//! separazione e' cio' che rende la console verificabile senza un terminale.
//!
//! # Come si scrivono i comandi
//!
//! ```text
//! /utenti                                  elenco dei veicoli e del loro stato
//! /broadcast <testo>                       avviso a tutta la flotta
//! /msg <username> <testo>                  messaggio diretto a un veicolo
//! /analisi <username> <today|week|month>   riepilogo dei movimenti
//! /aiuto                                   elenco dei comandi
//! /esci                                    spegnimento ordinato del server
//! ```
//!
//! Solo l'ultimo campo puo' contenere spazi, ed e' sempre un testo libero: il
//! destinatario e' percio' la prima parola dopo il nome del comando. Non e' una perdita,
//! perche' uno [`Username`](georuggine_common::types::Username) ammette solo
//! alfanumerici ASCII e underscore. La divisione passa da [`str::split_once`] e
//! [`str::split_whitespace`], mai indicizzando i byte: una riga multibyte non puo' far
//! cadere il server su un indice fuori da un confine di carattere.
//!
//! `/utenti` elenca tutti gli utenti registrati con lo stato (sconnesso, fermo o in
//! movimento): la connessione la dice il registro, il movimento lo
//! [`StateTracker`]. L'ultima posizione nota si ottiene leggendo tutto lo storico e
//! tenendone la coda, perche' il [`Repository`] non ha un metodo dedicato.

use std::sync::Arc;

use chrono::{DateTime, SecondsFormat, TimeDelta, Utc};
use georuggine_common::clock::Clock;
use georuggine_common::protocol::AnalysisRange;
use georuggine_common::types::{PositionRecord, UserId, UserState};
use thiserror::Error;
use tokio::sync::broadcast;
use tokio::task;

use crate::analysis::{self, AnalysisRangeExt as _};
use crate::messaging::{MessageHub, MessagingError};
use crate::net::ConnectionRegistry;
use crate::state::StateTracker;
use crate::storage::{Repository, StorageError};

/// Elenco dei comandi, stampato da `/aiuto`.
const AIUTO: &str = "\
Comandi disponibili:
  /utenti                                  elenco dei veicoli con stato e ultima posizione
  /broadcast <testo>                       manda un avviso a tutta la flotta
  /msg <username> <testo>                  manda un messaggio diretto a un veicolo
  /analisi <username> <today|week|month>   riepiloga i movimenti di un veicolo
  /aiuto                                   questo elenco
  /esci                                    avvia lo spegnimento ordinato del server";

/// Secondi di anticipo su [`Clock::now`] entro cui puo' trovarsi una posizione salvata.
///
/// L'istante di una rilevazione lo misura il client, e il livello di rete ne accetta fino
/// a cinque minuti di anticipo: cercare fino a `now` soltanto lascerebbe fuori proprio le
/// rilevazioni piu' recenti di un veicolo il cui orologio va avanti.
const ANTICIPO_TOLLERATO_SECS: i64 = 300;

/// Comando riconosciuto dalla console.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Comando {
    /// Elenca i veicoli connessi con la loro ultima posizione nota.
    Utenti,
    /// Manda un avviso a tutta la flotta.
    Broadcast(String),
    /// Manda un messaggio a un solo veicolo.
    Messaggio {
        /// Username del destinatario.
        destinatario: String,
        /// Corpo del messaggio.
        testo: String,
    },
    /// Riepiloga i movimenti di un veicolo su un intervallo.
    Analisi {
        /// Username dell'utente da analizzare.
        destinatario: String,
        /// Intervallo su cui calcolare il riepilogo.
        range: AnalysisRange,
    },
    /// Elenca i comandi disponibili.
    Aiuto,
    /// Avvia lo spegnimento ordinato del server.
    Esci,
}

/// Errore di [`analizza_comando`], cioe' una riga che non e' un comando eseguibile.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ErroreComando {
    /// La riga e' vuota o fatta di soli spazi.
    ///
    /// Non e' un errore dell'operatore: chi chiama la riconosce e passa alla riga
    /// successiva senza stampare. Sta fra gli errori perche' per una riga vuota non c'e'
    /// nessuna variante di [`Comando`] da restituire.
    #[error("riga vuota: nessun comando da eseguire")]
    RigaVuota,
    /// Il primo termine della riga non e' un comando conosciuto.
    #[error("comando sconosciuto {0:?}: /aiuto elenca quelli disponibili")]
    Sconosciuto(String),
    /// Al comando manca un argomento obbligatorio.
    #[error("{comando}: manca l'argomento <{argomento}>")]
    ArgomentoMancante {
        /// Nome del comando, come si scrive nella riga.
        comando: &'static str,
        /// Nome dell'argomento assente.
        argomento: &'static str,
    },
    /// Il comando ha ricevuto piu' argomenti di quanti ne accetti.
    #[error("{comando}: argomenti di troppo")]
    ArgomentiInEccesso {
        /// Nome del comando, come si scrive nella riga.
        comando: &'static str,
    },
    /// L'intervallo di `/analisi` non corrisponde a nessun alias conosciuto.
    #[error("intervallo {0:?} non riconosciuto: usa today/oggi, week/settimana o month/mese")]
    IntervalloSconosciuto(String),
}

/// Cosa la console deve stampare, e se il suo ciclo e' finito.
///
/// Un solo testo gia' pronto, riuscita o meno che sia l'operazione: il destinatario e'
/// una persona davanti a un terminale, non un chiamante che possa rimediare a un errore.
/// Per la stessa ragione [`esegui`] non restituisce un `Result`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Esito {
    /// Testo da mostrare all'operatore, senza l'a capo finale.
    pub testo: String,
    /// Vero se il ciclo della console deve terminare, cioe' solo dopo `/esci`.
    pub termina: bool,
}

impl Esito {
    /// Esito che stampa un testo e lascia la console in ascolto.
    fn parla(testo: impl Into<String>) -> Self {
        Self {
            testo: testo.into(),
            termina: false,
        }
    }

    /// Esito che stampa un testo e chiude il ciclo della console.
    fn congeda(testo: impl Into<String>) -> Self {
        Self {
            testo: testo.into(),
            termina: true,
        }
    }
}

/// Dipendenze su cui la console lavora.
///
/// Sono gli stessi esemplari condivisi che riceve il livello di rete: un registro o una
/// messaggistica diversi non vedrebbero nessuno dei veicoli collegati.
pub struct ConsoleContext {
    /// Persistenza di utenti, posizioni e messaggi.
    pub repo: Arc<dyn Repository>,
    /// Registro delle connessioni aperte, una per utente.
    pub registry: Arc<ConnectionRegistry>,
    /// Messaggistica, costruita sullo stesso [`registry`](Self::registry).
    pub messaging: Arc<MessageHub>,
    /// Sorgente dell'ora: la console non chiama mai `Utc::now()` da se'.
    pub clock: Arc<dyn Clock>,
    /// Macchina a stati fermo/movimento, la stessa che il livello di rete aggiorna.
    pub stati: Arc<StateTracker>,
    /// Canale su cui `/esci` annuncia lo spegnimento.
    pub shutdown: broadcast::Sender<()>,
}

/// Errore incontrato mentre si esegue un comando gia' riconosciuto.
///
/// Non esce dal modulo: [`esegui`] lo riduce al testo che l'operatore legge. E' un enum
/// perche' i casi hanno origini diverse (l'operatore, il database, il runtime) e il
/// messaggio lo deve dire.
#[derive(Debug, Error)]
enum ErroreEsecuzione {
    /// Nessun utente registrato ha lo username indicato.
    #[error("utente sconosciuto: {0:?}")]
    UtenteSconosciuto(String),
    /// Errore proveniente dalla persistenza.
    #[error("errore di persistenza: {0}")]
    Persistenza(#[from] StorageError),
    /// Errore proveniente dalla messaggistica.
    #[error("messaggio non inviato: {0}")]
    Messaggistica(#[from] MessagingError),
    /// L'attivita' di `spawn_blocking` che legge dal repository non e' arrivata in fondo.
    #[error("attivita' in background non completata: {0}")]
    Task(String),
}

/// Traduce una riga di testo nel comando che descrive.
///
/// La riga viene ripulita degli spazi ai due estremi; il primo termine e' il nome del
/// comando e tutto cio' che segue sono i suoi argomenti.
///
/// # Errori
///
/// - [`ErroreComando::RigaVuota`] se la riga e' vuota o fatta di soli spazi;
/// - [`ErroreComando::Sconosciuto`] se il primo termine non e' un comando;
/// - [`ErroreComando::ArgomentoMancante`] se manca un argomento obbligatorio;
/// - [`ErroreComando::ArgomentiInEccesso`] se il comando ne riceve piu' del previsto;
/// - [`ErroreComando::IntervalloSconosciuto`] se l'intervallo di `/analisi` non e' uno
///   degli alias ammessi.
pub fn analizza_comando(riga: &str) -> Result<Comando, ErroreComando> {
    let riga = riga.trim();
    if riga.is_empty() {
        return Err(ErroreComando::RigaVuota);
    }

    let (nome, resto) = spezza(riga);

    match nome {
        "/utenti" => senza_argomenti("/utenti", resto).map(|()| Comando::Utenti),
        "/aiuto" => senza_argomenti("/aiuto", resto).map(|()| Comando::Aiuto),
        "/esci" => senza_argomenti("/esci", resto).map(|()| Comando::Esci),
        "/broadcast" => {
            if resto.is_empty() {
                return Err(ErroreComando::ArgomentoMancante {
                    comando: "/broadcast",
                    argomento: "testo",
                });
            }
            Ok(Comando::Broadcast(resto.trim_end().to_owned()))
        }
        "/msg" => {
            let (destinatario, testo) = spezza(resto);
            if destinatario.is_empty() {
                return Err(ErroreComando::ArgomentoMancante {
                    comando: "/msg",
                    argomento: "destinatario",
                });
            }
            if testo.is_empty() {
                return Err(ErroreComando::ArgomentoMancante {
                    comando: "/msg",
                    argomento: "testo",
                });
            }
            Ok(Comando::Messaggio {
                destinatario: destinatario.to_owned(),
                testo: testo.trim_end().to_owned(),
            })
        }
        // `/analisi` non ha campi di testo libero, quindi si divide per parole: cosi' un
        // argomento in piu' e' distinguibile da uno username con uno spazio dentro, che
        // non esiste.
        "/analisi" => match resto.split_whitespace().collect::<Vec<&str>>().as_slice() {
            [] => Err(ErroreComando::ArgomentoMancante {
                comando: "/analisi",
                argomento: "destinatario",
            }),
            [_] => Err(ErroreComando::ArgomentoMancante {
                comando: "/analisi",
                argomento: "intervallo",
            }),
            [destinatario, intervallo] => Ok(Comando::Analisi {
                destinatario: (*destinatario).to_owned(),
                range: analizza_intervallo(intervallo)?,
            }),
            _ => Err(ErroreComando::ArgomentiInEccesso {
                comando: "/analisi",
            }),
        },
        altro => Err(ErroreComando::Sconosciuto(altro.to_owned())),
    }
}

/// Divide una riga gia' ripulita nel primo termine e in cio' che lo segue, senza gli
/// spazi iniziali. Se non ci sono spazi, il resto e' vuoto.
fn spezza(riga: &str) -> (&str, &str) {
    match riga.split_once(char::is_whitespace) {
        Some((primo, resto)) => (primo, resto.trim_start()),
        None => (riga, ""),
    }
}

/// Verifica che a un comando senza argomenti non ne sia stato passato nessuno.
///
/// # Errori
///
/// [`ErroreComando::ArgomentiInEccesso`] se dopo il nome del comando c'e' altro.
fn senza_argomenti(comando: &'static str, resto: &str) -> Result<(), ErroreComando> {
    if resto.is_empty() {
        Ok(())
    } else {
        Err(ErroreComando::ArgomentiInEccesso { comando })
    }
}

/// Traduce l'alias di un intervallo nella variante di [`AnalysisRange`] che indica.
///
/// [`AnalysisRange::Custom`] non ha un alias perche' la console non ha una sintassi per
/// scriverne gli estremi, ma [`esegui`] accetta comunque qualunque intervallo.
///
/// # Errori
///
/// [`ErroreComando::IntervalloSconosciuto`] su qualunque altra parola.
fn analizza_intervallo(parola: &str) -> Result<AnalysisRange, ErroreComando> {
    match parola {
        "today" | "oggi" => Ok(AnalysisRange::Today),
        "week" | "settimana" => Ok(AnalysisRange::CurrentWeek),
        "month" | "mese" => Ok(AnalysisRange::CurrentMonth),
        altro => Err(ErroreComando::IntervalloSconosciuto(altro.to_owned())),
    }
}

/// Esegue un comando sulle dipendenze del server.
///
/// Ogni esito, riuscito o fallito, diventa il testo di un [`Esito`]. L'unico comando che
/// chiude il ciclo e' `/esci`, che manda anche il segnale di spegnimento al resto del
/// server.
pub async fn esegui(comando: Comando, contesto: &ConsoleContext) -> Esito {
    match comando {
        Comando::Utenti => riferisci(elenca_utenti(contesto).await),
        Comando::Broadcast(testo) => riferisci(manda_avviso(&testo, contesto).await),
        Comando::Messaggio {
            destinatario,
            testo,
        } => riferisci(manda_messaggio(&destinatario, &testo, contesto).await),
        Comando::Analisi {
            destinatario,
            range,
        } => riferisci(riepiloga(&destinatario, range, contesto).await),
        Comando::Aiuto => Esito::parla(AIUTO),
        Comando::Esci => {
            // L'errore significa solo che nessuno e' in ascolto, cioe' che il server sta
            // gia' terminando per un'altra via.
            let _ = contesto.shutdown.send(());
            Esito::congeda("Spegnimento richiesto: il server chiude le connessioni aperte.")
        }
    }
}

/// Riduce l'esito di un comando al testo che la console stampa.
fn riferisci(esito: Result<String, ErroreEsecuzione>) -> Esito {
    match esito {
        Ok(testo) => Esito::parla(testo),
        Err(errore) => Esito::parla(errore.to_string()),
    }
}

/// Elenca tutti i veicoli registrati con il loro stato e l'ultima posizione nota.
///
/// Lo stato combina due fonti: chi non ha una voce nel registro delle connessioni e'
/// sconnesso; per chi ce l'ha, fermo o in movimento lo dice lo [`StateTracker`].
///
/// # Errori
///
/// [`ErroreEsecuzione::Persistenza`] o [`ErroreEsecuzione::Task`] se lo storico non e'
/// leggibile.
async fn elenca_utenti(contesto: &ConsoleContext) -> Result<String, ErroreEsecuzione> {
    let connessi = contesto.registry.connected_users().await;
    let repo = Arc::clone(&contesto.repo);
    let adesso = contesto.clock.now();
    let voci = in_background(move || raccogli_voci(repo.as_ref(), adesso)).await?;

    if voci.is_empty() {
        return Ok("Nessun veicolo registrato.".to_owned());
    }

    let quanti_connessi = voci
        .iter()
        .filter(|voce| connessi.contains(&voce.id))
        .count();
    let mut testo = format!(
        "Veicoli registrati ({}), di cui {}:",
        voci.len(),
        concorda(quanti_connessi, "connesso", "connessi")
    );
    for voce in &voci {
        let stato = stato_di(voce.id, &connessi, &contesto.stati);
        testo.push_str("\n  ");
        testo.push_str(&voce.riga(stato));
    }

    Ok(testo)
}

/// Stato da mostrare per un utente.
///
/// Un utente connesso di cui la macchina a stati non sa ancora nulla e' fermo: e' lo stato
/// con cui ogni sessione si apre.
fn stato_di(id: UserId, connessi: &[UserId], stati: &StateTracker) -> UserState {
    if !connessi.contains(&id) {
        return UserState::Disconnected;
    }
    match stati.current_state(id) {
        Some(UserState::Moving) => UserState::Moving,
        _ => UserState::Still,
    }
}

/// Riga dell'elenco di `/utenti`: chi e' il veicolo e dove si trovava l'ultima volta.
struct Voce {
    /// Identificatore dell'utente.
    id: UserId,
    /// Username dell'utente.
    nome: String,
    /// Ultima posizione nota, assente se l'utente non ne ha mai inviata una.
    ultima: Option<PositionRecord>,
}

impl Voce {
    /// Riga da mostrare all'operatore, con lo stato dell'utente.
    fn riga(&self, stato: UserState) -> String {
        match &self.ultima {
            Some(posizione) => format!(
                "{}  {stato}  ultimo contatto {} a {:.6}, {:.6}",
                self.nome,
                istante(posizione.recorded_at),
                posizione.coordinate.lat(),
                posizione.coordinate.lon(),
            ),
            None => format!("{}  {stato}  nessuna posizione registrata", self.nome),
        }
    }
}

/// Raccoglie nome e ultima posizione di ogni utente registrato, in ordine di id.
///
/// E' sincrona perche' il [`Repository`] lo e': il chiamante la porta su un thread da
/// lavoro. Le letture stanno in una chiamata sola invece di una per utente, cosi' il
/// passaggio all'esecutore si paga una volta.
///
/// # Errori
///
/// [`StorageError`] se l'elenco degli utenti o lo storico non sono leggibili.
fn raccogli_voci(repo: &dyn Repository, adesso: DateTime<Utc>) -> Result<Vec<Voce>, StorageError> {
    let fine = fine_dello_storico(adesso);

    repo.list_users()?
        .into_iter()
        .map(|utente| {
            // Manca un modo diretto di chiedere l'ultima posizione: si legge lo storico e
            // se ne tiene la coda, che e' la piu' recente perche' `positions_in_range`
            // ordina per `recorded_at` crescente.
            let mut posizioni =
                repo.positions_in_range(utente.id, DateTime::<Utc>::MIN_UTC, fine)?;

            Ok(Voce {
                id: utente.id,
                nome: utente.username.as_str().to_owned(),
                ultima: posizioni.pop(),
            })
        })
        .collect()
}

/// Estremo superiore dell'intervallo che contiene tutto lo storico.
///
/// Non e' `MAX_UTC` perche' il repository confronta gli istanti come testo: `MAX_UTC` si
/// scrive con l'anno preceduto da `+`, che in ASCII viene prima delle cifre, quindi
/// risulterebbe minore di qualunque data reale e l'intervallo sarebbe sempre vuoto.
/// `MIN_UTC` come estremo inferiore non ha il problema: il suo `-` viene prima delle
/// cifre, che e' proprio l'ordine voluto.
fn fine_dello_storico(adesso: DateTime<Utc>) -> DateTime<Utc> {
    adesso
        .checked_add_signed(TimeDelta::seconds(ANTICIPO_TOLLERATO_SECS))
        .unwrap_or(adesso)
}

/// Manda un avviso a tutta la flotta e riferisce quanti veicoli ha raggiunto.
///
/// # Errori
///
/// [`ErroreEsecuzione::Messaggistica`] se il testo supera il limite di lunghezza o se la
/// scrittura a storico fallisce.
async fn manda_avviso(testo: &str, contesto: &ConsoleContext) -> Result<String, ErroreEsecuzione> {
    let raggiunti = contesto.messaging.broadcast(testo).await?;

    Ok(format!(
        "Avviso inviato a {}.",
        concorda(raggiunti, "veicolo connesso", "veicoli connessi")
    ))
}

/// Manda un messaggio a un solo veicolo e riferisce se lo ha raggiunto subito.
///
/// # Errori
///
/// [`ErroreEsecuzione::UtenteSconosciuto`] se nessun utente registrato ha quel nome, e in
/// quel caso non viene scritto nulla; [`ErroreEsecuzione::Messaggistica`] se l'invio non
/// riesce.
async fn manda_messaggio(
    destinatario: &str,
    testo: &str,
    contesto: &ConsoleContext,
) -> Result<String, ErroreEsecuzione> {
    let user_id = risolvi_username(destinatario, contesto).await?;
    let consegnato = contesto.messaging.direct(user_id, testo).await?;

    Ok(if consegnato {
        format!("Messaggio consegnato a {destinatario}.")
    } else {
        format!(
            "{destinatario} non e' connesso: il messaggio resta in coda e gli sara' recapitato al prossimo accesso."
        )
    })
}

/// Riepiloga i movimenti di un veicolo sull'intervallo richiesto.
///
/// # Errori
///
/// [`ErroreEsecuzione::UtenteSconosciuto`] se nessun utente registrato ha quel nome;
/// [`ErroreEsecuzione::Persistenza`] o [`ErroreEsecuzione::Task`] se lo storico non e'
/// leggibile.
async fn riepiloga(
    destinatario: &str,
    range: AnalysisRange,
    contesto: &ConsoleContext,
) -> Result<String, ErroreEsecuzione> {
    let user_id = risolvi_username(destinatario, contesto).await?;
    let (from, to) = range.to_bounds(contesto.clock.now());

    let repo = Arc::clone(&contesto.repo);
    let posizioni = in_background(move || repo.positions_in_range(user_id, from, to)).await?;

    let riepilogo = analysis::analyze(&posizioni);

    Ok(format!(
        "Analisi di {destinatario} su {}, da {} a {} ({}):\n  \
         distanza percorsa:  {:.1} m\n  \
         velocita' media:    {:.1} km/h\n  \
         tempo in movimento: {}\n  \
         tempo in pausa:     {}\n  \
         soste:              {}\n  \
         tragitto:           {}",
        nome_intervallo(range),
        istante(from),
        istante(to),
        concorda(posizioni.len(), "posizione", "posizioni"),
        riepilogo.total_distance_meters,
        riepilogo.average_speed_kmh,
        formatta_durata(riepilogo.moving_duration_secs),
        formatta_durata(riepilogo.pause_duration_secs),
        riepilogo.stop_count,
        descrivi_tragitto(&riepilogo.route, riepilogo.route_points),
    ))
}

/// Il tragitto in una riga: quanti punti, da dove e fino a dove.
///
/// Elencare tutti i punti non servirebbe all'operatore, perche' un giorno di invii ogni 30
/// secondi ne conta migliaia: partenza e arrivo con i loro istanti dicono dove il veicolo
/// e' andato, e il numero di punti quanto e' fitto il percorso. Il numero e' `totale`,
/// quello del tragitto completo, non la lunghezza di `route`, che puo' essere un campione.
fn descrivi_tragitto(route: &[PositionRecord], totale: u64) -> String {
    let punto = |posizione: &PositionRecord| {
        format!(
            "{:.6}, {:.6} ({})",
            posizione.coordinate.lat(),
            posizione.coordinate.lon(),
            istante(posizione.recorded_at),
        )
    };

    match route {
        [] => "nessun punto".to_owned(),
        [solo] => format!("1 punto, a {}", punto(solo)),
        [primo, .., ultimo] => format!("{totale} punti, da {} a {}", punto(primo), punto(ultimo)),
    }
}

/// Cerca l'id dell'utente che ha lo username indicato.
///
/// Il confronto avviene sul testo e non su uno `Username` costruito dall'argomento: cosi'
/// un nome che non supererebbe nemmeno la validazione da' lo stesso "utente sconosciuto"
/// di un nome ben formato ma inesistente, che e' cio' che all'operatore interessa.
///
/// # Errori
///
/// [`ErroreEsecuzione::UtenteSconosciuto`] se nessuno ha quel nome;
/// [`ErroreEsecuzione::Persistenza`] o [`ErroreEsecuzione::Task`] se l'elenco non e'
/// leggibile.
async fn risolvi_username(
    destinatario: &str,
    contesto: &ConsoleContext,
) -> Result<UserId, ErroreEsecuzione> {
    let repo = Arc::clone(&contesto.repo);
    let utenti = in_background(move || repo.list_users()).await?;

    utenti
        .into_iter()
        .find(|utente| utente.username.as_str() == destinatario)
        .map(|utente| utente.id)
        .ok_or_else(|| ErroreEsecuzione::UtenteSconosciuto(destinatario.to_owned()))
}

/// Esegue una lettura del repository su un thread da lavoro: i suoi metodi sono
/// sincroni, e chiamarli qui fermerebbe un thread dell'esecutore.
///
/// # Errori
///
/// [`ErroreEsecuzione::Persistenza`] se la lettura fallisce, [`ErroreEsecuzione::Task`]
/// se l'attivita' non arriva in fondo.
async fn in_background<T, F>(lettura: F) -> Result<T, ErroreEsecuzione>
where
    F: FnOnce() -> Result<T, StorageError> + Send + 'static,
    T: Send + 'static,
{
    task::spawn_blocking(lettura)
        .await
        .map_err(|errore| {
            ErroreEsecuzione::Task(format!("attivita' di lettura interrotta: {errore}"))
        })?
        .map_err(ErroreEsecuzione::from)
}

/// Nome con cui l'intervallo compare nel riepilogo.
fn nome_intervallo(range: AnalysisRange) -> &'static str {
    match range {
        AnalysisRange::Today => "oggi",
        AnalysisRange::CurrentWeek => "la settimana corrente",
        AnalysisRange::CurrentMonth => "il mese corrente",
        AnalysisRange::Custom { .. } => "l'intervallo indicato",
    }
}

/// Istante come lo mostra la console: RFC3339 in UTC, al secondo.
fn istante(quando: DateTime<Utc>) -> String {
    quando.to_rfc3339_opts(SecondsFormat::Secs, true)
}

/// Accorda un sostantivo con il numero che lo precede.
fn concorda(quanti: usize, singolare: &str, plurale: &str) -> String {
    if quanti == 1 {
        format!("{quanti} {singolare}")
    } else {
        format!("{quanti} {plurale}")
    }
}

/// Durata in secondi come la legge una persona: `2h 05m 30s`, `5m 30s`, `30s`.
///
/// Le unita' vuote in testa non compaiono, quelle in mezzo si': `1h 00m 05s` dice piu' di
/// `1h 5s`, dove i cinque secondi si potrebbero scambiare per cinque minuti.
fn formatta_durata(secondi: u64) -> String {
    let ore = secondi / 3_600;
    let minuti = (secondi % 3_600) / 60;
    let resto = secondi % 60;

    if ore > 0 {
        format!("{ore}h {minuti:02}m {resto:02}s")
    } else if minuti > 0 {
        format!("{minuti}m {resto:02}s")
    } else {
        format!("{resto}s")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use chrono::{Local, MappedLocalTime, NaiveDate, NaiveTime, TimeZone};
    use georuggine_common::clock::FakeClock;
    use georuggine_common::protocol::ServerMessage;
    use georuggine_common::types::{Coordinate, Username};
    use tokio::sync::mpsc;

    use crate::storage::{PendingMessage, SqliteRepository};

    /// Messaggi che una coda di prova puo' accodare prima di riempirsi.
    const CAPACITA: usize = 4;

    /// Gradi di latitudine che valgono circa 100 m.
    const GRADI_PER_CENTO_METRI: f64 = 0.000_9;

    /// Coordinate di partenza dei test: piazza Castello, Torino.
    const LAT_PARTENZA: f64 = 45.0703;
    const LON_PARTENZA: f64 = 7.6869;

    #[test]
    fn ogni_comando_valido_nella_forma_minima_e_riconosciuto() {
        for (riga, atteso) in [
            ("/utenti", Comando::Utenti),
            ("/aiuto", Comando::Aiuto),
            ("/esci", Comando::Esci),
            (
                "/broadcast fermi tutti",
                Comando::Broadcast("fermi tutti".to_owned()),
            ),
            (
                "/msg veicolo_01 rientra",
                Comando::Messaggio {
                    destinatario: "veicolo_01".to_owned(),
                    testo: "rientra".to_owned(),
                },
            ),
            (
                "/analisi veicolo_01 today",
                Comando::Analisi {
                    destinatario: "veicolo_01".to_owned(),
                    range: AnalysisRange::Today,
                },
            ),
        ] {
            assert_eq!(analizza_comando(riga), Ok(atteso), "riga {riga:?}");
        }
    }

    #[test]
    fn solo_l_ultimo_campo_puo_contenere_spazi() {
        assert_eq!(analizza_comando("   /utenti  \n"), Ok(Comando::Utenti));
        assert_eq!(
            analizza_comando("  /msg  veicolo_01   rientra in sede  \n"),
            Ok(Comando::Messaggio {
                destinatario: "veicolo_01".to_owned(),
                testo: "rientra in sede".to_owned(),
            })
        );
        assert_eq!(
            analizza_comando("/broadcast manutenzione alle 18 in sede"),
            Ok(Comando::Broadcast(
                "manutenzione alle 18 in sede".to_owned()
            ))
        );
    }

    #[test]
    fn un_argomento_obbligatorio_mancante_e_un_errore_leggibile() {
        for (riga, comando, argomento) in [
            ("/broadcast   ", "/broadcast", "testo"),
            ("/msg", "/msg", "destinatario"),
            ("/msg   ", "/msg", "destinatario"),
            ("/msg veicolo_01", "/msg", "testo"),
            ("/analisi", "/analisi", "destinatario"),
            ("/analisi veicolo_01   ", "/analisi", "intervallo"),
        ] {
            assert_eq!(
                analizza_comando(riga),
                Err(ErroreComando::ArgomentoMancante { comando, argomento }),
                "riga {riga:?}"
            );
        }

        assert_eq!(
            analizza_comando("/msg veicolo_01")
                .expect_err("manca il testo")
                .to_string(),
            "/msg: manca l'argomento <testo>"
        );
    }

    #[test]
    fn gli_argomenti_di_troppo_sono_un_errore() {
        for (riga, comando) in [
            ("/utenti tutti", "/utenti"),
            ("/aiuto tutto", "/aiuto"),
            ("/esci subito", "/esci"),
            ("/analisi veicolo_01 today e anche domani", "/analisi"),
        ] {
            assert_eq!(
                analizza_comando(riga),
                Err(ErroreComando::ArgomentiInEccesso { comando }),
                "riga {riga:?}"
            );
        }
    }

    #[test]
    fn gli_alias_dello_stesso_intervallo_danno_la_stessa_variante() {
        for (primo, secondo, atteso) in [
            ("today", "oggi", AnalysisRange::Today),
            ("week", "settimana", AnalysisRange::CurrentWeek),
            ("month", "mese", AnalysisRange::CurrentMonth),
        ] {
            let comando = |alias: &str| {
                analizza_comando(&format!("/analisi veicolo_01 {alias}"))
                    .expect("l'alias e' riconosciuto")
            };
            let atteso = Comando::Analisi {
                destinatario: "veicolo_01".to_owned(),
                range: atteso,
            };

            assert_eq!(comando(primo), atteso, "alias {primo:?}");
            assert_eq!(comando(secondo), atteso, "alias {secondo:?}");
        }
        assert_eq!(
            analizza_comando("/analisi veicolo_01 ieri"),
            Err(ErroreComando::IntervalloSconosciuto("ieri".to_owned()))
        );
    }

    #[test]
    fn una_riga_vuota_o_sconosciuta_non_e_un_comando() {
        for riga in ["", "   ", "\t", "  \r\n"] {
            assert_eq!(
                analizza_comando(riga),
                Err(ErroreComando::RigaVuota),
                "riga {riga:?}"
            );
        }
        for riga in ["/pippo", "/pippo con argomenti", "utenti", "ciao"] {
            let errore = analizza_comando(riga).expect_err("non e' un comando");
            assert!(
                matches!(errore, ErroreComando::Sconosciuto(_)),
                "riga {riga:?}: atteso Sconosciuto, ottenuto {errore:?}"
            );
        }
    }

    #[test]
    fn un_testo_multibyte_non_manda_in_panico_il_parsing() {
        // Se la divisione indicizzasse i byte invece di tagliare sui confini di
        // carattere, queste righe farebbero cadere il server. Il destinatario non e' uno
        // username valido, ma il parsing non lo sa e non deve nemmeno provarci.
        assert_eq!(
            analizza_comando("/broadcast attenzione: neve 🚚 sull'A4 città"),
            Ok(Comando::Broadcast(
                "attenzione: neve 🚚 sull'A4 città".to_owned()
            ))
        );
        assert_eq!(
            analizza_comando("/msg città 🚚 rientra"),
            Ok(Comando::Messaggio {
                destinatario: "città".to_owned(),
                testo: "🚚 rientra".to_owned(),
            })
        );
        assert_eq!(
            analizza_comando("/analisi città oggi"),
            Ok(Comando::Analisi {
                destinatario: "città".to_owned(),
                range: AnalysisRange::Today,
            })
        );
    }

    #[test]
    fn errore_comando_si_mostra_in_italiano() {
        assert_eq!(
            ErroreComando::Sconosciuto("/pippo".to_owned()).to_string(),
            "comando sconosciuto \"/pippo\": /aiuto elenca quelli disponibili"
        );
        assert_eq!(
            ErroreComando::ArgomentiInEccesso { comando: "/utenti" }.to_string(),
            "/utenti: argomenti di troppo"
        );
        assert_eq!(
            ErroreComando::IntervalloSconosciuto("ieri".to_owned()).to_string(),
            "intervallo \"ieri\" non riconosciuto: usa today/oggi, week/settimana o month/mese"
        );
    }

    /// Console sotto esame e le dipendenze che il test ispeziona.
    struct Banco {
        contesto: ConsoleContext,
        repo: Arc<dyn Repository>,
        orologio: Arc<FakeClock>,
    }

    /// Istante UTC del mezzogiorno locale del 2026-08-13.
    ///
    /// Le varianti di calendario di [`AnalysisRange`] partono dalla mezzanotte
    /// **locale**: partire da mezzogiorno tiene le posizioni dentro la giornata locale
    /// qualunque sia il fuso della macchina che esegue i test.
    fn mezzogiorno_locale() -> DateTime<Utc> {
        let data = NaiveDate::from_ymd_opt(2026, 8, 13).expect("data di test valida");
        let orario = NaiveTime::from_hms_opt(12, 0, 0).expect("orario di test valido");
        match Local.from_local_datetime(&data.and_time(orario)) {
            MappedLocalTime::Single(istante) => istante.to_utc(),
            MappedLocalTime::Ambiguous(prima, _) => prima.to_utc(),
            MappedLocalTime::None => panic!("il mezzogiorno locale deve esistere"),
        }
    }

    /// Monta una console su un database in RAM e un orologio fermo.
    fn banco() -> Banco {
        let orologio = Arc::new(FakeClock::new(mezzogiorno_locale()));
        let repo: Arc<dyn Repository> = Arc::new(
            SqliteRepository::new(":memory:", Arc::clone(&orologio) as Arc<dyn Clock>)
                .expect("apertura in memoria riuscita"),
        );
        let registry = Arc::new(ConnectionRegistry::new());
        let messaging = Arc::new(MessageHub::new(
            Arc::clone(&repo),
            Arc::clone(&registry),
            Arc::clone(&orologio) as Arc<dyn Clock>,
        ));

        Banco {
            contesto: ConsoleContext {
                repo: Arc::clone(&repo),
                registry,
                messaging,
                clock: Arc::clone(&orologio) as Arc<dyn Clock>,
                stati: Arc::new(StateTracker::new(Arc::clone(&orologio) as Arc<dyn Clock>)),
                shutdown: broadcast::channel(1).0,
            },
            repo,
            orologio,
        }
    }

    fn registra(banco: &Banco, nome: &str) -> UserId {
        let username = Username::new(nome).expect("username di test valido");
        banco
            .repo
            .create_user(&username, "hash")
            .expect("la registrazione riesce")
    }

    /// Iscrive una connessione finta e restituisce la coda su cui il test la legge.
    async fn connetti(banco: &Banco, user_id: UserId) -> mpsc::Receiver<ServerMessage> {
        let (mittente, coda) = mpsc::channel(CAPACITA);
        banco.contesto.registry.register(user_id, mittente).await;
        coda
    }

    /// Scrive una posizione a `secondi` dall'istante dell'orologio, `passi` scatti a nord.
    fn registra_posizione(banco: &Banco, user_id: UserId, passi: f64, secondi: i64) {
        let coordinate =
            Coordinate::new(LAT_PARTENZA + passi * GRADI_PER_CENTO_METRI, LON_PARTENZA)
                .expect("coordinata di test valida");

        banco
            .repo
            .insert_position(&PositionRecord {
                user_id,
                coordinate,
                recorded_at: banco.orologio.now() + TimeDelta::seconds(secondi),
            })
            .expect("la scrittura riesce");
    }

    #[tokio::test]
    async fn utenti_elenca_tutti_i_registrati_con_stato_e_ultima_posizione() {
        let banco = banco();
        assert_eq!(
            esegui(Comando::Utenti, &banco.contesto).await.testo,
            "Nessun veicolo registrato."
        );

        let primo = registra(&banco, "veicolo_01");
        let secondo = registra(&banco, "veicolo_02");
        // Il terzo utente e' registrato ma non collegato: compare come sconnesso.
        registra(&banco, "veicolo_03");
        let _code = (
            connetti(&banco, primo).await,
            connetti(&banco, secondo).await,
        );
        // Il primo si sposta di circa 100 m fra due posizioni: e' in movimento. Il secondo
        // si e' appena collegato e non ha ancora inviato nulla: e' fermo.
        banco.contesto.stati.on_connect(primo);
        banco.contesto.stati.on_connect(secondo);
        let partenza = Coordinate::new(LAT_PARTENZA, LON_PARTENZA).expect("coordinata valida");
        let arrivo = Coordinate::new(LAT_PARTENZA + GRADI_PER_CENTO_METRI, LON_PARTENZA)
            .expect("coordinata valida");
        banco.contesto.stati.on_position(primo, partenza);
        banco.contesto.stati.on_position(primo, arrivo);
        registra_posizione(&banco, primo, 0.0, -120);
        registra_posizione(&banco, primo, 1.0, -60);

        let esito = esegui(Comando::Utenti, &banco.contesto).await;

        assert!(
            esito
                .testo
                .starts_with("Veicoli registrati (3), di cui 2 connessi:"),
            "intestazione inattesa: {}",
            esito.testo
        );
        assert!(
            esito
                .testo
                .contains("veicolo_01  In movimento  ultimo contatto"),
            "manca il primo veicolo: {}",
            esito.testo
        );
        assert!(
            esito
                .testo
                .contains(&istante(banco.orologio.now() - TimeDelta::seconds(60))),
            "l'ultimo contatto deve essere la posizione piu' recente: {}",
            esito.testo
        );
        assert!(
            esito
                .testo
                .contains("veicolo_02  Fermo  nessuna posizione registrata"),
            "manca il secondo veicolo: {}",
            esito.testo
        );
        assert!(
            esito
                .testo
                .contains("veicolo_03  Disconnesso  nessuna posizione registrata"),
            "un utente non connesso va elencato come sconnesso: {}",
            esito.testo
        );
    }

    #[tokio::test]
    async fn broadcast_raggiunge_le_connessioni_registrate() {
        let banco = banco();
        let mut prima = connetti(&banco, UserId(1)).await;
        let mut seconda = connetti(&banco, UserId(2)).await;

        let esito = esegui(
            Comando::Broadcast("manutenzione alle 18".to_owned()),
            &banco.contesto,
        )
        .await;

        assert_eq!(esito.testo, "Avviso inviato a 2 veicoli connessi.");
        let atteso = ServerMessage::Broadcast {
            body: "manutenzione alle 18".to_owned(),
            sent_at: banco.orologio.now(),
        };
        assert_eq!(prima.recv().await, Some(atteso.clone()));
        assert_eq!(seconda.recv().await, Some(atteso));
    }

    #[tokio::test]
    async fn messaggio_a_un_connesso_e_immediato_a_uno_scollegato_resta_in_coda() {
        let banco = banco();
        let collegato = registra(&banco, "veicolo_01");
        let scollegato = registra(&banco, "veicolo_02");
        let mut coda = connetti(&banco, collegato).await;

        let esito = esegui(
            Comando::Messaggio {
                destinatario: "veicolo_01".to_owned(),
                testo: "torna in sede".to_owned(),
            },
            &banco.contesto,
        )
        .await;
        assert_eq!(esito.testo, "Messaggio consegnato a veicolo_01.");
        assert_eq!(
            coda.recv().await,
            Some(ServerMessage::Direct {
                body: "torna in sede".to_owned(),
                sent_at: banco.orologio.now(),
            })
        );

        let esito = esegui(
            Comando::Messaggio {
                destinatario: "veicolo_02".to_owned(),
                testo: "rientra in sede".to_owned(),
            },
            &banco.contesto,
        )
        .await;
        assert!(
            esito.testo.contains("in coda"),
            "l'esito deve dire che il messaggio attende: {}",
            esito.testo
        );
        assert_eq!(
            banco
                .repo
                .pending_messages(scollegato)
                .expect("lettura riuscita"),
            vec![PendingMessage {
                body: "rientra in sede".to_owned(),
                sent_at: banco.orologio.now(),
            }]
        );
    }

    #[tokio::test]
    async fn uno_username_inesistente_non_scrive_nulla() {
        let banco = banco();
        let esistente = registra(&banco, "veicolo_01");

        for comando in [
            Comando::Messaggio {
                destinatario: "pippo".to_owned(),
                testo: "rientra".to_owned(),
            },
            Comando::Analisi {
                destinatario: "pippo".to_owned(),
                range: AnalysisRange::Today,
            },
        ] {
            let esito = esegui(comando, &banco.contesto).await;
            assert_eq!(esito.testo, "utente sconosciuto: \"pippo\"");
            assert!(!esito.termina);
        }

        assert!(
            banco
                .repo
                .pending_messages(esistente)
                .expect("lettura riuscita")
                .is_empty(),
            "il messaggio non deve finire a un altro utente"
        );
        assert_eq!(
            banco.repo.list_users().expect("lettura riuscita").len(),
            1,
            "un destinatario inesistente non va creato"
        );
    }

    #[tokio::test]
    async fn analisi_riporta_i_numeri_del_riepilogo() {
        let banco = banco();
        let user_id = registra(&banco, "veicolo_01");
        // Dieci punti in linea retta, uno scatto di circa 100 m ogni 10 s: gli stessi
        // numeri verificati in `analysis.rs`, cioe' 900 m in 90 s a 36 km/h.
        for i in 0..10 {
            registra_posizione(&banco, user_id, f64::from(i), i64::from(i) * 10 - 90);
        }

        let esito = esegui(
            Comando::Analisi {
                destinatario: "veicolo_01".to_owned(),
                range: AnalysisRange::Today,
            },
            &banco.contesto,
        )
        .await;

        assert!(
            esito
                .testo
                .starts_with("Analisi di veicolo_01 su oggi, da "),
            "intestazione inattesa: {}",
            esito.testo
        );
        for atteso in [
            "(10 posizioni)",
            "velocita' media:    36.0 km/h",
            "tempo in movimento: 1m 30s",
            "tempo in pausa:     0s",
            "soste:              0",
            "tragitto:           10 punti, da 45.070300, 7.686900",
            "a 45.078400, 7.686900",
        ] {
            assert!(
                esito.testo.contains(atteso),
                "manca {atteso:?}: {}",
                esito.testo
            );
        }
    }

    #[tokio::test]
    async fn analisi_di_un_utente_senza_posizioni_e_a_zero() {
        let banco = banco();
        registra(&banco, "veicolo_01");

        let esito = esegui(
            Comando::Analisi {
                destinatario: "veicolo_01".to_owned(),
                range: AnalysisRange::CurrentWeek,
            },
            &banco.contesto,
        )
        .await;

        assert!(
            esito.testo.contains("(0 posizioni)"),
            "nessuna posizione da analizzare: {}",
            esito.testo
        );
        assert!(esito.testo.contains("distanza percorsa:  0.0 m"));
        assert!(esito.testo.contains("tragitto:           nessun punto"));
    }

    #[tokio::test]
    async fn aiuto_elenca_tutti_i_comandi() {
        let banco = banco();

        let esito = esegui(Comando::Aiuto, &banco.contesto).await;

        for comando in [
            "/utenti",
            "/broadcast",
            "/msg",
            "/analisi",
            "/aiuto",
            "/esci",
        ] {
            assert!(
                esito.testo.contains(comando),
                "l'aiuto non nomina {comando}: {}",
                esito.testo
            );
        }
        assert!(!esito.termina);
    }

    #[tokio::test]
    async fn esci_manda_il_segnale_di_spegnimento_e_chiude_il_ciclo() {
        let banco = banco();
        let mut spegnimento = banco.contesto.shutdown.subscribe();

        let esito = esegui(Comando::Esci, &banco.contesto).await;

        assert!(esito.termina, "/esci deve chiudere il ciclo della console");
        assert_eq!(spegnimento.recv().await, Ok(()));

        // Senza nessuno in ascolto `send` fallisce, ma la console deve congedarsi
        // comunque.
        drop(spegnimento);
        assert!(esegui(Comando::Esci, &banco.contesto).await.termina);
    }

    #[test]
    fn le_durate_e_i_plurali_si_leggono() {
        assert_eq!(formatta_durata(0), "0s");
        assert_eq!(formatta_durata(90), "1m 30s");
        assert_eq!(formatta_durata(3_600), "1h 00m 00s");
        assert_eq!(formatta_durata(7_505), "2h 05m 05s");

        assert_eq!(concorda(0, "veicolo", "veicoli"), "0 veicoli");
        assert_eq!(concorda(1, "veicolo", "veicoli"), "1 veicolo");
        assert_eq!(concorda(2, "veicolo", "veicoli"), "2 veicoli");
    }
}
