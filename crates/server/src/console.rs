//! Console amministrativa: i comandi che l'operatore scrive sullo standard input.
//!
//! Il modulo copre il requisito R14. La lettura di stdin **non** sta qui: la console si
//! divide in una funzione pura, [`analizza_comando`], che traduce una riga di testo in un
//! [`Comando`], e in [`esegui`], che quel comando lo applica alle dipendenze del server.
//! Chi legge le righe (il binario in `src/main.rs`) mette insieme le due cose. La
//! separazione e' cio' che rende la console verificabile senza un terminale: i test di
//! questo modulo non aprono ne' stdin ne' un socket.
//!
//! # Come si scrivono i comandi
//!
//! Un comando per riga, con il nome che comincia per `/`:
//!
//! ```text
//! /utenti                                  elenco dei veicoli connessi
//! /broadcast <testo>                       avviso a tutta la flotta
//! /msg <username> <testo>                  messaggio diretto a un veicolo
//! /analisi <username> <today|week|month>   riepilogo dei movimenti
//! /log                                     ultime righe di log rilevanti
//! /aiuto                                   elenco dei comandi
//! /esci                                    spegnimento ordinato del server
//! ```
//!
//! Nomi dei comandi e alias degli intervalli si scrivono in minuscolo e il confronto
//! rispetta le maiuscole: l'alternativa sarebbe un `to_lowercase` per ogni riga, che
//! alloca, oppure un confronto che vale solo per l'ASCII, che tratterebbe diversamente
//! due parole a seconda di come sono scritte.
//!
//! # Dove possono stare gli spazi
//!
//! Solo l'ultimo campo di un comando puo' contenerli, ed e' sempre un testo libero: il
//! corpo di `/broadcast` e quello di `/msg`. Il destinatario e' percio' **la prima parola
//! dopo il nome del comando**, e uno username con uno spazio dentro non e' esprimibile.
//! Non e' una perdita: [`Username`](georuggine_common::types::Username) ammette solo
//! alfanumerici ASCII e underscore, quindi uno username con uno spazio non esiste. La
//! divisione avviene con [`str::split_once`] e [`str::split_whitespace`], mai indicizzando
//! i byte di una stringa: una riga con caratteri multibyte non puo' quindi far cadere il
//! server su un indice che non sta su un confine di carattere.
//!
//! # Cosa la console non sa ancora dire
//!
//! Tre limiti sono voluti e restano da colmare in requisiti successivi:
//!
//! - `/utenti` mostra "connesso" e non lo stato di movimento. Lo stato vive in
//!   [`StateTracker`](crate::state::StateTracker), che [`ConsoleContext`] non ha fra le
//!   proprie dipendenze: da qui non c'e' modo di leggerlo in modo pulito, e nessuna
//!   [`UserState`](georuggine_common::types::UserState) viene quindi stampata.
//! - l'ultima posizione nota si ottiene leggendo tutto lo storico dell'utente e tenendone
//!   l'ultima riga, perche' [`Repository`] non ha un metodo dedicato. Su un archivio
//!   grande e' una lettura sproporzionata a cio' che se ne ricava; aggiungere quel metodo
//!   e' un cambiamento del contratto della persistenza e non appartiene a questo passo.
//! - `/log` e' riconosciuto ma non legge ancora nulla: la lettura del file di log arriva
//!   con il requisito R15.
//!
//! Come nel resto del server i metodi del [`Repository`] sono sincroni, quindi ogni
//! accesso passa da `tokio::task::spawn_blocking`, e l'ora si legge dal [`Clock`]
//! iniettato e mai da `Utc::now()`.

use std::sync::Arc;

use chrono::{DateTime, SecondsFormat, TimeDelta, Utc};
use georuggine_common::clock::Clock;
use georuggine_common::protocol::AnalysisRange;
use georuggine_common::types::{PositionRecord, UserId};
use thiserror::Error;
use tokio::sync::broadcast;
use tokio::task;

use crate::analysis::{self, AnalysisRangeExt as _};
use crate::messaging::{MessageHub, MessagingError};
use crate::net::ConnectionRegistry;
use crate::storage::{Repository, StorageError};

/// Elenco dei comandi, stampato da `/aiuto`.
const AIUTO: &str = "\
Comandi disponibili:
  /utenti                                  elenco dei veicoli connessi, con ultima posizione
  /broadcast <testo>                       manda un avviso a tutta la flotta
  /msg <username> <testo>                  manda un messaggio diretto a un veicolo
  /analisi <username> <today|week|month>   riepiloga i movimenti di un veicolo
  /log                                     ultime righe di log rilevanti
  /aiuto                                   questo elenco
  /esci                                    avvia lo spegnimento ordinato del server";

/// Risposta di `/log` finche' la lettura del file non e' implementata.
const LOG_NON_DISPONIBILE: &str = "Le ultime righe di log non sono ancora disponibili: \
la lettura del file di log arriva con il requisito R15. Il comando esiste gia' e ne \
prepara il posto.";

/// Secondi di anticipo su [`Clock::now`] entro cui puo' trovarsi una posizione salvata.
///
/// L'istante di una rilevazione lo misura il client, e il livello di rete ne accetta fino
/// a cinque minuti di anticipo sull'orologio del server. Cercare l'ultima posizione nota
/// fino a `now` soltanto lascerebbe percio' fuori proprio le rilevazioni piu' recenti di
/// un veicolo il cui orologio va avanti.
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
    /// Mostra le ultime righe di log rilevanti.
    Log,
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
    /// Non e' un errore dell'operatore: e' una riga senza niente da eseguire, e chi
    /// chiama puo' semplicemente passare oltre senza stampare nulla. Sta fra gli errori
    /// perche' [`analizza_comando`] restituisce un [`Comando`], e per una riga vuota non
    /// c'e' nessuna variante da restituire.
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
/// una persona davanti a un terminale, non un chiamante che possa rimediare a un errore,
/// quindi distinguere i due casi in due canali diversi non gli servirebbe a nulla. Per la
/// stessa ragione [`esegui`] non restituisce un `Result`.
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
/// Sono gli stessi esemplari condivisi che il livello di rete riceve in
/// [`Dipendenze`](crate::net::Dipendenze): un registro o una messaggistica diversi non
/// vedrebbero nessuno dei veicoli collegati.
pub struct ConsoleContext {
    /// Persistenza di utenti, posizioni e messaggi.
    pub repo: Arc<dyn Repository>,
    /// Registro delle connessioni aperte, una per utente.
    pub registry: Arc<ConnectionRegistry>,
    /// Messaggistica, costruita sullo stesso [`registry`](Self::registry).
    pub messaging: Arc<MessageHub>,
    /// Sorgente dell'ora: la console non chiama mai `Utc::now()` da se'.
    pub clock: Arc<dyn Clock>,
    /// Canale su cui `/esci` annuncia lo spegnimento.
    pub shutdown: broadcast::Sender<()>,
}

/// Errore incontrato mentre si esegue un comando gia' riconosciuto.
///
/// Non esce dal modulo: [`esegui`] lo riduce al testo che l'operatore legge. Esiste come
/// enum, e non come stringa costruita sul posto, perche' i tre casi hanno origini
/// diverse (l'operatore, il database, il runtime) e il messaggio lo deve dire.
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
/// comando e tutto cio' che segue sono i suoi argomenti. Non tocca nessuna dipendenza e
/// non legge nulla: e' una funzione pura, e i suoi test non hanno bisogno di un server.
///
/// # Errori
///
/// - [`ErroreComando::RigaVuota`] se la riga e' vuota o fatta di soli spazi. Non e' un
///   errore vero: chi chiama la riconosce e passa alla riga successiva senza stampare;
/// - [`ErroreComando::Sconosciuto`] se il primo termine non e' un comando;
/// - [`ErroreComando::ArgomentoMancante`] se manca un argomento obbligatorio, per esempio
///   `/msg` senza destinatario o senza testo;
/// - [`ErroreComando::ArgomentiInEccesso`] se il comando ne riceve piu' di quanti ne
///   accetti, per esempio `/utenti tutti`;
/// - [`ErroreComando::IntervalloSconosciuto`] se l'intervallo di `/analisi` non e' uno
///   degli alias ammessi: `today`/`oggi`, `week`/`settimana`, `month`/`mese`.
pub fn analizza_comando(riga: &str) -> Result<Comando, ErroreComando> {
    let riga = riga.trim();
    if riga.is_empty() {
        return Err(ErroreComando::RigaVuota);
    }

    let (nome, resto) = spezza(riga);

    match nome {
        "/utenti" => senza_argomenti("/utenti", resto).map(|()| Comando::Utenti),
        "/log" => senza_argomenti("/log", resto).map(|()| Comando::Log),
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

/// Divide una riga gia' ripulita nel primo termine e in cio' che lo segue.
///
/// Il resto arriva senza gli spazi iniziali, cosi' chi lo riceve lo puo' trattare come
/// una riga a sua volta. Se non ci sono spazi, il resto e' vuoto. La divisione passa da
/// [`str::split_once`], che taglia su un confine di carattere: una riga multibyte non
/// puo' produrre un indice illegale.
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
/// Gli alias sono due per intervallo, uno inglese e uno italiano, e sono tutti qui:
/// `today`/`oggi`, `week`/`settimana`, `month`/`mese`. [`AnalysisRange::Custom`] non ha
/// un alias perche' la console non ha una sintassi per scriverne gli estremi; il resto
/// del modulo lo tratta comunque, visto che [`esegui`] accetta qualunque intervallo.
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
/// Non restituisce un `Result`: ogni esito, riuscito o fallito, diventa il testo di un
/// [`Esito`], perche' e' cio' che la console deve mostrare all'operatore. L'unico
/// comando che chiude il ciclo e' `/esci`, che manda anche il segnale di spegnimento al
/// resto del server.
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
        Comando::Log => Esito::parla(LOG_NON_DISPONIBILE),
        Comando::Aiuto => Esito::parla(AIUTO),
        Comando::Esci => {
            // Come in `main.rs`: l'errore significa solo che nessuno e' in ascolto, cioe'
            // che il server sta gia' terminando per un'altra via.
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

/// Elenca i veicoli connessi con la loro ultima posizione nota.
///
/// Lo stato di movimento non compare: vedi la nota nel modulo.
///
/// # Errori
///
/// [`ErroreEsecuzione::Persistenza`] o [`ErroreEsecuzione::Task`] se lo storico non e'
/// leggibile.
async fn elenca_utenti(contesto: &ConsoleContext) -> Result<String, ErroreEsecuzione> {
    let connessi = contesto.registry.connected_users().await;
    if connessi.is_empty() {
        return Ok("Nessun veicolo connesso.".to_owned());
    }

    let repo = Arc::clone(&contesto.repo);
    let adesso = contesto.clock.now();
    let voci = in_background(move || raccogli_voci(repo.as_ref(), &connessi, adesso)).await?;

    let mut testo = format!("Veicoli connessi ({}):", voci.len());
    for voce in &voci {
        testo.push_str("\n  ");
        testo.push_str(&voce.riga());
    }

    Ok(testo)
}

/// Riga dell'elenco di `/utenti`: chi e' connesso e dove si trovava l'ultima volta.
struct Voce {
    /// Identificatore dell'utente connesso.
    id: UserId,
    /// Username, assente se il repository non conosce piu' quell'id.
    nome: Option<String>,
    /// Ultima posizione nota, assente se l'utente non ne ha mai inviata una.
    ultima: Option<PositionRecord>,
}

impl Voce {
    /// Riga da mostrare all'operatore.
    fn riga(&self) -> String {
        // Un utente connesso e' per forza autenticato, quindi registrato: il caso senza
        // nome puo' nascere solo da un archivio manomesso, e mostrare l'id e' comunque
        // piu' utile che tacere la connessione.
        let nome = self
            .nome
            .clone()
            .unwrap_or_else(|| format!("#{} (utente non registrato)", self.id.0));

        match &self.ultima {
            Some(posizione) => format!(
                "{nome}  connesso  ultimo contatto {} a {:.6}, {:.6}",
                istante(posizione.recorded_at),
                posizione.coordinate.lat(),
                posizione.coordinate.lon(),
            ),
            None => format!("{nome}  connesso  nessuna posizione registrata"),
        }
    }
}

/// Raccoglie nome e ultima posizione di ogni utente connesso.
///
/// E' sincrona perche' il [`Repository`] lo e': il chiamante la porta su un thread da
/// lavoro. Le letture stanno tutte in una chiamata sola invece di una per utente, cosi'
/// il passaggio all'esecutore si paga una volta.
///
/// # Errori
///
/// [`StorageError`] se l'elenco degli utenti o lo storico non sono leggibili.
fn raccogli_voci(
    repo: &dyn Repository,
    connessi: &[UserId],
    adesso: DateTime<Utc>,
) -> Result<Vec<Voce>, StorageError> {
    let utenti = repo.list_users()?;
    let fine = fine_dello_storico(adesso);

    connessi
        .iter()
        .map(|id| {
            let nome = utenti
                .iter()
                .find(|utente| utente.id == *id)
                .map(|utente| utente.username.as_str().to_owned());
            // Manca un modo diretto di chiedere l'ultima posizione: si legge tutto lo
            // storico e se ne tiene la coda, che e' la piu' recente perche'
            // `positions_in_range` ordina per `recorded_at` crescente.
            let mut posizioni = repo.positions_in_range(*id, DateTime::<Utc>::MIN_UTC, fine)?;

            Ok(Voce {
                id: *id,
                nome,
                ultima: posizioni.pop(),
            })
        })
        .collect()
}

/// Estremo superiore dell'intervallo che contiene tutto lo storico.
///
/// Non e' `DateTime::<Utc>::MAX_UTC` perche' [`SqliteRepository`] confronta gli istanti
/// come testo: `MAX_UTC` si scrive con l'anno preceduto da `+`, che nella tabella ASCII
/// viene prima delle cifre, quindi risulterebbe minore di qualunque data reale e
/// l'intervallo sarebbe sempre vuoto. `MIN_UTC` come estremo inferiore non ha lo stesso
/// problema: il suo `-` viene prima delle cifre, che e' proprio l'ordine voluto.
///
/// [`SqliteRepository`]: crate::storage::SqliteRepository
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
         soste:              {}",
        nome_intervallo(range),
        istante(from),
        istante(to),
        concorda(posizioni.len(), "posizione", "posizioni"),
        riepilogo.total_distance_meters,
        riepilogo.average_speed_kmh,
        formatta_durata(riepilogo.moving_duration_secs),
        formatta_durata(riepilogo.pause_duration_secs),
        riepilogo.stop_count,
    ))
}

/// Cerca l'id dell'utente che ha lo username indicato.
///
/// Il confronto avviene sul testo e non su un [`Username`](georuggine_common::types::Username)
/// costruito dall'argomento: cosi' un nome che non supererebbe nemmeno la validazione, per
/// esempio con un trattino, produce lo stesso "utente sconosciuto" di un nome ben formato
/// ma inesistente, che e' cio' che all'operatore interessa sapere.
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

/// Esegue una lettura del repository su un thread da lavoro.
///
/// I metodi del [`Repository`] sono sincroni: chiamarli qui fermerebbe un thread
/// dell'esecutore per tutta la durata dell'accesso a SQLite.
///
/// # Errori
///
/// [`ErroreEsecuzione::Persistenza`] se la lettura fallisce, [`ErroreEsecuzione::Task`]
/// se l'attivita' non arriva in fondo, cosa che accade solo se il runtime si sta
/// spegnendo.
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

    use crate::storage::{InMemoryRepository, PendingMessage, SqliteRepository};

    /// Messaggi che una coda di prova puo' accodare prima di riempirsi.
    const CAPACITA: usize = 4;

    /// Segnali di spegnimento che il canale di prova conserva.
    const CAPACITA_SPEGNIMENTO: usize = 1;

    /// Gradi di latitudine che valgono circa 100 m, come nei test di [`analysis`].
    const GRADI_PER_CENTO_METRI: f64 = 0.000_9;

    /// Latitudine di partenza dei test: piazza Castello, Torino.
    const LAT_PARTENZA: f64 = 45.0703;

    /// Longitudine di partenza dei test.
    const LON_PARTENZA: f64 = 7.6869;

    // --- analizza_comando ---

    #[test]
    fn ogni_comando_valido_nella_forma_minima_e_riconosciuto() {
        for (riga, atteso) in [
            ("/utenti", Comando::Utenti),
            ("/log", Comando::Log),
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
    fn gli_spazi_ai_bordi_della_riga_non_contano() {
        assert_eq!(analizza_comando("   /utenti  \n"), Ok(Comando::Utenti));
        assert_eq!(
            analizza_comando("  /msg  veicolo_01   rientra in sede  \n"),
            Ok(Comando::Messaggio {
                destinatario: "veicolo_01".to_owned(),
                testo: "rientra in sede".to_owned(),
            })
        );
    }

    #[test]
    fn il_testo_di_broadcast_e_msg_puo_contenere_spazi() {
        assert_eq!(
            analizza_comando("/broadcast manutenzione alle 18 in sede"),
            Ok(Comando::Broadcast(
                "manutenzione alle 18 in sede".to_owned()
            ))
        );
        assert_eq!(
            analizza_comando("/msg veicolo_01 torna in sede subito"),
            Ok(Comando::Messaggio {
                destinatario: "veicolo_01".to_owned(),
                testo: "torna in sede subito".to_owned(),
            })
        );
    }

    #[test]
    fn msg_senza_testo_e_un_errore_leggibile() {
        let errore = analizza_comando("/msg veicolo_01").expect_err("manca il testo");

        assert_eq!(
            errore,
            ErroreComando::ArgomentoMancante {
                comando: "/msg",
                argomento: "testo",
            }
        );
        assert_eq!(errore.to_string(), "/msg: manca l'argomento <testo>");
    }

    #[test]
    fn msg_senza_destinatario_e_un_errore_leggibile() {
        for riga in ["/msg", "/msg   "] {
            assert_eq!(
                analizza_comando(riga),
                Err(ErroreComando::ArgomentoMancante {
                    comando: "/msg",
                    argomento: "destinatario",
                }),
                "riga {riga:?}"
            );
        }
    }

    #[test]
    fn broadcast_senza_testo_e_un_errore() {
        assert_eq!(
            analizza_comando("/broadcast   "),
            Err(ErroreComando::ArgomentoMancante {
                comando: "/broadcast",
                argomento: "testo",
            })
        );
    }

    #[test]
    fn analisi_con_intervallo_non_riconosciuto_e_un_errore() {
        assert_eq!(
            analizza_comando("/analisi veicolo_01 ieri"),
            Err(ErroreComando::IntervalloSconosciuto("ieri".to_owned()))
        );
        // Un intervallo vuoto non e' una parola che non si riconosce: e' un argomento che
        // non c'e', e il messaggio lo dice.
        assert_eq!(
            analizza_comando("/analisi veicolo_01   "),
            Err(ErroreComando::ArgomentoMancante {
                comando: "/analisi",
                argomento: "intervallo",
            })
        );
    }

    #[test]
    fn analisi_senza_destinatario_e_un_errore() {
        assert_eq!(
            analizza_comando("/analisi"),
            Err(ErroreComando::ArgomentoMancante {
                comando: "/analisi",
                argomento: "destinatario",
            })
        );
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
    }

    #[test]
    fn un_comando_sconosciuto_e_un_errore() {
        for riga in ["/pippo", "/pippo con argomenti", "utenti", "ciao"] {
            let errore = analizza_comando(riga).expect_err("non e' un comando");
            assert!(
                matches!(errore, ErroreComando::Sconosciuto(_)),
                "riga {riga:?}: atteso Sconosciuto, ottenuto {errore:?}"
            );
        }
    }

    #[test]
    fn una_riga_vuota_o_di_soli_spazi_non_e_un_comando() {
        for riga in ["", "   ", "\t", "  \r\n"] {
            assert_eq!(
                analizza_comando(riga),
                Err(ErroreComando::RigaVuota),
                "riga {riga:?}"
            );
        }
    }

    #[test]
    fn i_comandi_senza_argomenti_rifiutano_quelli_in_piu() {
        for (riga, comando) in [
            ("/utenti tutti", "/utenti"),
            ("/log 20", "/log"),
            ("/aiuto tutto", "/aiuto"),
            ("/esci subito", "/esci"),
        ] {
            assert_eq!(
                analizza_comando(riga),
                Err(ErroreComando::ArgomentiInEccesso { comando }),
                "riga {riga:?}"
            );
        }
    }

    #[test]
    fn analisi_con_un_argomento_di_troppo_e_un_errore() {
        assert_eq!(
            analizza_comando("/analisi veicolo_01 today e anche domani"),
            Err(ErroreComando::ArgomentiInEccesso {
                comando: "/analisi"
            })
        );
    }

    #[test]
    fn un_testo_multibyte_non_manda_in_panico_il_parsing() {
        // Caratteri che occupano piu' di un byte in UTF-8: se la divisione indicizzasse i
        // byte invece di tagliare sui confini di carattere, questa riga farebbe cadere il
        // server.
        assert_eq!(
            analizza_comando("/broadcast attenzione: neve 🚚 sull'A4 città"),
            Ok(Comando::Broadcast(
                "attenzione: neve 🚚 sull'A4 città".to_owned()
            ))
        );
        assert_eq!(
            analizza_comando("/msg veicolo_01 città sommersa 🚚"),
            Ok(Comando::Messaggio {
                destinatario: "veicolo_01".to_owned(),
                testo: "città sommersa 🚚".to_owned(),
            })
        );
    }

    #[test]
    fn un_destinatario_multibyte_non_manda_in_panico_il_parsing() {
        // Non e' uno username valido, ma il parsing non lo sa e non deve nemmeno
        // provarci: deve solo restituirlo senza cadere.
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
        assert_eq!(
            ErroreComando::RigaVuota.to_string(),
            "riga vuota: nessun comando da eseguire"
        );
    }

    // --- banco di prova per esegui ---

    /// Console sotto esame e le dipendenze che il test ispeziona.
    struct Banco {
        /// Dipendenze passate a [`esegui`].
        contesto: ConsoleContext,
        /// Persistenza in memoria, la stessa che il contesto usa.
        repo: Arc<InMemoryRepository>,
        /// Orologio fermo, la stessa sorgente che il contesto legge.
        orologio: Arc<FakeClock>,
    }

    /// Istante UTC del mezzogiorno locale del 2026-08-13.
    ///
    /// Le tre varianti di calendario di [`AnalysisRange`] partono dalla mezzanotte
    /// **locale**: partire da mezzogiorno tiene le posizioni dei test dentro la giornata
    /// locale qualunque sia il fuso della macchina che li esegue.
    fn mezzogiorno_locale() -> DateTime<Utc> {
        let data = NaiveDate::from_ymd_opt(2026, 8, 13).expect("data di test valida");
        let orario = NaiveTime::from_hms_opt(12, 0, 0).expect("orario di test valido");
        match Local.from_local_datetime(&data.and_time(orario)) {
            MappedLocalTime::Single(istante) => istante.to_utc(),
            MappedLocalTime::Ambiguous(prima, _) => prima.to_utc(),
            MappedLocalTime::None => panic!("il mezzogiorno locale deve esistere"),
        }
    }

    /// Monta una console su un repository in memoria e un orologio fermo.
    fn banco() -> Banco {
        let orologio = Arc::new(FakeClock::new(mezzogiorno_locale()));
        let repo = Arc::new(InMemoryRepository::new(
            Arc::clone(&orologio) as Arc<dyn Clock>
        ));
        let registry = Arc::new(ConnectionRegistry::new());
        let messaging = Arc::new(MessageHub::new(
            Arc::clone(&repo) as Arc<dyn Repository>,
            Arc::clone(&registry),
            Arc::clone(&orologio) as Arc<dyn Clock>,
        ));
        let (shutdown, _) = broadcast::channel(CAPACITA_SPEGNIMENTO);

        Banco {
            contesto: ConsoleContext {
                repo: Arc::clone(&repo) as Arc<dyn Repository>,
                registry,
                messaging,
                clock: Arc::clone(&orologio) as Arc<dyn Clock>,
                shutdown,
            },
            repo,
            orologio,
        }
    }

    /// Registra un utente e ne restituisce l'id.
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

    // --- esegui: utenti ---

    #[tokio::test]
    async fn utenti_senza_nessuno_collegato_lo_dice() {
        let banco = banco();

        let esito = esegui(Comando::Utenti, &banco.contesto).await;

        assert_eq!(esito.testo, "Nessun veicolo connesso.");
        assert!(!esito.termina);
    }

    #[tokio::test]
    async fn utenti_elenca_i_connessi_con_l_ultima_posizione() {
        let banco = banco();
        let primo = registra(&banco, "veicolo_01");
        let secondo = registra(&banco, "veicolo_02");
        // Il terzo utente e' registrato ma non collegato: non deve comparire.
        registra(&banco, "veicolo_03");
        let _code = (
            connetti(&banco, primo).await,
            connetti(&banco, secondo).await,
        );
        registra_posizione(&banco, primo, 0.0, -120);
        registra_posizione(&banco, primo, 1.0, -60);

        let esito = esegui(Comando::Utenti, &banco.contesto).await;

        assert!(
            esito.testo.starts_with("Veicoli connessi (2):"),
            "intestazione inattesa: {}",
            esito.testo
        );
        assert!(
            esito
                .testo
                .contains("veicolo_01  connesso  ultimo contatto"),
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
                .contains("veicolo_02  connesso  nessuna posizione registrata"),
            "manca il secondo veicolo: {}",
            esito.testo
        );
        assert!(
            !esito.testo.contains("veicolo_03"),
            "un utente non connesso non va elencato: {}",
            esito.testo
        );
    }

    #[tokio::test]
    async fn utenti_trova_l_ultima_posizione_anche_su_sqlite() {
        // L'intervallo con cui si cerca l'ultima posizione parte da `MIN_UTC`, e regge
        // solo se il confronto testuale di `SqliteRepository` conserva l'ordine
        // cronologico. `InMemoryRepository` confronta i `DateTime` e non se ne
        // accorgerebbe: questo test e' l'unico posto in cui quella scelta viene provata.
        let orologio = Arc::new(FakeClock::new(mezzogiorno_locale()));
        let repo: Arc<dyn Repository> = Arc::new(
            SqliteRepository::new(":memory:", Arc::clone(&orologio) as Arc<dyn Clock>)
                .expect("il database in memoria si apre"),
        );
        let registry = Arc::new(ConnectionRegistry::new());
        let contesto = ConsoleContext {
            repo: Arc::clone(&repo),
            registry: Arc::clone(&registry),
            messaging: Arc::new(MessageHub::new(
                Arc::clone(&repo),
                Arc::clone(&registry),
                Arc::clone(&orologio) as Arc<dyn Clock>,
            )),
            clock: Arc::clone(&orologio) as Arc<dyn Clock>,
            shutdown: broadcast::channel(CAPACITA_SPEGNIMENTO).0,
        };

        let username = Username::new("veicolo_01").expect("username di test valido");
        let user_id = repo
            .create_user(&username, "hash")
            .expect("la registrazione riesce");
        let (mittente, _coda) = mpsc::channel(CAPACITA);
        registry.register(user_id, mittente).await;
        for (passi, secondi) in [(0.0, -120), (1.0, -60)] {
            repo.insert_position(&PositionRecord {
                user_id,
                coordinate: Coordinate::new(
                    LAT_PARTENZA + passi * GRADI_PER_CENTO_METRI,
                    LON_PARTENZA,
                )
                .expect("coordinata di test valida"),
                recorded_at: orologio.now() + TimeDelta::seconds(secondi),
            })
            .expect("la scrittura riesce");
        }

        let esito = esegui(Comando::Utenti, &contesto).await;

        assert!(
            esito
                .testo
                .contains(&istante(orologio.now() - TimeDelta::seconds(60))),
            "l'ultima posizione deve essere quella piu' recente: {}",
            esito.testo
        );
    }

    // --- esegui: broadcast ---

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
        assert!(!esito.termina);
        let atteso = ServerMessage::Broadcast {
            body: "manutenzione alle 18".to_owned(),
            sent_at: banco.orologio.now(),
        };
        assert_eq!(prima.recv().await, Some(atteso.clone()));
        assert_eq!(seconda.recv().await, Some(atteso));
    }

    #[tokio::test]
    async fn broadcast_a_flotta_ferma_accorda_il_singolare() {
        let banco = banco();
        let _coda = connetti(&banco, UserId(1)).await;

        let esito = esegui(Comando::Broadcast("chiusura".to_owned()), &banco.contesto).await;

        assert_eq!(esito.testo, "Avviso inviato a 1 veicolo connesso.");
    }

    // --- esegui: messaggi diretti ---

    #[tokio::test]
    async fn messaggio_a_un_utente_non_connesso_resta_in_coda() {
        let banco = banco();
        let user_id = registra(&banco, "veicolo_01");

        let esito = esegui(
            Comando::Messaggio {
                destinatario: "veicolo_01".to_owned(),
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
                .pending_messages(user_id)
                .expect("lettura riuscita"),
            vec![PendingMessage {
                body: "rientra in sede".to_owned(),
                sent_at: banco.orologio.now(),
            }]
        );
    }

    #[tokio::test]
    async fn messaggio_a_un_utente_connesso_e_consegnato_subito() {
        let banco = banco();
        let user_id = registra(&banco, "veicolo_01");
        let mut coda = connetti(&banco, user_id).await;

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
    }

    #[tokio::test]
    async fn messaggio_a_uno_username_inesistente_non_scrive_nulla() {
        let banco = banco();
        let esistente = registra(&banco, "veicolo_01");

        let esito = esegui(
            Comando::Messaggio {
                destinatario: "pippo".to_owned(),
                testo: "rientra".to_owned(),
            },
            &banco.contesto,
        )
        .await;

        assert_eq!(esito.testo, "utente sconosciuto: \"pippo\"");
        assert!(!esito.termina);
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

    // --- esegui: analisi ---

    #[tokio::test]
    async fn analisi_di_un_utente_con_posizioni_produce_numeri_coerenti() {
        let banco = banco();
        let user_id = registra(&banco, "veicolo_01");
        // Dieci punti in linea retta, uno scatto di circa 100 m ogni 10 s: gli stessi
        // numeri gia' verificati in `analysis.rs`, cioe' 900 m in 90 s a 36 km/h.
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
        assert!(
            esito.testo.contains("(10 posizioni)"),
            "le posizioni lette vanno riferite: {}",
            esito.testo
        );
        assert!(
            esito.testo.contains("velocita' media:    36.0 km/h"),
            "velocita' media inattesa: {}",
            esito.testo
        );
        assert!(
            esito.testo.contains("tempo in movimento: 1m 30s"),
            "durata in movimento inattesa: {}",
            esito.testo
        );
        assert!(
            esito.testo.contains("tempo in pausa:     0s"),
            "durata in pausa inattesa: {}",
            esito.testo
        );
        assert!(
            esito.testo.contains("soste:              0"),
            "soste inattese: {}",
            esito.testo
        );
        // La distanza dipende dalla formula dell'emisenoverso, gia' verificata altrove:
        // qui basta che sia quella dei punti letti e non un valore inventato.
        let atteso = analysis::analyze(
            &banco
                .repo
                .positions_in_range(
                    user_id,
                    banco.orologio.now() - TimeDelta::seconds(120),
                    banco.orologio.now(),
                )
                .expect("lettura riuscita"),
        );
        assert!(
            esito
                .testo
                .contains(&format!("{:.1} m", atteso.total_distance_meters)),
            "distanza inattesa: {}",
            esito.testo
        );
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
        assert!(esito.testo.contains("soste:              0"));
    }

    #[tokio::test]
    async fn analisi_di_uno_username_inesistente_e_un_errore() {
        let banco = banco();

        let esito = esegui(
            Comando::Analisi {
                destinatario: "pippo".to_owned(),
                range: AnalysisRange::Today,
            },
            &banco.contesto,
        )
        .await;

        assert_eq!(esito.testo, "utente sconosciuto: \"pippo\"");
        assert!(!esito.termina);
    }

    // --- esegui: log, aiuto, esci ---

    #[tokio::test]
    async fn log_annuncia_che_arriva_con_r15() {
        let banco = banco();

        let esito = esegui(Comando::Log, &banco.contesto).await;

        assert!(
            esito.testo.contains("R15"),
            "il segnaposto deve dire quando arriva il log vero: {}",
            esito.testo
        );
        assert!(!esito.termina);
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
            "/log",
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
        assert_eq!(
            spegnimento.recv().await,
            Ok(()),
            "il segnale deve raggiungere chi ascolta"
        );
    }

    #[tokio::test]
    async fn esci_non_fallisce_se_nessuno_ascolta() {
        let banco = banco();

        // Nessun iscritto al canale: `send` restituisce un errore che non riguarda
        // l'operatore, e la console deve comunque congedarsi.
        let esito = esegui(Comando::Esci, &banco.contesto).await;

        assert!(esito.termina);
        assert!(esito.testo.contains("Spegnimento"));
    }

    // --- formattazione ---

    #[test]
    fn formatta_durata_mostra_solo_le_unita_utili() {
        assert_eq!(formatta_durata(0), "0s");
        assert_eq!(formatta_durata(45), "45s");
        assert_eq!(formatta_durata(90), "1m 30s");
        assert_eq!(formatta_durata(3_600), "1h 00m 00s");
        assert_eq!(formatta_durata(7_505), "2h 05m 05s");
    }

    #[test]
    fn concorda_usa_il_singolare_solo_per_uno() {
        assert_eq!(concorda(0, "veicolo", "veicoli"), "0 veicoli");
        assert_eq!(concorda(1, "veicolo", "veicoli"), "1 veicolo");
        assert_eq!(concorda(2, "veicolo", "veicoli"), "2 veicoli");
    }
}
