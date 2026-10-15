//! Analisi dei comandi digitati nella riga di input della TUI.
//!
//! Solo `/msg` accetta testo con spazi. Una riga vuota o senza `/` iniziale non e' un
//! errore: vale [`ErroreInput::NienteDaFare`] e viene ignorata.

use georuggine_common::protocol::AnalysisRange;
use georuggine_common::types::{Coordinate, CoordinateError};
use thiserror::Error;

/// Comando riconosciuto dalla riga di input della TUI.
#[derive(Debug, Clone, PartialEq)]
pub enum ComandoUtente {
    /// Manda un messaggio testuale al server (`/msg <testo>`).
    Messaggio(String),
    /// Imposta la posizione corrente su una sorgente manuale (`/pos <lat> <lon>`).
    PosizioneManuale(Coordinate),
    /// Chiede il riepilogo dei movimenti su un intervallo (`/analisi <today|week|month>`).
    Analisi(AnalysisRange),
    /// Chiude la sessione e termina la TUI (`/quit`).
    Esci,
}

/// Errore restituito da [`analizza_input`] quando la riga non e' un comando eseguibile.
#[derive(Debug, Clone, PartialEq, Error)]
pub enum ErroreInput {
    /// Riga vuota o senza `/` iniziale: nulla da eseguire.
    #[error("nessun comando da eseguire")]
    NienteDaFare,
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
    /// Un campo che deve essere numerico non lo e'.
    #[error("{comando}: {campo} non numerico: {valore:?}")]
    NumeroNonValido {
        /// Nome del comando, come si scrive nella riga.
        comando: &'static str,
        /// Nome del campo che non si interpreta come numero.
        campo: &'static str,
        /// Testo fornito, non interpretabile.
        valore: String,
    },
    /// Latitudine o longitudine numeriche ma fuori dagli intervalli ammessi.
    #[error("/pos: coordinata non valida: {0}")]
    CoordinataNonValida(#[from] CoordinateError),
    /// L'intervallo di `/analisi` non corrisponde a nessuno dei tre ammessi.
    #[error("/analisi: intervallo {0:?} non riconosciuto: usa today, week o month")]
    IntervalloSconosciuto(String),
    /// Il primo termine della riga comincia per `/` ma non e' un comando conosciuto.
    #[error("comando sconosciuto {0:?}")]
    ComandoSconosciuto(String),
}

/// Traduce una riga nel comando corrispondente.
///
/// # Errori
///
/// La variante di [`ErroreInput`] che descrive il problema.
pub fn analizza_input(riga: &str) -> Result<ComandoUtente, ErroreInput> {
    let riga = riga.trim();
    if !riga.starts_with('/') {
        return Err(ErroreInput::NienteDaFare);
    }

    let (nome, resto) = spezza(riga);

    match nome {
        "/quit" => senza_argomenti("/quit", resto).map(|()| ComandoUtente::Esci),
        "/msg" => {
            if resto.is_empty() {
                return Err(ErroreInput::ArgomentoMancante {
                    comando: "/msg",
                    argomento: "testo",
                });
            }
            Ok(ComandoUtente::Messaggio(resto.trim_end().to_owned()))
        }
        "/pos" => match resto.split_whitespace().collect::<Vec<&str>>().as_slice() {
            [] => Err(ErroreInput::ArgomentoMancante {
                comando: "/pos",
                argomento: "lat",
            }),
            [_] => Err(ErroreInput::ArgomentoMancante {
                comando: "/pos",
                argomento: "lon",
            }),
            [lat, lon] => {
                let lat = analizza_f64("/pos", "lat", lat)?;
                let lon = analizza_f64("/pos", "lon", lon)?;
                Ok(ComandoUtente::PosizioneManuale(Coordinate::new(lat, lon)?))
            }
            _ => Err(ErroreInput::ArgomentiInEccesso { comando: "/pos" }),
        },
        "/analisi" => match resto.split_whitespace().collect::<Vec<&str>>().as_slice() {
            [] => Err(ErroreInput::ArgomentoMancante {
                comando: "/analisi",
                argomento: "intervallo",
            }),
            [intervallo] => Ok(ComandoUtente::Analisi(analizza_intervallo(intervallo)?)),
            _ => Err(ErroreInput::ArgomentiInEccesso {
                comando: "/analisi",
            }),
        },
        altro => Err(ErroreInput::ComandoSconosciuto(altro.to_owned())),
    }
}

/// Divide una riga nel primo termine e nel resto, su un confine di carattere.
fn spezza(riga: &str) -> (&str, &str) {
    match riga.split_once(char::is_whitespace) {
        Some((primo, resto)) => (primo, resto.trim_start()),
        None => (riga, ""),
    }
}

/// Verifica che il comando non abbia argomenti.
///
/// # Errori
///
/// [`ErroreInput::ArgomentiInEccesso`] se ce ne sono.
fn senza_argomenti(comando: &'static str, resto: &str) -> Result<(), ErroreInput> {
    if resto.is_empty() {
        Ok(())
    } else {
        Err(ErroreInput::ArgomentiInEccesso { comando })
    }
}

/// Interpreta un campo come `f64`.
///
/// # Errori
///
/// [`ErroreInput::NumeroNonValido`] se non e' un numero.
fn analizza_f64(
    comando: &'static str,
    campo: &'static str,
    valore: &str,
) -> Result<f64, ErroreInput> {
    valore
        .trim()
        .parse()
        .map_err(|_| ErroreInput::NumeroNonValido {
            comando,
            campo,
            valore: valore.to_owned(),
        })
}

/// Traduce `today`, `week` o `month` in [`AnalysisRange`].
///
/// # Errori
///
/// [`ErroreInput::IntervalloSconosciuto`] per qualunque altra parola.
fn analizza_intervallo(parola: &str) -> Result<AnalysisRange, ErroreInput> {
    match parola {
        "today" => Ok(AnalysisRange::Today),
        "week" => Ok(AnalysisRange::CurrentWeek),
        "month" => Ok(AnalysisRange::CurrentMonth),
        altro => Err(ErroreInput::IntervalloSconosciuto(altro.to_owned())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- forma minima dei quattro comandi ---

    #[test]
    fn ogni_comando_valido_nella_forma_minima_e_riconosciuto() {
        assert_eq!(
            analizza_input("/msg rientra in sede"),
            Ok(ComandoUtente::Messaggio("rientra in sede".to_owned()))
        );
        assert_eq!(
            analizza_input("/pos 45.07 7.68"),
            Ok(ComandoUtente::PosizioneManuale(
                Coordinate::new(45.07, 7.68).expect("coordinata di test valida")
            ))
        );
        assert_eq!(
            analizza_input("/analisi today"),
            Ok(ComandoUtente::Analisi(AnalysisRange::Today))
        );
        assert_eq!(
            analizza_input("/analisi week"),
            Ok(ComandoUtente::Analisi(AnalysisRange::CurrentWeek))
        );
        assert_eq!(
            analizza_input("/analisi month"),
            Ok(ComandoUtente::Analisi(AnalysisRange::CurrentMonth))
        );
        assert_eq!(analizza_input("/quit"), Ok(ComandoUtente::Esci));
    }

    #[test]
    fn gli_spazi_ai_bordi_della_riga_non_contano() {
        assert_eq!(analizza_input("   /quit  \n"), Ok(ComandoUtente::Esci));
        assert_eq!(
            analizza_input("  /pos   45.07   7.68  \n"),
            Ok(ComandoUtente::PosizioneManuale(
                Coordinate::new(45.07, 7.68).expect("coordinata di test valida")
            ))
        );
    }

    // --- /pos: argomenti mancanti o non numerici ---

    #[test]
    fn pos_con_argomenti_mancanti_e_un_errore_leggibile() {
        assert_eq!(
            analizza_input("/pos"),
            Err(ErroreInput::ArgomentoMancante {
                comando: "/pos",
                argomento: "lat",
            })
        );
        assert_eq!(
            analizza_input("/pos 45.07"),
            Err(ErroreInput::ArgomentoMancante {
                comando: "/pos",
                argomento: "lon",
            })
        );
    }

    #[test]
    fn pos_con_argomenti_non_numerici_non_manda_in_panico_il_parsing() {
        assert_eq!(
            analizza_input("/pos abc 7.68"),
            Err(ErroreInput::NumeroNonValido {
                comando: "/pos",
                campo: "lat",
                valore: "abc".to_owned(),
            })
        );
        assert_eq!(
            analizza_input("/pos 45.07 xyz"),
            Err(ErroreInput::NumeroNonValido {
                comando: "/pos",
                campo: "lon",
                valore: "xyz".to_owned(),
            })
        );
    }

    #[test]
    fn pos_con_coordinata_fuori_intervallo_e_un_errore() {
        assert!(matches!(
            analizza_input("/pos 91.0 7.68"),
            Err(ErroreInput::CoordinataNonValida(_))
        ));
    }

    #[test]
    fn pos_con_argomenti_di_troppo_e_un_errore() {
        assert_eq!(
            analizza_input("/pos 45.07 7.68 10.0"),
            Err(ErroreInput::ArgomentiInEccesso { comando: "/pos" })
        );
    }

    // --- /analisi: intervallo non riconosciuto ---

    #[test]
    fn analisi_con_intervallo_non_riconosciuto_e_un_errore() {
        assert_eq!(
            analizza_input("/analisi ieri"),
            Err(ErroreInput::IntervalloSconosciuto("ieri".to_owned()))
        );
        assert_eq!(
            analizza_input("/analisi oggi"),
            Err(ErroreInput::IntervalloSconosciuto("oggi".to_owned())),
            "solo gli alias inglesi sono ammessi in questa TUI"
        );
    }

    #[test]
    fn analisi_senza_intervallo_e_un_errore() {
        assert_eq!(
            analizza_input("/analisi"),
            Err(ErroreInput::ArgomentoMancante {
                comando: "/analisi",
                argomento: "intervallo",
            })
        );
    }

    // --- /msg e /quit: argomenti mancanti o in eccesso ---

    #[test]
    fn msg_senza_testo_e_un_errore() {
        for riga in ["/msg", "/msg   "] {
            assert_eq!(
                analizza_input(riga),
                Err(ErroreInput::ArgomentoMancante {
                    comando: "/msg",
                    argomento: "testo",
                }),
                "riga {riga:?}"
            );
        }
    }

    #[test]
    fn quit_con_argomenti_e_un_errore() {
        assert_eq!(
            analizza_input("/quit adesso"),
            Err(ErroreInput::ArgomentiInEccesso { comando: "/quit" })
        );
    }

    // --- righe senza comando ---

    #[test]
    fn una_riga_senza_slash_iniziale_non_e_un_errore() {
        for riga in ["ciao", "utenti", "45.07 7.68"] {
            assert_eq!(
                analizza_input(riga),
                Err(ErroreInput::NienteDaFare),
                "riga {riga:?}"
            );
        }
    }

    #[test]
    fn una_riga_vuota_o_di_soli_spazi_non_e_un_errore() {
        for riga in ["", "   ", "\t", "  \r\n"] {
            assert_eq!(
                analizza_input(riga),
                Err(ErroreInput::NienteDaFare),
                "riga {riga:?}"
            );
        }
    }

    #[test]
    fn un_comando_sconosciuto_e_un_errore() {
        assert_eq!(
            analizza_input("/pippo con argomenti"),
            Err(ErroreInput::ComandoSconosciuto("/pippo".to_owned()))
        );
    }

    // --- Unicode multibyte ---

    #[test]
    fn un_messaggio_con_caratteri_multibyte_non_manda_in_panico_il_parsing() {
        assert_eq!(
            analizza_input("/msg attenzione: neve 🚚 sull'A4 città"),
            Ok(ComandoUtente::Messaggio(
                "attenzione: neve 🚚 sull'A4 città".to_owned()
            ))
        );
    }

    #[test]
    fn un_comando_sconosciuto_multibyte_non_manda_in_panico_il_parsing() {
        assert_eq!(
            analizza_input("/città 🚚"),
            Err(ErroreInput::ComandoSconosciuto("/città".to_owned()))
        );
    }

    // --- messaggi degli errori ---

    #[test]
    fn errore_input_si_mostra_in_italiano() {
        assert_eq!(
            ErroreInput::NienteDaFare.to_string(),
            "nessun comando da eseguire"
        );
        assert_eq!(
            ErroreInput::ComandoSconosciuto("/pippo".to_owned()).to_string(),
            "comando sconosciuto \"/pippo\""
        );
        assert_eq!(
            ErroreInput::IntervalloSconosciuto("ieri".to_owned()).to_string(),
            "/analisi: intervallo \"ieri\" non riconosciuto: usa today, week o month"
        );
    }
}
