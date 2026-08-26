//! Framing dei messaggi di protocollo su TCP.
//!
//! TCP e' un flusso di byte senza confini di messaggio: una `read` puo' restituire mezzo
//! messaggio, o due messaggi attaccati. [`MessageCodec`] ricostruisce i confini con un
//! frame a prefisso di lunghezza:
//!
//! ```text
//! +--------------------+---------------------------+
//! | lunghezza: u32 BE  | payload postcard (N byte)  |
//! +--------------------+---------------------------+
//! ```
//!
//! La lunghezza e' big-endian, l'ordine di rete, cosi' il formato sul filo non dipende
//! dall'architettura dei due peer.
//!
//! [`CodecError::FrameTooLarge`] e [`CodecError::Serialization`] non sono recuperabili
//! sulla stessa connessione: il flusso resta disallineato rispetto ai confini dei frame,
//! quindi il chiamante deve chiudere la connessione anziche' continuare a leggere.

use std::io;
use std::marker::PhantomData;

use serde::Serialize;
use serde::de::DeserializeOwned;
use thiserror::Error;
use tokio_util::bytes::{Buf, BufMut, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

use crate::protocol::{ClientMessage, ServerMessage};

/// Dimensione massima ammessa per il payload di un frame, in byte.
///
/// Sta diversi ordini di grandezza sopra al messaggio piu' grande che il protocollo
/// prevede e limita la memoria che una singola connessione puo' far riservare.
pub const MAX_FRAME_BYTES: usize = 1 << 20;

/// Byte occupati dal prefisso di lunghezza che apre ogni frame.
const LENGTH_PREFIX_BYTES: usize = size_of::<u32>();

/// Errore prodotto dalla codifica o dalla decodifica di un frame.
#[derive(Debug, Error)]
pub enum CodecError {
    /// Guasto di I/O sulla connessione sottostante.
    #[error("errore di I/O sulla connessione: {0}")]
    Io(#[from] io::Error),
    /// Il frame supera [`MAX_FRAME_BYTES`], in codifica o nella lunghezza dichiarata.
    #[error("frame di {len} byte: il massimo ammesso e' {} byte", MAX_FRAME_BYTES)]
    FrameTooLarge {
        /// Dimensione del payload, in byte.
        len: u64,
    },
    /// I byte del payload non compongono un messaggio del tipo atteso.
    #[error("payload non conforme al protocollo: {0}")]
    Serialization(#[from] postcard::Error),
}

/// Codec a prefisso di lunghezza per i messaggi `T` serializzati con postcard.
///
/// Si usa con `Framed::new(stream, MessageCodec::new())`. Il codec e' privo di stato: la
/// riassemblazione dei frame parziali vive nel buffer che `Framed` gli passa. Ogni
/// estremo usa il codec del proprio verso: il client legge [`ServerMessage`] e scrive
/// [`ClientMessage`], il server fa l'opposto.
pub struct MessageCodec<T> {
    /// Fissa il tipo dei messaggi senza occupare spazio. La forma `fn(T) -> T` rende il
    /// codec `Send` e `Sync` qualunque sia `T`, cosa che un `PhantomData<T>` non farebbe.
    _message: PhantomData<fn(T) -> T>,
}

impl<T> MessageCodec<T> {
    /// Costruisce un codec per i messaggi di tipo `T`.
    pub const fn new() -> Self {
        Self {
            _message: PhantomData,
        }
    }
}

impl<T> Default for MessageCodec<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Encoder<T> for MessageCodec<T>
where
    T: Serialize,
{
    type Error = CodecError;

    /// Serializza il messaggio e lo accoda a `dst` preceduto dalla sua lunghezza.
    ///
    /// # Errori
    ///
    /// [`CodecError::Serialization`] se il messaggio non e' serializzabile e
    /// [`CodecError::FrameTooLarge`] se il payload supera [`MAX_FRAME_BYTES`]. In
    /// entrambi i casi `dst` resta invariato: non viene scritto un frame a meta'.
    fn encode(&mut self, item: T, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let payload = postcard::to_stdvec(&item)?;
        if payload.len() > MAX_FRAME_BYTES {
            return Err(CodecError::FrameTooLarge {
                len: payload.len() as u64,
            });
        }

        // Il controllo qui sopra garantisce `payload.len() <= 1 MiB`: la conversione a
        // u32 non tronca.
        let len = payload.len() as u32;
        dst.reserve(LENGTH_PREFIX_BYTES + payload.len());
        dst.put_u32(len);
        dst.put_slice(&payload);
        Ok(())
    }
}

impl<T> Decoder for MessageCodec<T>
where
    T: DeserializeOwned,
{
    type Item = T;
    type Error = CodecError;

    /// Estrae da `src` il primo frame completo, se e' gia' arrivato per intero.
    ///
    /// Finche' il buffer non contiene l'header o l'intero payload restituisce `Ok(None)`
    /// senza consumare i byte gia' ricevuti, cosi' che la lettura successiva li ritrovi.
    ///
    /// # Errori
    ///
    /// [`CodecError::FrameTooLarge`] se l'header dichiara piu' di [`MAX_FRAME_BYTES`]
    /// byte e [`CodecError::Serialization`] se il payload non rappresenta un `T`.
    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < LENGTH_PREFIX_BYTES {
            return Ok(None);
        }

        let mut header = [0_u8; LENGTH_PREFIX_BYTES];
        header.copy_from_slice(&src[..LENGTH_PREFIX_BYTES]);
        let dichiarata = u64::from(u32::from_be_bytes(header));

        // Il confronto precede ogni `reserve`: un header ostile che dichiara 4 GiB costa
        // un errore e non la memoria che dichiara.
        if dichiarata > MAX_FRAME_BYTES as u64 {
            return Err(CodecError::FrameTooLarge { len: dichiarata });
        }

        let payload_len = dichiarata as usize;
        let frame_len = LENGTH_PREFIX_BYTES + payload_len;
        if src.len() < frame_len {
            // Payload legittimo ma incompleto: si prepara lo spazio per il resto e si
            // lascia intatto quanto arrivato finora.
            src.reserve(frame_len - src.len());
            return Ok(None);
        }

        src.advance(LENGTH_PREFIX_BYTES);
        let payload = src.split_to(payload_len);
        Ok(Some(postcard::from_bytes(&payload)?))
    }
}

/// Codec dei messaggi che il client invia al server.
pub type ClientCodec = MessageCodec<ClientMessage>;

/// Codec dei messaggi che il server invia al client.
pub type ServerCodec = MessageCodec<ServerMessage>;

#[cfg(test)]
mod tests {
    use super::*;

    use chrono::DateTime;

    use crate::protocol::{AckKind, PROTOCOL_VERSION};
    use crate::types::Coordinate;

    fn saluto() -> ClientMessage {
        ClientMessage::Hello {
            protocol_version: PROTOCOL_VERSION,
        }
    }

    fn posizione() -> ClientMessage {
        ClientMessage::Position {
            coordinate: Coordinate::new(45.0703, 7.6869).expect("coordinata di test valida"),
            recorded_at: DateTime::from_timestamp(1_700_000_000, 0)
                .expect("istante di test valido"),
        }
    }

    /// Compone a mano un frame: la lunghezza e' un parametro a se' per poter dichiarare
    /// un valore che non corrisponde al payload effettivo.
    fn frame_con(lunghezza: u32, payload: &[u8]) -> BytesMut {
        let mut buffer = BytesMut::new();
        buffer.put_u32(lunghezza);
        buffer.put_slice(payload);
        buffer
    }

    fn frame_di(messaggio: ClientMessage) -> BytesMut {
        let mut buffer = BytesMut::new();
        ClientCodec::new()
            .encode(messaggio, &mut buffer)
            .expect("encode riuscito");
        buffer
    }

    #[test]
    fn encode_e_decode_conservano_il_messaggio_nei_due_versi() {
        let mut buffer = frame_di(posizione());
        let decodificato = ClientCodec::new()
            .decode(&mut buffer)
            .expect("decode senza errori")
            .expect("il frame e' completo");
        assert_eq!(decodificato, posizione(), "roundtrip non fedele");
        assert!(buffer.is_empty(), "il frame consumato non lascia byte");

        let risposta = ServerMessage::Ack {
            request: AckKind::Position,
        };
        let mut codec = ServerCodec::new();
        let mut buffer = BytesMut::new();
        codec
            .encode(risposta.clone(), &mut buffer)
            .expect("encode riuscito");
        assert_eq!(
            codec
                .decode(&mut buffer)
                .expect("decode senza errori")
                .expect("il frame e' completo"),
            risposta
        );
    }

    #[test]
    fn un_frame_parziale_non_consuma_il_buffer() {
        // Prima meta' header, poi payload incompleto: in nessuno dei due casi i byte
        // gia' arrivati vanno consumati.
        for tagliato_a in [2, frame_di(posizione()).len() - 1] {
            let intero = frame_di(posizione());
            let mut buffer = BytesMut::from(&intero[..tagliato_a]);
            let arrivati = buffer.clone();
            let mut codec = ClientCodec::new();

            assert!(
                codec
                    .decode(&mut buffer)
                    .expect("decode senza errori")
                    .is_none(),
                "un frame incompleto non va decodificato"
            );
            assert_eq!(&buffer[..], &arrivati[..], "byte consumati troppo presto");

            buffer.put_slice(&intero[tagliato_a..]);
            assert_eq!(
                codec
                    .decode(&mut buffer)
                    .expect("decode senza errori")
                    .expect("con il resto dei byte il frame e' completo"),
                posizione()
            );
        }
    }

    #[test]
    fn due_frame_nello_stesso_buffer_escono_in_ordine() {
        let primo = saluto();
        let secondo = ClientMessage::TextToServer {
            body: "in arrivo al deposito".to_owned(),
        };
        let mut codec = ClientCodec::new();
        let mut buffer = BytesMut::new();

        codec
            .encode(primo.clone(), &mut buffer)
            .expect("encode del primo riuscito");
        codec
            .encode(secondo.clone(), &mut buffer)
            .expect("encode del secondo riuscito");

        for atteso in [primo, secondo] {
            assert_eq!(
                codec
                    .decode(&mut buffer)
                    .expect("decode senza errori")
                    .expect("il frame e' completo"),
                atteso
            );
        }
        assert!(
            codec
                .decode(&mut buffer)
                .expect("decode senza errori")
                .is_none(),
            "dopo due frame non resta nulla da decodificare"
        );
    }

    #[test]
    fn una_lunghezza_oltre_il_massimo_e_rifiutata_prima_di_riservare() {
        let mut buffer = frame_con(u32::MAX, &[]);
        let capacita_prima = buffer.capacity();

        let errore = ClientCodec::new()
            .decode(&mut buffer)
            .expect_err("un frame da 4 GiB va rifiutato");

        assert!(
            matches!(errore, CodecError::FrameTooLarge { len } if len == u64::from(u32::MAX)),
            "atteso FrameTooLarge, ottenuto {errore:?}"
        );
        assert!(
            buffer.capacity() <= capacita_prima,
            "capacita' passata da {capacita_prima} a {} byte",
            buffer.capacity()
        );
    }

    #[test]
    fn un_messaggio_troppo_grande_non_viene_codificato() {
        let enorme = ClientMessage::TextToServer {
            body: "a".repeat(MAX_FRAME_BYTES + 1),
        };
        let mut buffer = BytesMut::new();

        let errore = ClientCodec::new()
            .encode(enorme, &mut buffer)
            .expect_err("un payload oltre il massimo va rifiutato");

        assert!(
            matches!(errore, CodecError::FrameTooLarge { .. }),
            "atteso FrameTooLarge, ottenuto {errore:?}"
        );
        assert!(
            buffer.is_empty(),
            "un frame rifiutato non va scritto a meta'"
        );
    }

    #[test]
    fn un_payload_corrotto_produce_un_errore_di_serializzazione() {
        // Nessuna di queste sequenze e' un `ClientMessage`: un indice di variante
        // inesistente, un varint che non termina, una variante troncata prima dei campi.
        let corrotti: [&[u8]; 3] = [&[0xC8, 0x01], &[0xFF, 0xFF, 0xFF, 0xFF, 0xFF], &[0x01]];

        for corrotto in corrotti {
            let lunghezza = u32::try_from(corrotto.len()).expect("payload di test corto");
            let errore = ClientCodec::new()
                .decode(&mut frame_con(lunghezza, corrotto))
                .expect_err("i byte non compongono un messaggio");

            assert!(
                matches!(errore, CodecError::Serialization(_)),
                "atteso Serialization per {corrotto:?}, ottenuto {errore:?}"
            );
        }
    }

    #[test]
    fn gli_errori_si_mostrano_in_italiano() {
        assert_eq!(
            CodecError::FrameTooLarge { len: 2_000_000 }.to_string(),
            "frame di 2000000 byte: il massimo ammesso e' 1048576 byte"
        );
        assert_eq!(
            CodecError::Io(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "connessione chiusa"
            ))
            .to_string(),
            "errore di I/O sulla connessione: connessione chiusa"
        );
    }
}
