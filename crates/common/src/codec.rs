//! Framing dei messaggi su TCP con prefisso di lunghezza:
//!
//! ```text
//! +--------------------+---------------------------+
//! | lunghezza: u32 BE  | payload postcard (N byte)  |
//! +--------------------+---------------------------+
//! ```
//!
//! Dopo [`CodecError::FrameTooLarge`] o [`CodecError::Serialization`] il flusso non e'
//! piu' allineato ai confini dei frame: il chiamante deve chiudere la connessione.

use std::io;
use std::marker::PhantomData;

use serde::Serialize;
use serde::de::DeserializeOwned;
use thiserror::Error;
use tokio_util::bytes::{Buf, BufMut, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

use crate::protocol::{ClientMessage, ServerMessage};

/// Dimensione massima del payload di un frame, in byte: limita la memoria che una
/// connessione puo' far riservare.
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

/// Codec a prefisso di lunghezza per messaggi `T` serializzati con postcard.
///
/// Il client legge [`ServerMessage`] e scrive [`ClientMessage`], il server il contrario.
pub struct MessageCodec<T> {
    /// `fn(T) -> T` rende il codec `Send` e `Sync` qualunque sia `T`.
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

    /// Serializza il messaggio preceduto dalla sua lunghezza.
    ///
    /// # Errori
    ///
    /// [`CodecError::Serialization`] o [`CodecError::FrameTooLarge`]; in entrambi i casi
    /// `dst` resta invariato.
    fn encode(&mut self, item: T, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let payload = postcard::to_stdvec(&item)?;
        if payload.len() > MAX_FRAME_BYTES {
            return Err(CodecError::FrameTooLarge {
                len: payload.len() as u64,
            });
        }

        // Il controllo sopra garantisce che la lunghezza stia in un u32.
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

    /// Estrae il primo frame completo; `Ok(None)` se non e' ancora arrivato per intero.
    ///
    /// # Errori
    ///
    /// [`CodecError::FrameTooLarge`] se l'header dichiara troppi byte,
    /// [`CodecError::Serialization`] se il payload non e' un `T`.
    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < LENGTH_PREFIX_BYTES {
            return Ok(None);
        }

        let mut header = [0_u8; LENGTH_PREFIX_BYTES];
        header.copy_from_slice(&src[..LENGTH_PREFIX_BYTES]);
        let dichiarata = u64::from(u32::from_be_bytes(header));

        // Il controllo precede ogni `reserve`: un header ostile non fa allocare memoria.
        if dichiarata > MAX_FRAME_BYTES as u64 {
            return Err(CodecError::FrameTooLarge { len: dichiarata });
        }

        let payload_len = dichiarata as usize;
        let frame_len = LENGTH_PREFIX_BYTES + payload_len;
        if src.len() < frame_len {
            // Payload incompleto: si riserva lo spazio e si attende il resto.
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

    /// Compone a mano un frame, con una lunghezza dichiarata anche diversa dal payload.
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
        // Header o payload incompleti: i byte gia' arrivati non vanno consumati.
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
