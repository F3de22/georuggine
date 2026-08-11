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
//! dall'architettura dei due peer. Il payload e' il messaggio serializzato con postcard,
//! come descritto in [`crate::protocol`].
//!
//! # Limite di dimensione
//!
//! Un peer ostile puo' dichiarare nell'header una lunghezza arbitraria senza inviare un
//! solo byte di payload. Per questo [`MessageCodec::decode`] confronta la lunghezza
//! dichiarata con [`MAX_FRAME_BYTES`] **prima** di riservare spazio nel buffer: un header
//! da 4 GiB costa un errore, non 4 GiB di memoria.
//!
//! [`CodecError::FrameTooLarge`] e [`CodecError::Serialization`] non sono recuperabili
//! sulla stessa connessione: il flusso resta disallineato rispetto ai confini dei frame,
//! quindi il chiamante deve chiudere la connessione anziche' continuare a leggere.

use std::fmt;
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
/// Un mebibyte sta diversi ordini di grandezza sopra al messaggio piu' grande che il
/// protocollo prevede (una `AnalysisReport` con una rotta lunga) e tiene comunque limitata
/// la memoria che una singola connessione puo' far riservare.
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
/// Implementa [`Encoder`] e [`Decoder`] di `tokio-util`, quindi si usa con
/// `Framed::new(stream, MessageCodec::new())` per ottenere uno `Stream`/`Sink` di
/// messaggi al posto di un flusso di byte. Il codec e' privo di stato: la
/// riassemblazione dei frame parziali vive nel buffer che `Framed` gli passa.
///
/// Ogni estremo della connessione usa il codec sul proprio verso: il client legge
/// [`ServerMessage`] e scrive [`ClientMessage`], il server fa l'opposto. Gli alias
/// [`ClientCodec`] e [`ServerCodec`] nominano i due tipi.
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

impl<T> Clone for MessageCodec<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T> Copy for MessageCodec<T> {}

impl<T> fmt::Debug for MessageCodec<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("MessageCodec")
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
    /// Restituisce [`CodecError::Serialization`] se il messaggio non e' serializzabile e
    /// [`CodecError::FrameTooLarge`] se il payload supera [`MAX_FRAME_BYTES`]. In
    /// entrambi i casi `dst` resta invariato: non viene scritto un frame a meta'.
    fn encode(&mut self, item: T, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let payload = postcard::to_stdvec(&item)?;
        if payload.len() > MAX_FRAME_BYTES {
            return Err(CodecError::FrameTooLarge {
                len: payload.len() as u64,
            });
        }

        // Il controllo qui sopra garantisce `payload.len() <= 1 MiB`, quindi la
        // conversione a u32 non tronca.
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
    /// Restituisce [`CodecError::FrameTooLarge`] se l'header dichiara piu' di
    /// [`MAX_FRAME_BYTES`] byte e [`CodecError::Serialization`] se il payload non
    /// rappresenta un valore di tipo `T`.
    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < LENGTH_PREFIX_BYTES {
            return Ok(None);
        }

        let mut header = [0_u8; LENGTH_PREFIX_BYTES];
        // Il controllo qui sopra garantisce che i byte dell'header ci siano tutti, quindi
        // le due fette hanno la stessa lunghezza e la copia non puo' fallire.
        header.copy_from_slice(&src[..LENGTH_PREFIX_BYTES]);
        let dichiarata = u64::from(u32::from_be_bytes(header));

        // Il confronto precede ogni `reserve`: cosi' un header ostile costa un errore e
        // non la memoria che dichiara.
        if dichiarata > MAX_FRAME_BYTES as u64 {
            return Err(CodecError::FrameTooLarge { len: dichiarata });
        }

        // `dichiarata <= 1 MiB` entra in una usize su ogni piattaforma supportata.
        let payload_len = dichiarata as usize;
        let frame_len = LENGTH_PREFIX_BYTES + payload_len;
        if src.len() < frame_len {
            // Il payload e' legittimo ma incompleto: si prepara lo spazio per il resto e
            // si lascia intatto quanto arrivato finora.
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
    use tokio_util::bytes::{BufMut, BytesMut};
    use tokio_util::codec::{Decoder, Encoder};

    use crate::protocol::{AckKind, ClientMessage, PROTOCOL_VERSION, ServerMessage};
    use crate::types::Coordinate;

    /// Messaggio di apertura, il piu' corto del protocollo.
    fn saluto() -> ClientMessage {
        ClientMessage::Hello {
            protocol_version: PROTOCOL_VERSION,
        }
    }

    /// Messaggio con campi annidati, per un roundtrip meno banale.
    fn posizione() -> ClientMessage {
        ClientMessage::Position {
            coordinate: Coordinate::new(45.0703, 7.6869).expect("coordinata di test valida"),
            recorded_at: DateTime::from_timestamp(1_700_000_000, 0)
                .expect("istante di test valido"),
        }
    }

    /// Compone a mano un frame con la lunghezza dichiarata e il payload dati.
    ///
    /// La lunghezza e' un parametro a se' proprio per poter dichiarare un valore che non
    /// corrisponde al payload effettivo.
    fn frame_con(lunghezza: u32, payload: &[u8]) -> BytesMut {
        let mut buffer = BytesMut::new();
        buffer.put_u32(lunghezza);
        buffer.put_slice(payload);
        buffer
    }

    /// Serializza il messaggio in un buffer nuovo.
    fn frame_di(messaggio: ClientMessage) -> BytesMut {
        let mut buffer = BytesMut::new();
        ClientCodec::new()
            .encode(messaggio, &mut buffer)
            .expect("encode riuscito");
        buffer
    }

    #[test]
    fn encode_e_decode_conservano_il_messaggio() {
        let originale = posizione();
        let mut codec = ClientCodec::new();
        let mut buffer = BytesMut::new();

        codec
            .encode(originale.clone(), &mut buffer)
            .expect("encode riuscito");
        let decodificato = codec
            .decode(&mut buffer)
            .expect("decode senza errori")
            .expect("il frame e' completo");

        assert_eq!(decodificato, originale, "roundtrip non fedele");
        assert!(buffer.is_empty(), "il frame consumato non lascia byte");
    }

    #[test]
    fn il_codec_del_server_fa_lo_stesso_roundtrip() {
        let originale = ServerMessage::Ack {
            request: AckKind::Position,
        };
        let mut codec = ServerCodec::new();
        let mut buffer = BytesMut::new();

        codec
            .encode(originale.clone(), &mut buffer)
            .expect("encode riuscito");
        let decodificato = codec
            .decode(&mut buffer)
            .expect("decode senza errori")
            .expect("il frame e' completo");

        assert_eq!(decodificato, originale, "roundtrip non fedele");
    }

    #[test]
    fn header_a_meta_non_consuma_il_buffer() {
        let intero = frame_di(saluto());
        let mut buffer = BytesMut::from(&intero[..2]);
        let arrivati = buffer.clone();
        let mut codec = ClientCodec::new();

        assert!(
            codec
                .decode(&mut buffer)
                .expect("decode senza errori")
                .is_none(),
            "meta' header non basta a comporre un frame"
        );
        assert_eq!(
            &buffer[..],
            &arrivati[..],
            "i byte gia' arrivati non vanno consumati"
        );

        buffer.put_slice(&intero[2..]);
        let decodificato = codec
            .decode(&mut buffer)
            .expect("decode senza errori")
            .expect("con il resto dell'header e del payload il frame e' completo");
        assert_eq!(decodificato, saluto());
    }

    #[test]
    fn payload_parziale_non_consuma_il_buffer() {
        let intero = frame_di(posizione());
        let mut buffer = BytesMut::from(&intero[..intero.len() - 1]);
        let arrivati = buffer.clone();
        let mut codec = ClientCodec::new();

        assert!(
            codec
                .decode(&mut buffer)
                .expect("decode senza errori")
                .is_none(),
            "un payload incompleto non compone un frame"
        );
        assert_eq!(
            &buffer[..],
            &arrivati[..],
            "i byte gia' arrivati non vanno consumati"
        );

        buffer.put_slice(&intero[intero.len() - 1..]);
        let decodificato = codec
            .decode(&mut buffer)
            .expect("decode senza errori")
            .expect("con l'ultimo byte il frame e' completo");
        assert_eq!(decodificato, posizione());
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

        assert_eq!(
            codec
                .decode(&mut buffer)
                .expect("decode senza errori")
                .expect("il primo frame e' completo"),
            primo
        );
        assert_eq!(
            codec
                .decode(&mut buffer)
                .expect("decode senza errori")
                .expect("il secondo frame e' completo"),
            secondo
        );
        assert!(
            codec
                .decode(&mut buffer)
                .expect("decode senza errori")
                .is_none(),
            "dopo due frame non resta nulla da decodificare"
        );
        assert!(buffer.is_empty(), "entrambi i frame sono stati consumati");
    }

    #[test]
    fn lunghezza_oltre_il_massimo_e_rifiutata() {
        let dichiarata = u32::try_from(MAX_FRAME_BYTES).expect("il massimo sta in un u32") + 1;
        let mut buffer = frame_con(dichiarata, &[]);

        let errore = ClientCodec::new()
            .decode(&mut buffer)
            .expect_err("un frame oltre il massimo va rifiutato");

        assert!(
            matches!(errore, CodecError::FrameTooLarge { len } if len == u64::from(dichiarata)),
            "atteso FrameTooLarge, ottenuto {errore:?}"
        );
    }

    #[test]
    fn lunghezza_enorme_non_fa_riservare_memoria() {
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
            "la lunghezza va controllata prima di riservare: capacita' passata da \
             {capacita_prima} a {} byte",
            buffer.capacity()
        );
    }

    #[test]
    fn messaggio_troppo_grande_non_viene_codificato() {
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
    fn payload_corrotto_produce_un_errore_di_serializzazione() {
        // Nessuna di queste sequenze e' un `ClientMessage`: un indice di variante
        // inesistente, un varint che non termina, una variante troncata prima dei campi.
        let corrotti: [&[u8]; 3] = [&[0xC8, 0x01], &[0xFF, 0xFF, 0xFF, 0xFF, 0xFF], &[0x01]];

        for corrotto in corrotti {
            let lunghezza = u32::try_from(corrotto.len()).expect("payload di test corto");
            let mut buffer = frame_con(lunghezza, corrotto);

            let errore = ClientCodec::new()
                .decode(&mut buffer)
                .expect_err("i byte non compongono un messaggio");

            assert!(
                matches!(errore, CodecError::Serialization(_)),
                "atteso Serialization per {corrotto:?}, ottenuto {errore:?}"
            );
        }
    }

    #[test]
    fn gli_errori_si_mostrano_in_italiano() {
        let troppo_grande = CodecError::FrameTooLarge { len: 2_000_000 };
        assert_eq!(
            troppo_grande.to_string(),
            "frame di 2000000 byte: il massimo ammesso e' 1048576 byte"
        );

        let io = CodecError::Io(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "connessione chiusa",
        ));
        assert_eq!(
            io.to_string(),
            "errore di I/O sulla connessione: connessione chiusa"
        );

        let serializzazione = ClientCodec::new()
            .decode(&mut frame_con(1, &[0xC8]))
            .expect_err("i byte non compongono un messaggio");
        assert!(
            serializzazione
                .to_string()
                .starts_with("payload non conforme al protocollo: "),
            "messaggio inatteso: {serializzazione}"
        );
    }
}
