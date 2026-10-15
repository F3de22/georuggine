# Georuggine

Sistema client/server di geolocalizzazione per una flotta di veicoli: ogni veicolo invia la
propria posizione ogni 30 secondi, il server la salva, ne ricava lo stato di movimento e le
analisi dei percorsi e scambia messaggi con i veicoli. Progetto per l'esame di Programmazione
di Sistema.

## Architettura

Workspace Cargo con tre crate sotto `crates/` e i test end-to-end in `tests/`:

- `common`: tipi di dominio, messaggi e codec `Encoder`/`Decoder`;
- `server`: accetta le connessioni TCP, autentica i veicoli, salva i dati su SQLite;
- `client`: simula un veicolo, ne invia la posizione e lo mostra in una TUI.

Il client apre una connessione TCP verso il server e vi scambia messaggi serializzati con
postcard e framing a prefisso di lunghezza. Le dipendenze vanno da `common` verso i binari.

## Requisiti

- Rust stabile **1.88 o superiore** (edition 2024; la versione minima la impone
  `ratatui` 0.30), via [rustup](https://rustup.rs).
- Un compilatore C, richiesto da `rusqlite` con feature `bundled` che compila SQLite dai
  sorgenti: `build-essential` su Linux, Command Line Tools su macOS, Build Tools per Visual
  Studio con workload C++ su Windows.

## Build ed esecuzione

I comandi sono identici su Linux e Windows:

```bash
cargo build --workspace   # build di debug
cargo build --release     # ottimizzata: LTO fat, panic abort, binari strippati
cargo run -p georuggine-server
cargo run -p georuggine-client
```

Per passare argomenti al programma anziché a Cargo, separali con `--`.

## Sviluppo

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test --workspace
```

La CI esegue gli stessi comandi su `ubuntu-latest` e `windows-latest`. Invarianti di codice:

- niente `unwrap`, `expect` o `panic!` in produzione: errori propagati con `Result` e `?`;
- `thiserror` nelle librerie, `anyhow` solo nei `main`;
- nessuna operazione bloccante in un task async: SQLite e I/O su file passano da
  `tokio::task::spawn_blocking` (fa eccezione la lettura del CSV della sorgente, una sola
  volta all'avvio del client);
- nessun lock tenuto attraverso un `.await`: il guard va rilasciato prima della sospensione;
- la logica di dominio non chiama `Utc::now()` ma riceve il tempo dal trait `Clock`, così i
  test possono iniettare un orologio deterministico;
- nessun `unsafe` fuori dal modulo `cpu_time`, e ogni blocco `unsafe` porta un commento
  `// SAFETY:` che ne giustifica l'invariante;
- in edition 2024 ogni blocco `extern` si dichiara `unsafe extern "C"` e
  `unsafe_op_in_unsafe_fn` è un errore: ogni chiamata FFI va in un blocco `unsafe`
  esplicito, anche dentro funzioni già `unsafe`.

## Scelte tecniche

La serializzazione usa `postcard` e non `bincode`: bincode non è più mantenuto e ha
l'advisory RUSTSEC-2025-0141 per abbandono, il fork `bincode-next` è giovane e alza l'MSRV,
e JSON produrrebbe payload tre o quattro volte più grandi su un canale che trasporta
posizioni a ritmo costante. postcard si integra con serde senza feature aggiuntive e usa
codifica varint, quindi frame più compatti; in cambio i frame non sono ispezionabili con
strumenti testuali. `tokio` è dipendenza di `common` perché il codec implementa i trait
`Encoder`/`Decoder` di `tokio-util`: `common` è una libreria di protocollo, non di soli tipi.
