# Georuggine

Georuggine è un sistema client/server di geolocalizzazione per una flotta di veicoli, scritto
in Rust. Ogni client simula un veicolo e invia la propria posizione al server ogni 30 secondi;
il server la salva su SQLite, ne ricava lo stato di movimento e le analisi dei percorsi e
scambia messaggi con i veicoli. Progetto per l'esame di Programmazione di Sistema.

## Funzionalità

- **Stati dei veicoli**: Fermo, In movimento, Disconnesso. Un veicolo passa in movimento
  quando una posizione dista più di 5 m dalla precedente della stessa sessione e torna fermo
  dopo 180 s senza spostamenti; il server controlla gli stati ogni 10 s e notifica i cambi
  al client.
- **Quattro sorgenti di posizione**: percorso da file CSV, cammino casuale con seme, rotta
  a velocità costante con soste, posizione impostata a mano.
- **Analisi dei percorsi** su oggi, settimana corrente (da lunedì) o mese corrente, dalla
  mezzanotte locale del server fino all'istante della richiesta: distanza percorsa, tempo in
  movimento e in pausa, velocità media sul solo tempo in movimento, numero di soste (pause
  di almeno 180 s) e tragitto campionato su al massimo 1.000 punti.
- **Messaggi**: avvisi broadcast ai veicoli connessi e messaggi diretti a un veicolo;
  un diretto per un veicolo scollegato resta in attesa e viene consegnato al login
  successivo. I veicoli possono scrivere al server. Ogni messaggio ha al massimo 1.024
  caratteri.
- **Riconnessione automatica**: se la connessione cade il client riprova dopo 1, 2, 4 e
  8 s, poi ogni 30 s, con un ritardo casuale aggiuntivo fino al 20%. Nel frattempo accoda
  fino a 200 posizioni, scartando le più vecchie, e le invia al rientro. Il server accetta
  però al massimo 10 posizioni per connessione ogni 60 s: quelle oltre il limite vengono
  rifiutate e non sono reinviate, quindi dopo una disconnessione di più di circa cinque
  minuti una parte delle posizioni accodate va persa.
- **Log del tempo di CPU** del server, scritto su file ogni 120 s.
- **Interfaccia testuale (TUI)** del client con `ratatui`; console a riga di comando sul
  server.

## Architettura

```text
.
├── Cargo.toml                          workspace, dipendenze comuni, profilo release
├── crates/
│   ├── common/                         tipi di dominio, protocollo, codec, trait Clock
│   ├── server/                         rete, autenticazione, SQLite, stati, analisi,
│   │                                   messaggi, console, log del tempo di CPU
│   └── client/                         rete e riconnessione, sorgenti di posizione, TUI
│       └── assets/percorso_demo.csv    percorso di esempio per la sorgente file
├── tests/                              sei scenari end-to-end client/server su TCP
└── .github/workflows/ci.yml            integrazione continua
```

`server` e `client` dipendono solo da `common` e non l'uno dall'altro: comunicano
esclusivamente via TCP. Entrambi sono un binario più una libreria, così il crate `tests`
avvia server e client veri nello stesso processo. La sequenza del protocollo è
`Hello -> Welcome -> Register|Login -> SessionToken`, poi posizioni, testi, richieste di
analisi e `Ping`, infine `Logout`.

Il server salva tutto in un unico file SQLite con tre tabelle: `users` (username e hash
della password), `positions` (coordinate e istante di rilevazione) e `messages` (avvisi,
diretti e testi dei veicoli, con lo stato di consegna).

Scelte progettuali:

- **postcard con framing a lunghezza prefissata** (`u32` big endian, payload massimo 1 MiB):
  codifica binaria compatta integrata con serde; il limite impedisce a una connessione di
  far riservare memoria arbitraria.
- **Argon2id** in formato PHC (19 MiB, 2 passate, 1 corsia): parametri consigliati da OWASP;
  per un utente inesistente si verifica un hash fittizio, così la risposta non rivela quali
  utenti sono registrati.
- **SQLite incluso nel binario** (`rusqlite` con feature `bundled`): nessun servizio esterno
  da installare; le chiamate girano in `spawn_blocking` per non bloccare il runtime.
- **tokio, con un gestore che legge e un task che scrive per ogni connessione**: le scritture
  passano da una coda registrata per utente, così console e messaggistica raggiungono un
  veicolo senza toccare il socket, e togliere la voce dal registro chiude la connessione.
- **Orologio iniettabile** (trait `Clock`): la logica di dominio non legge l'ora di sistema,
  quindi i test fanno passare i 180 s del ritorno a fermo in un istante.
- **Tipi validati anche in deserializzazione**: `Coordinate` e `Username` arrivati dalla rete
  sono sempre in intervallo e ben formati.

## Requisiti

- **Rust 1.88 o superiore** (`rust-version` del workspace, imposta da `ratatui` 0.30),
  installato con [rustup](https://rustup.rs); per aggiornarlo: `rustup update stable`.
- **Un compilatore C**, perché `rusqlite` compila SQLite dai sorgenti:
  - macOS: `xcode-select --install` (Command Line Tools);
  - Linux: `sudo apt install build-essential` su Debian/Ubuntu, `gcc` sulle altre
    distribuzioni;
  - Windows: Visual Studio Build Tools con il carico di lavoro "Sviluppo di applicazioni
    desktop con C++", già richiesto da rustup per la toolchain MSVC.

Piattaforme: Linux, Windows e macOS. La CI compila e collauda su Linux e Windows
(`ubuntu-latest`, `windows-latest`) con la toolchain stabile più recente; macOS non è
nella CI ed è stato provato solo a mano.

## Compilazione

```bash
cargo build --release --workspace
```

Gli eseguibili sono `target/release/georuggine-server` e `target/release/georuggine-client`
(su Windows `target\release\georuggine-server.exe` e `target\release\georuggine-client.exe`).
Il profilo release usa LTO completa, una sola unità di codegen, `panic = "abort"` e simboli
rimossi. Dimensioni misurate dal gruppo su macOS: server 2.698.656 byte (2,57 MiB), client
919.424 byte (0,88 MiB).

## Avvio del server

```bash
./target/release/georuggine-server
```

| Opzione | Significato | Predefinito |
| --- | --- | --- |
| `--bind <INDIRIZZO>` | indirizzo IP e porta di ascolto | `127.0.0.1:7878` |
| `--db <PERCORSO>` | file SQLite della persistenza | `georuggine.db` |
| `--log-dir <PERCORSO>` | cartella dei log del tempo di CPU | `logs` |
| `--help` | mostra l'uso ed esce | |

Per accettare client da altre macchine: `--bind 0.0.0.0:7878`. Il server scrive il proprio
log sul terminale e legge i comandi della console dallo standard input:

| Comando | Effetto |
| --- | --- |
| `/utenti` | elenco dei veicoli con stato, ultimo contatto e ultima posizione |
| `/broadcast <testo>` | avviso a tutti i veicoli connessi |
| `/msg <username> <testo>` | messaggio diretto a un veicolo, anche scollegato |
| `/analisi <username> <periodo>` | riepilogo dei movimenti; periodo `today`, `week` o `month` (oppure `oggi`, `settimana`, `mese`) |
| `/aiuto` | elenco dei comandi |
| `/esci` | spegnimento ordinato |

`/esci` e Ctrl+C avviano lo stesso spegnimento: il server smette di accettare connessioni,
chiude quelle aperte e scrive la riga finale del log del tempo di CPU, attendendola al
massimo 5 s. I testi inviati dai veicoli compaiono nel log del server.

## Avvio del client

| Opzione generale | Significato |
| --- | --- |
| `--server <INDIRIZZO>` | indirizzo IP e porta del server (obbligatoria) |
| `--user <USERNAME>` | nome utente (obbligatoria) |
| `--password <PASSWORD>` | password (obbligatoria) |
| `--register` | registra un nuovo utente invece di accedere |
| `--source <SORGENTE>` | sorgente di posizione: `file`, `random`, `route` o `manual` (obbligatoria) |
| `--help` | mostra l'uso ed esce |

| Sorgente | Obbligatorie | Facoltative |
| --- | --- | --- |
| `file`: percorso registrato in CSV | `--file-path <PERCORSO>` | `--file-loop` (ripete il percorso da capo) |
| `random`: cammino casuale | `--start <LAT>,<LON>`, `--radius <METRI>`, `--step <METRI>`, `--seed <NUMERO>` | |
| `route`: rotta a velocità costante | `--start <LAT>,<LON>`, `--end <LAT>,<LON>`, `--speed <KM/H>` | `--pause <FRAZ>@<SEC>`, ripetibile (sosta di SEC secondi alla frazione FRAZ del tragitto, da 0 a 1) |
| `manual`: posizione decisa a mano | `--start <LAT>,<LON>` | |

Il CSV della sorgente `file` ha intestazione `lat,lon,offset_secs`, con l'offset in secondi
dall'inizio del percorso; le righe malformate vengono scartate.

Regole di accesso: lo username ha da 3 a 32 caratteri, solo lettere e cifre ASCII o
underscore; la password, controllata alla registrazione, ha almeno 8 caratteri con almeno
una lettera e una cifra.

**Ogni utente si registra con `--register` al primo avvio e solo allora**: dagli avvii
successivi si toglie l'opzione, altrimenti il server risponde che lo username è già in uso
e il client si chiude. Le riconnessioni automatiche usano sempre il login.

Nota: gli utenti restano nel database anche dopo lo spegnimento del server, e non esiste un
comando per cancellarli. Per ripetere i comandi con `--register` si ferma il server e si
eliminano, dalla cartella da cui è stato avviato, il file del database (`georuggine.db`, o
quello indicato con `--db`) e gli eventuali file `georuggine.db-wal` e `georuggine.db-shm`,
che SQLite crea accanto al database mentre il server è attivo. Al riavvio il server crea un
database vuoto. In bash:

```bash
rm -f georuggine.db georuggine.db-wal georuggine.db-shm
```

In PowerShell:

```powershell
Remove-Item georuggine.db, georuggine.db-wal, georuggine.db-shm -ErrorAction SilentlyContinue
```

In alternativa si può avviare il server su un file nuovo, per esempio `--db demo2.db`.

Esempi per i primi avvii su un database nuovo, dalla radice del repository con il server
sull'indirizzo predefinito (le virgolette servono a PowerShell e non cambiano nulla in bash).
Ogni client apre una TUI a schermo intero che occupa il terminale: va lanciato un comando
per terminale, separato anche da quello del server.

Furgone, sorgente `file`:

```bash
./target/release/georuggine-client --server 127.0.0.1:7878 --user furgone --password segreta123 --register --source file --file-path crates/client/assets/percorso_demo.csv --file-loop
```

Camion, sorgente `route`:

```bash
./target/release/georuggine-client --server 127.0.0.1:7878 --user camion --password segreta123 --register --source route --start "45.0703,7.6869" --end "45.4642,9.1900" --speed 70 --pause "0.5@120"
```

Auto, sorgente `manual`:

```bash
./target/release/georuggine-client --server 127.0.0.1:7878 --user auto --password segreta123 --register --source manual --start "45.0712,7.6801"
```

Rover, sorgente `random`:

```bash
./target/release/georuggine-client --server 127.0.0.1:7878 --user rover --password segreta123 --register --source random --start "45.0703,7.6869" --radius 800 --step 25 --seed 42
```

Gli stessi veicoli negli avvii successivi, già registrati (senza `--register`):

Furgone, sorgente `file`:

```bash
./target/release/georuggine-client --server 127.0.0.1:7878 --user furgone --password segreta123 --source file --file-path crates/client/assets/percorso_demo.csv --file-loop
```

Camion, sorgente `route`:

```bash
./target/release/georuggine-client --server 127.0.0.1:7878 --user camion --password segreta123 --source route --start "45.0703,7.6869" --end "45.4642,9.1900" --speed 70 --pause "0.5@120"
```

Auto, sorgente `manual`:

```bash
./target/release/georuggine-client --server 127.0.0.1:7878 --user auto --password segreta123 --source manual --start "45.0712,7.6801"
```

Rover, sorgente `random`:

```bash
./target/release/georuggine-client --server 127.0.0.1:7878 --user rover --password segreta123 --source random --start "45.0703,7.6869" --radius 800 --step 25 --seed 42
```

## Interfaccia del client

La schermata è divisa in quattro riquadri, con la riga di input in fondo:

| Riquadro | Contenuto |
| --- | --- |
| Stato (in alto a sinistra) | connessione (simbolo, testo e colore), utente, stato di movimento, sorgente, ultima posizione inviata, ultimo errore |
| Ultime posizioni (in alto a destra) | le ultime 20 posizioni inviate, la più recente in cima, con la distanza dalla precedente |
| Messaggi (in basso a sinistra) | messaggi ricevuti con orario e tipo (broadcast o diretto) |
| Ultima analisi (in basso a destra) | l'ultimo riepilogo ricevuto: distanza, velocità media, tempo in moto e in pausa, soste, punti del tragitto, partenza e arrivo |

| Comando | Effetto |
| --- | --- |
| `/msg <testo>` | invia un messaggio al server |
| `/pos <lat> <lon>` | imposta la posizione (solo con la sorgente `manual`) |
| `/analisi <periodo>` | chiede il riepilogo dei propri movimenti; periodo `today`, `week` o `month` |
| `/quit` | chiude il client |

`/quit` e Ctrl+C hanno lo stesso effetto: il client ripristina il terminale e, se è
connesso, invia `Logout` e attende al massimo 1 s che il server chiuda la connessione.
Da scollegato, `/msg` e `/analisi` vengono scartati con un avviso. Le righe che non
iniziano con `/` sono ignorate.

## Log del tempo di CPU

Il server scrive `cpu.log` nella cartella indicata da `--log-dir` (predefinita `logs`,
relativa alla cartella di lavoro), creandola se manca. Scrive una riga all'avvio, una ogni
120 s e una allo spegnimento; se il file è nuovo aggiunge prima l'intestazione, altrimenti
accoda. Le colonne sono separate da tabulazione:

| Colonna | Significato |
| --- | --- |
| `timestamp` | istante della riga, UTC, RFC 3339 al secondo |
| `cpu_utente_ms` | tempo di CPU in modalità utente dall'avvio del processo |
| `cpu_sistema_ms` | tempo di CPU in modalità kernel dall'avvio del processo |
| `cpu_totale_ms` | somma dei due |
| `delta_ms` | tempo di CPU consumato dalla riga precedente (0 sulla prima) |
| `percentuale_cpu` | `delta_ms` sul tempo reale trascorso, in percento con due decimali; può superare 100 su più core |
| `utenti_connessi` | veicoli connessi in quell'istante |

Il tempo di CPU si legge con `getrusage` su Unix e `GetProcessTimes` su Windows.

## Verifica e sviluppo

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test --workspace
```

La CI (`.github/workflows/ci.yml`) esegue questi tre comandi e `cargo build --release` a ogni
push su `main` e a ogni pull request, su `ubuntu-latest` e `windows-latest`, con
`RUSTFLAGS=-D warnings`. I test unitari stanno nei moduli; i sei scenari end-to-end in
`tests/` coprono accesso e analisi, messaggistica, stati, riconnessione, sicurezza e ciclo di
vita della sessione, con un orologio finto condiviso fra client e server.

Convenzioni rispettate nel codice:

- righe di al massimo 100 colonne (`rustfmt.toml`);
- niente `unwrap`, `expect` o `panic!` nel codice di produzione: errori propagati con
  `Result` e `?`;
- `thiserror` nelle librerie, `anyhow` solo nel `main` del server;
- SQLite e scrittura del log su file passano da `tokio::task::spawn_blocking`; fanno
  eccezione l'apertura del database e la lettura del CSV, una sola volta all'avvio;
- nessun lock tenuto attraverso un `.await`;
- la logica di dominio legge l'ora dal trait `Clock`, mai da `Utc::now()`;
- `unsafe` solo in `crates/server/src/cpu_time.rs`, ogni blocco con un commento `SAFETY:`.
