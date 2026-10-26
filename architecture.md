# Architecture

## Overview

findethedox has three conceptual layers:

1. **Source databases** — one or more `allmydox.db` SQLite files that contain
   the indexed document vocabulary and co-occurrence tables.
2. **Caches** — one pre-computed `<name>.cache.db` per source database,
   stored in the data folder. Searches query every cache and sum the scores,
   so each search takes a few ms.
3. **UI** — a PyQt6 main window with three interactive word clouds, a document
   panel, a sentence panel, and a built-in document viewer.

```mermaid
flowchart LR
    subgraph src["Source databases (one or more)"]
        d1[("allmydox_1.db")]
        d2[("allmydox_2.db")]
        dN[("allmydox_N.db")]
    end

    subgraph build["cache.py — built once, incremental updates per source"]
        c[("<name>.cache.db (one per source DB)\n───────────────\ncooccurrence\nword_freq\nmeta")]
    end

    subgraph ftd["findethedox"]
        wc["Word clouds\nNames · Nouns · Verbs"]
        dl["Document list"]
        sl["Sentence panel"]
    end

    f[("Document files\n.pdf · .docx · .txt")]

    src -- "ATTACH / aggregate\n(23 SQL steps per source)" --> build
    build -- "co-occurrence scores\nglobal word frequencies" --> wc
    src -- "document occurrences" --> dl
    dl --> sl
    f -- "sentence extraction" --> sl
```

---

## Modules

| File | Responsibility |
|---|---|
| `main.py` | Entry point: CLI arguments, config loading, creates `MainWindow` |
| `app.py` | `MainWindow`, `SetupDialog`, four `QThread` worker classes |
| `cloud_widget.py` | Interactive word cloud widget (wordcloud layout + matplotlib hit-testing) |
| `query.py` | Queries against source databases: frequencies, co-occurrences, document occurrences, index creation |
| `cache.py` | Pre-computed cache: `build`, `update`, `needs_update`, read accessors |
| `config.py` | Persistent settings in `~/.config/findethedox/config.json` |
| `doc_viewer.py` | Built-in viewer for PDF (pymupdf + highlight), DOCX, TXT; sentence extraction |

---

## Source databases

Each source database is an SQLite file produced by allmydox. The tables
relevant to findethedox:

| Table | Content |
|---|---|
| `documents` | One row per indexed document; `fileID` is the primary key |
| `nouns` / `noun_occurrences` | Vocabulary and per-document noun occurrence positions |
| `names` / `name_occurrences` | Same for proper names |
| `verbs` / `verb_occurrences` | Same for verbs |
| `noun_sentence` | Pairs of nouns/names that appear in the same sentence |
| `noun_paragraph` | Same within the same paragraph |
| `noun_verb_sentence` | Noun/name–verb pairs within the same sentence |

On first launch, `query.ensure_indexes()` adds 9 indexes to these tables.
Without them, co-occurrence queries would require full table scans (30+ s on
a 5 GB database).

---

## Cache

### Why a cache?

A single search requires aggregating co-occurrence counts across tens of
millions of rows in `noun_sentence` and `noun_paragraph`. Even with source-DB
indexes, each query takes 90–120 s on a 5 GB database. The cache pre-computes
all co-occurrence scores once, reducing every search to a single indexed
SELECT.

### Tables

| Table | Schema | Purpose |
|---|---|---|
| `cooccurrence` | `(src_word, src_kind, tgt_word, tgt_kind, score)` | All pre-aggregated scores |
| `word_freq` | `(word, kind, freq)` UNIQUE on `(word, kind)` | Total occurrence counts for the global cloud |
| `meta` | `(key, value)` | Per-source watermarks: `src:/path/to/db → last_fileID` |

Indexes on the cache:
- `idx_cooc` on `lower(src_word)` — used by every word search
- `idx_wf_uk` UNIQUE on `(word, kind)` — enforces the upsert constraint
- `idx_wf` on `(kind, freq DESC)` — used by the global cloud query

### Build process (`cache.build`)

Each source database is processed in sequence using SQLite's `ATTACH DATABASE`
so no data leaves SQLite. 23 `INSERT … SELECT` steps run per source:

| Count | Source table | Weight | What is captured |
|---|---|---|---|
| 8 | `noun_sentence` | ×1.3 | noun↔noun, noun↔name, name↔noun, name↔name — forward and reverse |
| 8 | `noun_paragraph` | ×1.0 | same four combinations — forward and reverse |
| 4 | `noun_verb_sentence` | ×1.3 | noun↔verb, name↔verb — both directions |
| 3 | occurrence tables | — | word frequency counts (noun, name, verb) |

After all sources are processed, staging tables (`raw`, `freq_raw`) are
consolidated into the permanent tables with a `GROUP BY … SUM(score)` pass,
indexes are created, and staging tables are dropped. Watermarks for each source
are written to `meta`.

### Incremental updates (`cache.update`)

For each source, the update reads its watermark from `meta`:

```
last = meta["src:/path/to/db"]  →  0 if not present (new source)
cur  = MAX(fileID) FROM documents in that source
if cur > last:
    run 23 steps with WHERE fileID > last   →  raw_delta / freq_delta
    merge delta into cooccurrence (INSERT)
    merge delta into word_freq   (INSERT … ON CONFLICT DO UPDATE SET freq = freq + excluded.freq)
    write new watermark to meta
```

### Cache file location

Each source database `<name>.db` has its own cache `<name>.cache.db`
(`cache.cache_path_for()`). It lives in the **data folder** chosen in the
Settings dialog; without a data folder it is placed next to the database.
`cache.find_databases()` lists every allmydox DB in the data folder (any
`*.db` with a `documents` table, excluding `*.cache.db`).

### One cache per source

Caches are independent: adding, removing or rebuilding one database never
touches the others. The `meta` table stores `max_file_id` (the watermark),
`src:<path>`, `docs_folder` and `docs_drive`. The watermark lookup falls back to a single
`src:*` row, so a cache still works after the data folder has been moved.
`cache.cooccurrences()` and `cache.global_frequencies()` take a list of
connections and sum the scores per `(word, kind)`; each cache contributes its
own top 200 / top 450 rows before merging.

---

## Worker threads

All slow operations run in `QThread` subclasses. Each worker opens its own
SQLite connections (connections cannot be shared across threads).

| Worker | When started | Signals emitted |
|---|---|---|
| `_IndexWorker` | App startup | `done` |
| `_CacheWorker` | First launch / Rebuild button / Setup dialog | `progress(label, cur, total)`, `done`, `error(msg)` |
| `_SearchWorker` | Search bar Enter / cloud click | `cooc_ready(rows, word)`, `docs_ready(occs, word)`, `not_found(word)`, `error(msg)` |
| `_SentenceWorker` | Single-click on document | `ready(sentences)`, `error(msg)` |

---

## Startup sequence

```
main.py
  └─ load config, resolve data_folder → db_paths (scan + external − disabled)
  └─ MainWindow(db_paths, data_folder)

MainWindow.__init__
  └─ _IndexWorker  ──────────────────────── ensure 9 source-DB indexes exist
        └─ done → _on_indexes_ready()
              ├─ all caches exist? → connect each, load_global() → show clouds
              └─ some missing     → _CacheWorker([(db, cache, "build"), …])
                                      └─ done → connect each, load_global() → show clouds

Toolbar "Rebuild Cache"
  └─ _CacheWorker([(db, cache, "build") for every active db])  ── full rebuild
```

---

## Query flow

### Global cloud (startup / empty search box)
```sql
SELECT word, kind, freq FROM word_freq
ORDER BY freq DESC LIMIT 450
```
Result is split by `kind` and handed to the three `CloudWidget` instances.

### Word search
```
_SearchWorker:
  1. SELECT tgt_word, tgt_kind, SUM(score) AS score
     FROM cooccurrence
     WHERE lower(src_word) = ?
     GROUP BY tgt_word, tgt_kind
     ORDER BY score DESC LIMIT 200
     → cooc_ready → update clouds

  2. for each source db:
       query.document_occurrences(conn, word)
     merge + deduplicate across sources
     → docs_ready → populate document list
```

### Two-word search
```
_SearchWorker (search text has two words):
  1. cache.pair_cooccurrences(conns, w1, w2)
     words related to both (scores added), else to either
     → cooc_ready → update clouds

  2. for each source db: query.pair_occurrences(conn, w1, w2)
       noun_sentence  + noun_verb_sentence  → sentence pairs
       noun_paragraph − sentence pairs      → paragraph pairs
     grouped per document, sorted by sentence hits
     → docs_ready → document list with hit counts
```
The pair tables store `(occ1, occ2)` in text order, so both directions are
queried. Each query starts from w1's occurrences (materialized CTE), follows
the `occ1`/`occ2` index into the pair table, and checks w2's occurrences via
an automatic index (`CROSS JOIN` fixes this order). On a database with
5.2 M sentence pairs a pair takes 0.1–0.4 s.

### Codes (allmydox v1.8+)
Source DBs may contain `codes`, `code_occurrences`, `code_sentence` and
`code_paragraph` (`query.has_codes()`). Then:
- the cache gets `code` rows in `word_freq` and `cooccurrence`
  (`cache._code_steps()`, same weights as nouns) and meta `codes = 1`;
  `cache.status()` returns `rebuild` for a DB with codes whose cache lacks it
- `document_occurrences()` also searches `code_occurrences`
- `pair_occurrences()` adds `code_sentence` / `code_paragraph` pairs
  (`_code_pairs()`, both join directions because the code can be either word)
- clouds and the global view are limited per kind (80 / 150 words), so
  frequent codes do not crowd out names and nouns

`doc_viewer.contexts_containing()` produces the sentence panel for a pair:
text is split into paragraphs at blank lines (as allmydox does), then into
sentences.

### Sentence extraction
```
_SentenceWorker:
  doc_viewer.sentences_containing(filepath, word)
  → extract full text (PDF page-by-page, DOCX XML, TXT)
  → split into sentences
  → filter to sentences containing the word
  → ready → populate sentence list
```

---

## Configuration file

`~/.config/findethedox/config.json`:

```json
{
  "data_folder":  "/path/to/datafolder",
  "external_dbs": ["/elsewhere/third.db"],
  "disabled_dbs": ["/path/to/datafolder/old.db"],
  "db_docs":      {"/path/to/datafolder/first.db": "/path/to/documents"},
  "db_drives":    {"/path/to/datafolder/first.db": "S:"},
  "docs_folder":  "/path/to/documents"
}
```

The active databases are every allmydox DB in `data_folder` plus
`external_dbs`, minus `disabled_dbs`. Older configs that only contain
`db_paths` are read as `external_dbs`; the legacy keys are dropped the next
time settings are saved.
