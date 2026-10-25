# findethedox

Visual exploration tool for document collections indexed by
[allmydox](https://github.com/cndrbrbr/allmydox). Displays three interactive
word clouds — Names, Nouns, Verbs — and lets you navigate from any word
directly to the documents and sentences where it appears.

Runs on **Linux** and **Windows**.

---

## Requirements

- Python 3.9 or newer
- One or more `allmydox.db` databases produced by [allmydox](https://github.com/cndrbrbr/allmydox)

---

## Setup

**Linux**

```bash
cd findethedox
bash setup.sh
```

**Windows**

```bat
cd findethedox
setup.bat
```

Both scripts install PyQt6, wordcloud, pymupdf, and matplotlib.

---

## Starting the application

```bash
python3 main.py
```

If no database path is given and `allmydox.db` is not found in the current
directory, a file dialog opens so you can browse to the database.

You can also pass one or more database paths directly:

```bash
python3 main.py /path/to/allmydox.db
python3 main.py /path/to/first.db /path/to/second.db
```

If the documents have been moved or the database was created on a different
machine, pass the folder where the files now live:

```bash
python3 main.py /path/to/allmydox.db --docs /path/to/documents
```

To keep all databases and their cache files together, put the `.db` files in
one folder and pass it as the data folder — every allmydox database found
there is searched:

```bash
python3 main.py --data /path/to/datafolder
```

On the first launch the application builds a search cache for each database
(`<name>.cache.db` in the data folder). This takes a few minutes depending on
database size and only happens once per database — subsequent launches are
instant.

---

## Interface overview

![findethedox screenshot](Screenshot_3.png)

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

```
┌─────────────────────────────────────────────────────────┬──────────────────┐
│  Search: [ type a word and press Enter ______________ ] │                  │
│                                                         │  Documents       │
│  ┌──────────────┐ ┌──────────────┐ ┌────────────────┐  │  (right-click    │
│  │    Names     │ │    Nouns     │ │     Verbs      │  │   or Enter)      │
│  │              │ │              │ │                │  │                  │
│  │  word cloud  │ │  word cloud  │ │  word cloud    │  │  ┌────────────┐  │
│  │              │ │              │ │                │  │  │ Sentences  │  │
│  └──────────────┘ └──────────────┘ └────────────────┘  │  │ (click doc)│  │
└─────────────────────────────────────────────────────────┴──────────────────┘
```

---

## Using the word clouds

### On startup — global view

When no search term has been entered, all three clouds show the most frequent
words across the entire document collection. Larger words appear more often in
the documents.

### Searching for a word

Type a word in the search bar and press **Enter**. The three clouds update to
show all nouns, names, and verbs that co-occur with your word in sentences or
paragraphs. Word size reflects the co-occurrence score:

> **score = sentence co-occurrences × 1.3 + paragraph co-occurrences**

Sentence co-occurrences are weighted 30 % higher because a shared sentence
is a stronger signal than a shared paragraph. If the word exists in more than
one category, results from all categories are combined.

### Left-click — follow a word

Left-clicking any word in any cloud makes that word the new search term. All
three clouds immediately recentre around the clicked word. This lets you
navigate through the vocabulary by following associations.

### Right-click — document list without changing the clouds

Right-clicking a word updates the document panel for that word without changing
the current cloud view. Useful for checking where a word appears while keeping
the co-occurrence picture visible.

---

## Document panel

The **document panel** on the right side of the window shows every document and
page where the current word appears, one entry per (document, page) pair.
Results from all loaded databases are merged and deduplicated.

The panel updates whenever the active word changes:

- **Enter** after typing → panel updates for the typed word
- **Left-click** on a cloud word → clouds recentre and panel updates
- **Right-click** on a cloud word → panel updates without changing the clouds

### Single-click — sentence preview

Single-clicking any document in the list opens a **sentence panel** alongside
it, showing all sentences from that document that contain the search word.
Works for PDF, DOCX, and TXT files.

### Double-click — open document viewer

Double-clicking opens the built-in document viewer at the relevant page.

---

## Document viewer

### PDF files

The PDF is rendered page by page. The viewer opens at the first page
containing the word; all occurrences of the word on the visible page are
**highlighted in yellow**.

Use the **◀ Prev** and **Next ▶** buttons to move between pages.

### DOCX and TXT files

The file content is shown as plain text. The cursor is placed at the first
occurrence of the word and scrolled into view.

---

## Menus and toolbar

### File menu

| Item | Shortcut | Description |
|---|---|---|
| Open Database… | Ctrl+O | Pick a different `.db` file and open it in a new window |
| Settings… | Ctrl+, | Data folder, databases and caches (see below) |

### Settings menu

| Item | Description |
|---|---|
| Set Documents Folder… | Override the folder used to locate document files |

**Set Documents Folder** is useful when the database was created on a different
machine or the document files have been moved. The application first tries the
path stored in the database; if the file is not found there it looks for the
filename inside the documents folder you specify.

### Toolbar

| Button | Description |
|---|---|
| Rebuild Cache | Deletes the caches of all active databases and rebuilds them from scratch (confirmation required) |

---

## Settings dialog

Open via **File > Settings…** or press **Ctrl+,**.

This dialog is the central place to configure where databases and caches
live and which databases are searched.

### Data folder

Select the folder that holds your allmydox databases. Every allmydox `.db`
file in it is listed automatically (click **Rescan** after copying new files
into the folder). Each database gets its **own cache file**,
`<name>.cache.db`, stored in the same folder — e.g. `archive.db` →
`archive.cache.db`.

### Database list

Each database appears with a checkbox, its cache file and its documents
folder. Only ticked databases are searched. The status icon shows the state
of the database's cache:

| Icon | Colour | Meaning |
|---|---|---|
| ✓ | Green | Cache up to date |
| ⚠ | Yellow | Cache exists, but new documents have been added to the source since the last build |
| ✗ | Red | A cache file with that name exists but does not belong to this database |
| — | Grey | Cache file does not exist yet |

**Add Database…** adds a database stored outside the data folder; its cache
is still written to the data folder. Two databases with the same file name
would share a cache file, so this is refused. **Remove Selected** removes
external databases; databases inside the data folder are only unticked.

### Building and updating the caches

The **Build Caches** / **Update Caches** button processes every ticked
database whose cache is missing or outdated:

- A database without a cache gets a full build.
- A database marked ⚠ gets an incremental update — only the new documents
  are processed.

A progress dialog shows each step. The status icons refresh automatically when
the operation completes.

### Applying changes

Click **Apply** to save the configuration and reopen the main window with the
ticked databases. Click **Cancel** to discard any changes.

---

## Multiple databases

findethedox can search across several allmydox databases simultaneously.
Each database has its own cache file; when you search, the co-occurrence
scores and word frequencies of all ticked databases are added together.
Documents from all sources are listed together and duplicates are removed.

To use multiple databases, put the `.db` files into the data folder (or add
them via **Add Database…**) in **Settings…**, then build the caches. Because
the caches are independent, adding or removing a database never requires
rebuilding the others.

---

## Notes

- The source databases are opened **read-only** during normal operation. The
  only write to a source database is the one-time index creation on first launch.
- If you add new documents to a source database using allmydox, open
  **Settings…** and click **Update Caches** to pick them up.
- All settings are saved automatically to
  `~/.config/findethedox/config.json` and restored on the next launch.
