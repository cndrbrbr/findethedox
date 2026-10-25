# Features

## Word clouds

### Global view (startup)
On launch, before any search, all three clouds show the most frequent words
across the entire document collection (up to 450 entries, weighted by total
occurrence count). Word size reflects frequency.

### Co-occurrence search
Type a word in the search bar and press **Enter**. The clouds update to show
every noun, name, and verb that co-occurs with that word in the same sentence
or paragraph, scored as:

    score = sentence_co_occurrences × 1.3 + paragraph_co_occurrences × 1.0

Sentence co-occurrences are weighted 30 % higher because sharing a sentence is
a stronger semantic signal than sharing a paragraph.

### Three word-kind clouds
Results are split into three independent clouds:

| Cloud | Contains |
|---|---|
| Names | Proper names — people, places, organisations |
| Nouns | Common nouns |
| Verbs | Action words |

### Resize-aware rendering
Each cloud re-renders at the actual canvas pixel dimensions when the window is
resized. Word count scales with available area (area ÷ 3000, clamped 10–250).
A 150 ms debounce prevents unnecessary redraws during window drag.

---

## Navigation

### Left-click — follow a word
Left-clicking any word makes it the new search term. All three clouds
immediately recentre around it. Use this to navigate the vocabulary by
following semantic associations.

### Right-click — document list only
Right-clicking updates the document panel for that word without changing the
clouds. Useful for checking where a word appears without losing the current
co-occurrence view.

---

## Document panel

### Document list
A panel on the right of the window lists every document and page where the
current word appears, one row per (document, page) pair. Results from all
source databases are merged and deduplicated.

### Single-click — sentence preview
Single-clicking a document opens a sentence panel alongside it, showing all
sentences from that document that contain the search word. Extraction runs in
a background thread; works for PDF, DOCX, and TXT files.

### Double-click — document viewer
Double-clicking opens the built-in document viewer at the first relevant page.

---

## Document viewer

### PDF
Pages are rendered as images. The viewer opens at the first page containing
the word; all occurrences on the visible page are highlighted in yellow.
**◀ Prev** and **Next ▶** buttons navigate between pages.

### DOCX and TXT
Content is displayed as plain text. The cursor is placed at the first
occurrence of the word and scrolled into view.

---

## Multiple databases

Multiple allmydox databases can be combined into a single search session.
Each source database has its own cache file; co-occurrence scores and word
frequencies are summed across the caches at query time. Document lookups
query each source independently and deduplicate results before display.

---

## Cache management

### Pre-computed cache
All co-occurrence scores are pre-computed into one cache file per database
(`<name>.cache.db` in the data folder) so every search is one indexed SELECT
per database (2–5 ms each) instead of a 90-second aggregation.

### Incremental updates
Each source database has its own watermark. An update only processes documents
added since the last build; existing cache data is left intact.

### Settings dialog
Accessible via **File > Settings…** (`Ctrl+,`). Lets you:

- Choose the **data folder** — all allmydox databases in it are listed, and
  each one's cache `<name>.cache.db` is stored there
- **Tick** which databases to search; **add** databases from other folders
- See the **cache status** for each database:
  - `✓` green — cache up to date
  - `⚠` yellow — new documents exist in the source
  - `✗` red — cache file belongs to a different database
  - `—` grey — cache file does not exist yet
- **Build** or **Update** the caches with a live progress bar
- **Apply** — saves the configuration and reopens the main window with the new
  database set

### Rebuild Cache toolbar button
Triggers a full cache rebuild from scratch after a confirmation dialog. Use
this to recover from a corrupted cache or after major changes to the source
databases.

---

## Configuration

All user-configurable paths are saved to `~/.config/findethedox/config.json`
and restored on the next launch. The settings stored are:

| Key | Description |
|---|---|
| `data_folder` | Folder holding the databases and their cache files |
| `external_dbs` | Databases added from outside the data folder |
| `disabled_dbs` | Databases that are listed but not searched |
| `db_docs` | Per-database documents folders |
| `docs_folder` | Fallback folder for locating document files |
