"""
Pre-compute all co-occurrence scores into a flat SQLite cache.
Build once (~5 min for large DBs), then every search is a single indexed SELECT.
Subsequent runs only process documents added since the last build/update.
Each source database has its own cache file (<stem>.cache.db) in the data
folder; searches query all caches and merge the scores.
"""
import os
import sqlite3
from pathlib import Path
from typing import Callable

ProgressFn = Callable[[str, int, int], None]


def _norm(db_paths) -> list[str]:
    """Accept a single path string or a list; always return a list."""
    return [db_paths] if isinstance(db_paths, str) else list(db_paths)


def default_cache_path(db_paths) -> str:
    return str(Path(_norm(db_paths)[0]).with_suffix("")) + ".cache.db"


def cache_path_for(db_path: str, data_folder: str | None = None) -> str:
    """Cache file for one source DB: <data_folder>/<stem>.cache.db,
    or next to the DB when no data folder is set."""
    folder = Path(data_folder) if data_folder else Path(db_path).parent
    return str(folder / (Path(db_path).stem + ".cache.db"))


def is_cache_file(path) -> bool:
    return Path(path).name.endswith(".cache.db")


def find_databases(folder: str) -> list[str]:
    """Return all allmydox source DBs (*.db with a documents table) in folder."""
    found: list[str] = []
    try:
        candidates = sorted(Path(folder).glob("*.db"))
    except OSError:
        return found
    for p in candidates:
        if is_cache_file(p) or not p.is_file():
            continue
        try:
            conn = sqlite3.connect(f"{p.resolve().as_uri()}?mode=ro", uri=True)
            ok = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='documents'"
            ).fetchone()
            conn.close()
        except Exception:
            ok = None
        if ok:
            found.append(str(p.absolute()))
    return found


def is_built(cache_path: str) -> bool:
    return Path(cache_path).exists()


def _watermark(conn: sqlite3.Connection, db_path: str) -> int | None:
    """Max fileID already in the cache for db_path, or None if not present."""
    row = conn.execute("SELECT value FROM meta WHERE key=?",
                       (f"src:{db_path}",)).fetchone()
    if row is None:
        row = conn.execute(
            "SELECT value FROM meta WHERE key='max_file_id'"
        ).fetchone()
    if row is None:
        # Single-source cache created under a different path (folder moved)
        rows = conn.execute(
            "SELECT value FROM meta WHERE key LIKE 'src:%'"
        ).fetchall()
        if len(rows) == 1:
            row = rows[0]
    return int(row[0]) if row else None


def _source_max(db_path: str) -> int:
    sc = sqlite3.connect(db_path)
    row = sc.execute("SELECT MAX(fileID) FROM documents").fetchone()
    sc.close()
    return int(row[0]) if row and row[0] else 0


def status(cache_path: str, db_path: str) -> str:
    """'no_cache', 'missing', 'outdated' or 'current' for one source DB."""
    if not Path(cache_path).exists():
        return "no_cache"
    try:
        cc = sqlite3.connect(f"file:{cache_path}?mode=ro", uri=True)
        n_src = cc.execute(
            "SELECT COUNT(*) FROM meta WHERE key LIKE 'src:%'"
        ).fetchone()[0]
        # Old combined cache holding several databases → needs its own rebuild
        last = _watermark(cc, db_path) if n_src <= 1 else None
        cache_codes = cc.execute("SELECT 1 FROM meta WHERE key='codes'").fetchone()
        cc.close()
        if last is None:
            return "missing"
        cur = _source_max(db_path)
        if not cache_codes and db_has_codes(db_path):
            return "rebuild"
    except Exception:
        return "missing"
    return "outdated" if cur > last else "current"


def db_has_codes(db_path: str) -> bool:
    """True if the source DB was indexed by an allmydox version that stores codes."""
    try:
        sc = sqlite3.connect(f"{Path(db_path).absolute().as_uri()}?mode=ro", uri=True)
        ok = sc.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='code_occurrences'"
        ).fetchone() and sc.execute("SELECT 1 FROM code_occurrences LIMIT 1").fetchone()
        sc.close()
        return bool(ok)
    except Exception:
        return False


def _src_has_codes(conn: sqlite3.Connection) -> bool:
    return conn.execute(
        "SELECT 1 FROM src.sqlite_master WHERE type='table' AND name='code_occurrences'"
    ).fetchone() is not None


def has_codes(conns: list[sqlite3.Connection]) -> bool:
    """True if any cache contains codes (shows the Codes cloud)."""
    return any(c.execute(
        "SELECT 1 FROM word_freq WHERE kind='code' LIMIT 1").fetchone() for c in conns)


def needs_update(cache_path: str, db_path: str) -> bool:
    """Return True if the source DB has documents not yet in its cache."""
    return status(cache_path, db_path) in ("missing", "outdated")


def build(db_paths, cache_path: str, progress: ProgressFn | None = None):
    """
    Read one or more allmydox databases and write a pre-aggregated cache.
    Tables produced:
      cooccurrence(src_word, src_kind, tgt_word, tgt_kind, score)
      word_freq(word, kind, freq)
      meta(key, value)  — per-source max_file_id watermarks as 'src:<path>'
    """
    paths = _norm(db_paths)

    # Keep per-DB documents folders across a rebuild
    kept_docs: list[tuple[str, str]] = []
    if os.path.exists(cache_path):
        try:
            old = sqlite3.connect(cache_path)
            kept_docs = old.execute(
                "SELECT key, value FROM meta WHERE key LIKE 'docs%'"
            ).fetchall()
            old.close()
        except Exception:
            pass
        os.remove(cache_path)

    conn = sqlite3.connect(cache_path)
    conn.executescript("PRAGMA journal_mode=WAL; PRAGMA cache_size=-131072;")
    conn.executescript("""
        CREATE TABLE raw (
            sw TEXT NOT NULL, sk TEXT NOT NULL,
            tw TEXT NOT NULL, tk TEXT NOT NULL,
            sc REAL NOT NULL
        );
        CREATE TABLE freq_raw (
            word TEXT NOT NULL, kind TEXT NOT NULL, freq INTEGER NOT NULL
        );
    """)

    total = len(paths) * len(_build_steps(codes=True)) + 1   # +1 for consolidation
    source_maxes: dict[str, int] = {}
    any_codes = False
    offset = 0

    for db_path in paths:
        name = Path(db_path).name
        conn.execute(f"ATTACH DATABASE '{db_path.replace(chr(39), chr(39)*2)}' AS src")
        codes = _src_has_codes(conn)
        any_codes |= codes
        steps = _build_steps(freq_table="freq_raw", codes=codes)
        for i, (label, sql) in enumerate(steps):
            if progress:
                lbl = f"[{name}] {label}" if len(paths) > 1 else label
                progress(lbl, offset + i, total)
            conn.execute(sql)
            conn.commit()
        row = conn.execute("SELECT MAX(fileID) FROM src.documents").fetchone()
        source_maxes[db_path] = int(row[0]) if row and row[0] else 0
        conn.execute("DETACH DATABASE src")
        offset += len(steps)

    if progress:
        progress("Consolidating and indexing…", total - 1, total)

    conn.executescript("""
        CREATE TABLE cooccurrence AS
            SELECT sw AS src_word, sk AS src_kind,
                   tw AS tgt_word, tk AS tgt_kind,
                   SUM(sc) AS score
            FROM raw
            GROUP BY sw, sk, tw, tk;

        DROP TABLE raw;

        CREATE TABLE word_freq AS
            SELECT word, kind, SUM(freq) AS freq
            FROM freq_raw
            GROUP BY word, kind;

        DROP TABLE freq_raw;

        CREATE UNIQUE INDEX idx_wf_uk ON word_freq(word, kind);
        CREATE INDEX idx_cooc ON cooccurrence(lower(src_word));
        CREATE INDEX idx_wf   ON word_freq(kind, freq DESC);
    """)

    conn.execute("CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT)")
    for db_path, max_id in source_maxes.items():
        conn.execute("INSERT INTO meta VALUES (?, ?)",
                     (f"src:{db_path}", str(max_id)))
    if len(source_maxes) == 1:
        conn.execute("INSERT INTO meta VALUES ('max_file_id', ?)",
                     (str(next(iter(source_maxes.values()))),))
    if any_codes:
        conn.execute("INSERT INTO meta VALUES ('codes', '1')")
    conn.executemany("INSERT OR REPLACE INTO meta VALUES (?, ?)", kept_docs)
    conn.commit()
    conn.close()

    if progress:
        progress("Done", total, total)


def update(db_paths, cache_path: str, progress: ProgressFn | None = None):
    """
    Append scores for documents added to any source DB since the cache was
    last built or updated.  Each source has its own max_file_id watermark.
    """
    paths = _norm(db_paths)
    conn = sqlite3.connect(cache_path)
    conn.executescript("PRAGMA journal_mode=WAL; PRAGMA cache_size=-131072;")

    # Determine which sources have new documents
    sources_to_update: list[tuple[str, int, int]] = []
    for db_path in paths:
        try:
            last = _watermark(conn, db_path) or 0
        except Exception:
            last = 0
        try:
            esc = db_path.replace("'", "''")
            conn.execute(f"ATTACH DATABASE '{esc}' AS src")
            row = conn.execute("SELECT MAX(fileID) FROM src.documents").fetchone()
            cur = int(row[0]) if row and row[0] else 0
            conn.execute("DETACH DATABASE src")
        except Exception:
            cur = 0
        if cur > last:
            sources_to_update.append((db_path, last, cur))

    if not sources_to_update:
        conn.close()
        if progress:
            progress("Already up to date", 1, 1)
        return

    n_steps = len(_build_steps(codes=True))
    total = len(sources_to_update) * (n_steps + 1)
    offset = 0

    for db_path, last_file_id, cur_max in sources_to_update:
        name = Path(db_path).name
        conn.executescript("""
            CREATE TABLE raw_delta (
                sw TEXT NOT NULL, sk TEXT NOT NULL,
                tw TEXT NOT NULL, tk TEXT NOT NULL,
                sc REAL NOT NULL
            );
            CREATE TABLE freq_delta (
                word TEXT NOT NULL, kind TEXT NOT NULL, freq INTEGER NOT NULL
            );
        """)

        esc = db_path.replace("'", "''")
        conn.execute(f"ATTACH DATABASE '{esc}' AS src")

        delta_steps = _build_steps(min_file_id=last_file_id,
                                   raw_table="raw_delta",
                                   freq_table="freq_delta",
                                   codes=_src_has_codes(conn))
        for i, (label, sql) in enumerate(delta_steps):
            if progress:
                lbl = f"[{name}] {label}" if len(sources_to_update) > 1 else label
                progress(lbl, offset + i, total)
            conn.execute(sql)
            conn.commit()

        if progress:
            lbl = f"[{name}] Merging…" if len(sources_to_update) > 1 else "Merging into cache…"
            progress(lbl, offset + n_steps, total)

        conn.executescript("""
            INSERT INTO cooccurrence
                SELECT sw, sk, tw, tk, SUM(sc)
                FROM raw_delta GROUP BY sw, sk, tw, tk;

            INSERT INTO word_freq(word, kind, freq)
                SELECT word, kind, SUM(freq) FROM freq_delta GROUP BY word, kind
                ON CONFLICT(word, kind) DO UPDATE SET freq = freq + excluded.freq;

            DROP TABLE raw_delta;
            DROP TABLE freq_delta;
        """)
        conn.execute("""
            INSERT INTO meta VALUES (?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
        """, (f"src:{db_path}", str(cur_max)))
        if len(paths) == 1:
            conn.execute("""
                INSERT INTO meta VALUES ('max_file_id', ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value
            """, (str(cur_max),))
        conn.commit()
        conn.execute("DETACH DATABASE src")
        offset += n_steps + 1

    conn.close()
    if progress:
        progress("Done", total, total)


# ---------------------------------------------------------------------------
# Cached query functions
# ---------------------------------------------------------------------------

def get_docs_folder(cache_path: str, db_path: str) -> str:
    """Return the documents folder stored in a per-DB cache ('' if unset)."""
    if not Path(cache_path).exists():
        return ""
    try:
        conn = sqlite3.connect(f"file:{cache_path}?mode=ro", uri=True)
        row = conn.execute(
            "SELECT value FROM meta WHERE key='docs_folder'"
        ).fetchone()
        if row is None:   # older combined cache: 'docs:<path>' keys
            row = conn.execute("SELECT value FROM meta WHERE key=?",
                               (f"docs:{db_path}",)).fetchone()
        conn.close()
        return row[0] if row else ""
    except Exception:
        return ""


def set_docs_folder(cache_path: str, docs_folder: str):
    """Store or update the documents folder in a per-DB cache's meta table."""
    if not Path(cache_path).exists():
        return
    try:
        conn = sqlite3.connect(cache_path)
        conn.execute("""
            INSERT INTO meta VALUES ('docs_folder', ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
        """, (docs_folder,))
        conn.commit()
        conn.close()
    except Exception:
        pass


def get_docs_drive(cache_path: str) -> str:
    """Return the Windows drive letter (e.g. 'S:') stored in a per-DB cache."""
    if not Path(cache_path).exists():
        return ""
    try:
        conn = sqlite3.connect(f"file:{cache_path}?mode=ro", uri=True)
        row = conn.execute(
            "SELECT value FROM meta WHERE key='docs_drive'"
        ).fetchone()
        conn.close()
        return row[0] if row else ""
    except Exception:
        return ""


def set_docs_drive(cache_path: str, drive: str):
    """Store the Windows drive letter in a per-DB cache ('' removes it)."""
    if not Path(cache_path).exists():
        return
    try:
        conn = sqlite3.connect(cache_path)
        if drive:
            conn.execute("""
                INSERT INTO meta VALUES ('docs_drive', ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value
            """, (drive,))
        else:
            conn.execute("DELETE FROM meta WHERE key='docs_drive'")
        conn.commit()
        conn.close()
    except Exception:
        pass


def connect(cache_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{cache_path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def _merge(results, key_cols: tuple[str, str], val_col: str, limit: int) -> list[dict]:
    """Sum val_col over identical (word, kind) keys from several caches."""
    totals: dict[tuple, float] = {}
    for rows in results:
        for r in rows:
            k = (r[key_cols[0]], r[key_cols[1]])
            totals[k] = totals.get(k, 0) + r[val_col]
    merged = sorted(totals.items(), key=lambda kv: kv[1], reverse=True)[:limit]
    return [{key_cols[0]: w, key_cols[1]: kind, val_col: v}
            for (w, kind), v in merged]


# Words per kind (name, noun, verb, code), so frequent numbers cannot crowd
# out the other clouds
_COOC_PER_KIND = 80
_FREQ_PER_KIND = 150


def _top_per_kind(rows: list[dict], kind_col: str, val_col: str, n: int) -> list[dict]:
    out: list[dict] = []
    counts: dict[str, int] = {}
    for r in sorted(rows, key=lambda r: r[val_col], reverse=True):
        k = r[kind_col]
        if counts.get(k, 0) < n:
            counts[k] = counts.get(k, 0) + 1
            out.append(r)
    return out


def cooccurrences(conns: list[sqlite3.Connection], word: str) -> list[dict]:
    """Return dicts (tgt_word, tgt_kind, score) ordered by score desc,
    summed over all caches, at most _COOC_PER_KIND per kind."""
    merged = _merge((c.execute("""
        SELECT tgt_word, tgt_kind, score FROM (
            SELECT tgt_word, tgt_kind, SUM(score) AS score,
                   ROW_NUMBER() OVER (PARTITION BY tgt_kind
                                      ORDER BY SUM(score) DESC) AS rn
            FROM cooccurrence
            WHERE lower(src_word) = ?
            GROUP BY tgt_word, tgt_kind)
        WHERE rn <= ?
    """, (word.lower(), _COOC_PER_KIND)).fetchall() for c in conns),
        ("tgt_word", "tgt_kind"), "score", 10**9)
    return _top_per_kind(merged, "tgt_kind", "score", _COOC_PER_KIND)


def pair_cooccurrences(conns: list[sqlite3.Connection], word1: str, word2: str) -> list[dict]:
    """Words related to both search words (scores added); if no word is
    related to both, the words related to either.  The search words
    themselves are left out."""
    r1 = {(r["tgt_word"], r["tgt_kind"]): r["score"] for r in cooccurrences(conns, word1)}
    r2 = {(r["tgt_word"], r["tgt_kind"]): r["score"] for r in cooccurrences(conns, word2)}
    keys = (r1.keys() & r2.keys()) or (r1.keys() | r2.keys())
    skip = {word1.lower(), word2.lower()}
    rows = [{"tgt_word": w, "tgt_kind": k, "score": r1.get((w, k), 0) + r2.get((w, k), 0)}
            for w, k in keys if w.lower() not in skip]
    return _top_per_kind(rows, "tgt_kind", "score", _COOC_PER_KIND)


def global_frequencies(conns: list[sqlite3.Connection]) -> list[dict]:
    """Return dicts (word, kind, freq) for the most frequent words across
    caches, at most _FREQ_PER_KIND per kind."""
    # One indexed query per kind (idx_wf on kind, freq DESC)
    merged = _merge((c.execute(" UNION ALL ".join(
        f"SELECT * FROM (SELECT word, kind, freq FROM word_freq "
        f"WHERE kind='{k}' ORDER BY freq DESC LIMIT {_FREQ_PER_KIND})"
        for k in ("name", "noun", "verb", "code"))).fetchall() for c in conns),
        ("word", "kind"), "freq", 10**9)
    return _top_per_kind(merged, "kind", "freq", _FREQ_PER_KIND)


# ---------------------------------------------------------------------------
# SQL step definitions — each step is one INSERT executed in sequence
# ---------------------------------------------------------------------------

_S = 1.3   # sentence weight
_P = 1.0   # paragraph weight


def _build_steps(
    min_file_id: int = 0,
    raw_table: str = "raw",
    freq_table: str = "word_freq",
    codes: bool = False,
) -> list[tuple[str, str]]:
    """
    Return (label, sql) pairs for each aggregation step.
    When min_file_id > 0 each step filters to occurrence rows with
    fileID > min_file_id so only new documents are processed.
    """
    steps: list[tuple[str, str]] = []
    fi = min_file_id

    def cooc(label: str, sql: str):
        steps.append((label, f"INSERT INTO {raw_table} {sql}"))

    def freq(label: str, sql: str):
        steps.append((label, f"INSERT INTO {freq_table} {sql}"))

    # ------------------------------------------------------------------
    # noun_sentence and noun_paragraph: all four occ_type combinations,
    # forward and reverse so every word maps to all its partners
    # ------------------------------------------------------------------
    for tbl, w, pfx in [
        ("src.noun_sentence",  _S, "Sentence"),
        ("src.noun_paragraph", _P, "Paragraph"),
    ]:
        w_nn = f"\n            WHERE o1.fileID>{fi} OR o2.fileID>{fi}" if fi else ""
        w_na = f"\n            WHERE o.fileID>{fi} OR nao.fileID>{fi}" if fi else ""
        w_aa = f"\n            WHERE na1o.fileID>{fi} OR na2o.fileID>{fi}" if fi else ""

        # noun(occ1) ↔ noun(occ2)
        cooc(f"{pfx}: noun→noun", f"""
            SELECT n1.noun,'noun',n2.noun,'noun',COUNT(*)*{w}
            FROM {tbl} c
            JOIN src.noun_occurrences o1 ON c.occ1_type='noun' AND c.occ1_id=o1.nounOccurrenceID
            JOIN src.nouns n1 ON o1.nounID=n1.nounID
            JOIN src.noun_occurrences o2 ON c.occ2_type='noun' AND c.occ2_id=o2.nounOccurrenceID
            JOIN src.nouns n2 ON o2.nounID=n2.nounID{w_nn}
            GROUP BY o1.nounID,o2.nounID""")
        cooc(f"{pfx}: noun←noun", f"""
            SELECT n2.noun,'noun',n1.noun,'noun',COUNT(*)*{w}
            FROM {tbl} c
            JOIN src.noun_occurrences o1 ON c.occ1_type='noun' AND c.occ1_id=o1.nounOccurrenceID
            JOIN src.nouns n1 ON o1.nounID=n1.nounID
            JOIN src.noun_occurrences o2 ON c.occ2_type='noun' AND c.occ2_id=o2.nounOccurrenceID
            JOIN src.nouns n2 ON o2.nounID=n2.nounID{w_nn}
            GROUP BY o2.nounID,o1.nounID""")

        # noun(occ1) ↔ name(occ2)
        cooc(f"{pfx}: noun→name", f"""
            SELECT n.noun,'noun',na.name,'name',COUNT(*)*{w}
            FROM {tbl} c
            JOIN src.noun_occurrences o  ON c.occ1_type='noun' AND c.occ1_id=o.nounOccurrenceID
            JOIN src.nouns n  ON o.nounID=n.nounID
            JOIN src.name_occurrences nao ON c.occ2_type='name' AND c.occ2_id=nao.nameOccurrenceID
            JOIN src.names na ON nao.nameID=na.nameID{w_na}
            GROUP BY o.nounID,nao.nameID""")
        cooc(f"{pfx}: name←noun", f"""
            SELECT na.name,'name',n.noun,'noun',COUNT(*)*{w}
            FROM {tbl} c
            JOIN src.noun_occurrences o  ON c.occ1_type='noun' AND c.occ1_id=o.nounOccurrenceID
            JOIN src.nouns n  ON o.nounID=n.nounID
            JOIN src.name_occurrences nao ON c.occ2_type='name' AND c.occ2_id=nao.nameOccurrenceID
            JOIN src.names na ON nao.nameID=na.nameID{w_na}
            GROUP BY nao.nameID,o.nounID""")

        # name(occ1) ↔ noun(occ2)
        cooc(f"{pfx}: name→noun", f"""
            SELECT na.name,'name',n.noun,'noun',COUNT(*)*{w}
            FROM {tbl} c
            JOIN src.name_occurrences nao ON c.occ1_type='name' AND c.occ1_id=nao.nameOccurrenceID
            JOIN src.names na ON nao.nameID=na.nameID
            JOIN src.noun_occurrences o  ON c.occ2_type='noun' AND c.occ2_id=o.nounOccurrenceID
            JOIN src.nouns n  ON o.nounID=n.nounID{w_na}
            GROUP BY nao.nameID,o.nounID""")
        cooc(f"{pfx}: noun←name", f"""
            SELECT n.noun,'noun',na.name,'name',COUNT(*)*{w}
            FROM {tbl} c
            JOIN src.name_occurrences nao ON c.occ1_type='name' AND c.occ1_id=nao.nameOccurrenceID
            JOIN src.names na ON nao.nameID=na.nameID
            JOIN src.noun_occurrences o  ON c.occ2_type='noun' AND c.occ2_id=o.nounOccurrenceID
            JOIN src.nouns n  ON o.nounID=n.nounID{w_na}
            GROUP BY o.nounID,nao.nameID""")

        # name(occ1) ↔ name(occ2)
        cooc(f"{pfx}: name→name", f"""
            SELECT na1.name,'name',na2.name,'name',COUNT(*)*{w}
            FROM {tbl} c
            JOIN src.name_occurrences na1o ON c.occ1_type='name' AND c.occ1_id=na1o.nameOccurrenceID
            JOIN src.names na1 ON na1o.nameID=na1.nameID
            JOIN src.name_occurrences na2o ON c.occ2_type='name' AND c.occ2_id=na2o.nameOccurrenceID
            JOIN src.names na2 ON na2o.nameID=na2.nameID{w_aa}
            GROUP BY na1o.nameID,na2o.nameID""")
        cooc(f"{pfx}: name←name", f"""
            SELECT na2.name,'name',na1.name,'name',COUNT(*)*{w}
            FROM {tbl} c
            JOIN src.name_occurrences na1o ON c.occ1_type='name' AND c.occ1_id=na1o.nameOccurrenceID
            JOIN src.names na1 ON na1o.nameID=na1.nameID
            JOIN src.name_occurrences na2o ON c.occ2_type='name' AND c.occ2_id=na2o.nameOccurrenceID
            JOIN src.names na2 ON na2o.nameID=na2.nameID{w_aa}
            GROUP BY na2o.nameID,na1o.nameID""")

    # ------------------------------------------------------------------
    # noun_verb_sentence — nouns/names ↔ verbs, both directions
    # ------------------------------------------------------------------
    for etype, occ_tbl, vtbl, occ_pk, vid, vcol in [
        ("noun", "src.noun_occurrences", "src.nouns", "nounOccurrenceID", "nounID", "noun"),
        ("name", "src.name_occurrences", "src.names", "nameOccurrenceID", "nameID", "name"),
    ]:
        w_ev = f"\n            WHERE o.fileID>{fi} OR vo.fileID>{fi}" if fi else ""

        cooc(f"Sentence: {etype}→verb", f"""
            SELECT e.{vcol},'{etype}',v.verb,'verb',COUNT(*)*{_S}
            FROM src.noun_verb_sentence nvs
            JOIN {occ_tbl} o  ON nvs.noun_occ_type='{etype}' AND nvs.noun_occ_id=o.{occ_pk}
            JOIN {vtbl} e     ON o.{vid}=e.{vid}
            JOIN src.verb_occurrences vo ON nvs.verb_occ_id=vo.verbOccurrenceID
            JOIN src.verbs v  ON vo.verbID=v.verbID{w_ev}
            GROUP BY o.{vid},vo.verbID""")
        cooc(f"Sentence: verb→{etype}", f"""
            SELECT v.verb,'verb',e.{vcol},'{etype}',COUNT(*)*{_S}
            FROM src.noun_verb_sentence nvs
            JOIN {occ_tbl} o  ON nvs.noun_occ_type='{etype}' AND nvs.noun_occ_id=o.{occ_pk}
            JOIN {vtbl} e     ON o.{vid}=e.{vid}
            JOIN src.verb_occurrences vo ON nvs.verb_occ_id=vo.verbOccurrenceID
            JOIN src.verbs v  ON vo.verbID=v.verbID{w_ev}
            GROUP BY vo.verbID,o.{vid}""")

    # ------------------------------------------------------------------
    # Word frequencies (for global cloud view)
    # ------------------------------------------------------------------
    for kind, occ_tbl, vocab_tbl, id_col, word_col in [
        ("noun", "src.noun_occurrences", "src.nouns", "nounID", "noun"),
        ("name", "src.name_occurrences", "src.names", "nameID", "name"),
        ("verb", "src.verb_occurrences", "src.verbs", "verbID", "verb"),
    ]:
        w_f = f"\n            WHERE o.fileID>{fi}" if fi else ""
        freq(f"Frequencies: {kind}s", f"""
            SELECT w.{word_col},'{kind}',COUNT(*)
            FROM {occ_tbl} o JOIN {vocab_tbl} w USING({id_col}){w_f}
            GROUP BY o.{id_col}""")

    if codes:
        _code_steps(cooc, freq, fi)

    return steps


def _code_steps(cooc, freq, fi: int):
    """Steps for allmydox code tables: code_sentence / code_paragraph link a
    code occurrence with a noun, name or code occurrence."""
    occ = {
        "noun": ("src.noun_occurrences", "nounOccurrenceID", "src.nouns", "nounID", "noun"),
        "name": ("src.name_occurrences", "nameOccurrenceID", "src.names", "nameID", "name"),
        "code": ("src.code_occurrences", "codeOccurrenceID", "src.codes", "codeID", "code"),
    }
    for tbl, w, pfx in [
        ("src.code_sentence",  _S, "Sentence"),
        ("src.code_paragraph", _P, "Paragraph"),
    ]:
        for otype, (o_tbl, o_pk, v_tbl, v_id, v_col) in occ.items():
            where = f"\n            WHERE co.fileID>{fi} OR oo.fileID>{fi}" if fi else ""
            body = f"""
            FROM {tbl} c
            JOIN src.code_occurrences co ON c.code_occ_id=co.codeOccurrenceID
            JOIN src.codes cd ON co.codeID=cd.codeID
            JOIN {o_tbl} oo ON c.occ_type='{otype}' AND c.occ_id=oo.{o_pk}
            JOIN {v_tbl} ov ON oo.{v_id}=ov.{v_id}{where}"""
            cooc(f"{pfx}: code→{otype}", f"""
            SELECT cd.code,'code',ov.{v_col},'{otype}',COUNT(*)*{w}{body}
            GROUP BY co.codeID,oo.{v_id}""")
            cooc(f"{pfx}: {otype}→code", f"""
            SELECT ov.{v_col},'{otype}',cd.code,'code',COUNT(*)*{w}{body}
            GROUP BY oo.{v_id},co.codeID""")

    w_f = f"\n            WHERE o.fileID>{fi}" if fi else ""
    freq("Frequencies: codes", f"""
            SELECT w.code,'code',COUNT(*)
            FROM src.code_occurrences o JOIN src.codes w USING(codeID){w_f}
            GROUP BY o.codeID""")
