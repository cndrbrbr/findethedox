"""
Pre-compute all co-occurrence scores into a flat SQLite cache.
Build once (~5 min for large DBs), then every search is a single indexed SELECT.
"""
import os
import sqlite3
from pathlib import Path
from typing import Callable

ProgressFn = Callable[[str, int, int], None]


def default_cache_path(db_path: str) -> str:
    return str(Path(db_path).with_suffix("")) + ".cache.db"


def is_built(cache_path: str) -> bool:
    return Path(cache_path).exists()


def build(db_path: str, cache_path: str, progress: ProgressFn | None = None):
    """
    Read allmydox.db and write a pre-aggregated cache.
    Tables produced:
      cooccurrence(src_word, src_kind, tgt_word, tgt_kind, score)
      word_freq(word, kind, freq)
    """
    if os.path.exists(cache_path):
        os.remove(cache_path)

    conn = sqlite3.connect(cache_path)
    conn.executescript("PRAGMA journal_mode=WAL; PRAGMA cache_size=-131072;")
    conn.execute(f"ATTACH DATABASE '{db_path}' AS src")
    conn.executescript("""
        CREATE TABLE raw (
            sw TEXT NOT NULL, sk TEXT NOT NULL,
            tw TEXT NOT NULL, tk TEXT NOT NULL,
            sc REAL NOT NULL
        );
        CREATE TABLE word_freq (
            word TEXT NOT NULL, kind TEXT NOT NULL, freq INTEGER NOT NULL
        );
    """)

    steps = _build_steps()
    total = len(steps) + 1   # +1 for consolidation

    for i, (label, sql) in enumerate(steps):
        if progress:
            progress(label, i, total)
        conn.execute(sql)
        conn.commit()

    if progress:
        progress("Consolidating and indexing…", len(steps), total)

    conn.executescript("""
        CREATE TABLE cooccurrence AS
            SELECT sw AS src_word, sk AS src_kind,
                   tw AS tgt_word, tk AS tgt_kind,
                   SUM(sc) AS score
            FROM raw
            GROUP BY sw, sk, tw, tk;

        DROP TABLE raw;

        CREATE INDEX idx_cooc ON cooccurrence(lower(src_word));
        CREATE INDEX idx_wf   ON word_freq(kind, freq DESC);
    """)
    conn.commit()
    conn.close()

    if progress:
        progress("Done", total, total)


# ---------------------------------------------------------------------------
# Cached query functions
# ---------------------------------------------------------------------------

def connect(cache_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{cache_path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def cooccurrences(conn: sqlite3.Connection, word: str) -> list:
    """Return rows (tgt_word, tgt_kind, score) ordered by score desc."""
    return conn.execute("""
        SELECT tgt_word, tgt_kind, SUM(score) AS score
        FROM cooccurrence
        WHERE lower(src_word) = ?
        GROUP BY tgt_word, tgt_kind
        ORDER BY score DESC
        LIMIT 200
    """, (word.lower(),)).fetchall()


def global_frequencies(conn: sqlite3.Connection) -> list:
    """Return rows (word, kind, freq) for the most frequent words."""
    return conn.execute("""
        SELECT word, kind, freq FROM word_freq
        ORDER BY freq DESC LIMIT 450
    """).fetchall()


# ---------------------------------------------------------------------------
# SQL step definitions — each step is one INSERT executed in sequence
# ---------------------------------------------------------------------------

_S = 1.3   # sentence weight
_P = 1.0   # paragraph weight


def _build_steps() -> list[tuple[str, str]]:
    steps: list[tuple[str, str]] = []

    def cooc(label: str, sql: str):
        """Add a step that inserts into the raw co-occurrence table."""
        steps.append((label, f"INSERT INTO raw {sql}"))

    def freq(label: str, sql: str):
        """Add a step that inserts into word_freq."""
        steps.append((label, f"INSERT INTO word_freq {sql}"))

    # ------------------------------------------------------------------
    # noun_sentence and noun_paragraph: all four occ_type combinations,
    # forward and reverse so every word maps to all its partners
    # ------------------------------------------------------------------
    for tbl, w, pfx in [
        ("src.noun_sentence",  _S, "Sentence"),
        ("src.noun_paragraph", _P, "Paragraph"),
    ]:
        # noun(occ1) ↔ noun(occ2)
        cooc(f"{pfx}: noun→noun", f"""
            SELECT n1.noun,'noun',n2.noun,'noun',COUNT(*)*{w}
            FROM {tbl} c
            JOIN src.noun_occurrences o1 ON c.occ1_type='noun' AND c.occ1_id=o1.nounOccurrenceID
            JOIN src.nouns n1 ON o1.nounID=n1.nounID
            JOIN src.noun_occurrences o2 ON c.occ2_type='noun' AND c.occ2_id=o2.nounOccurrenceID
            JOIN src.nouns n2 ON o2.nounID=n2.nounID
            GROUP BY o1.nounID,o2.nounID""")
        cooc(f"{pfx}: noun←noun", f"""
            SELECT n2.noun,'noun',n1.noun,'noun',COUNT(*)*{w}
            FROM {tbl} c
            JOIN src.noun_occurrences o1 ON c.occ1_type='noun' AND c.occ1_id=o1.nounOccurrenceID
            JOIN src.nouns n1 ON o1.nounID=n1.nounID
            JOIN src.noun_occurrences o2 ON c.occ2_type='noun' AND c.occ2_id=o2.nounOccurrenceID
            JOIN src.nouns n2 ON o2.nounID=n2.nounID
            GROUP BY o2.nounID,o1.nounID""")

        # noun(occ1) ↔ name(occ2)
        cooc(f"{pfx}: noun→name", f"""
            SELECT n.noun,'noun',na.name,'name',COUNT(*)*{w}
            FROM {tbl} c
            JOIN src.noun_occurrences o  ON c.occ1_type='noun' AND c.occ1_id=o.nounOccurrenceID
            JOIN src.nouns n  ON o.nounID=n.nounID
            JOIN src.name_occurrences nao ON c.occ2_type='name' AND c.occ2_id=nao.nameOccurrenceID
            JOIN src.names na ON nao.nameID=na.nameID
            GROUP BY o.nounID,nao.nameID""")
        cooc(f"{pfx}: name←noun", f"""
            SELECT na.name,'name',n.noun,'noun',COUNT(*)*{w}
            FROM {tbl} c
            JOIN src.noun_occurrences o  ON c.occ1_type='noun' AND c.occ1_id=o.nounOccurrenceID
            JOIN src.nouns n  ON o.nounID=n.nounID
            JOIN src.name_occurrences nao ON c.occ2_type='name' AND c.occ2_id=nao.nameOccurrenceID
            JOIN src.names na ON nao.nameID=na.nameID
            GROUP BY nao.nameID,o.nounID""")

        # name(occ1) ↔ noun(occ2)
        cooc(f"{pfx}: name→noun", f"""
            SELECT na.name,'name',n.noun,'noun',COUNT(*)*{w}
            FROM {tbl} c
            JOIN src.name_occurrences nao ON c.occ1_type='name' AND c.occ1_id=nao.nameOccurrenceID
            JOIN src.names na ON nao.nameID=na.nameID
            JOIN src.noun_occurrences o  ON c.occ2_type='noun' AND c.occ2_id=o.nounOccurrenceID
            JOIN src.nouns n  ON o.nounID=n.nounID
            GROUP BY nao.nameID,o.nounID""")
        cooc(f"{pfx}: noun←name", f"""
            SELECT n.noun,'noun',na.name,'name',COUNT(*)*{w}
            FROM {tbl} c
            JOIN src.name_occurrences nao ON c.occ1_type='name' AND c.occ1_id=nao.nameOccurrenceID
            JOIN src.names na ON nao.nameID=na.nameID
            JOIN src.noun_occurrences o  ON c.occ2_type='noun' AND c.occ2_id=o.nounOccurrenceID
            JOIN src.nouns n  ON o.nounID=n.nounID
            GROUP BY o.nounID,nao.nameID""")

        # name(occ1) ↔ name(occ2)
        cooc(f"{pfx}: name→name", f"""
            SELECT na1.name,'name',na2.name,'name',COUNT(*)*{w}
            FROM {tbl} c
            JOIN src.name_occurrences na1o ON c.occ1_type='name' AND c.occ1_id=na1o.nameOccurrenceID
            JOIN src.names na1 ON na1o.nameID=na1.nameID
            JOIN src.name_occurrences na2o ON c.occ2_type='name' AND c.occ2_id=na2o.nameOccurrenceID
            JOIN src.names na2 ON na2o.nameID=na2.nameID
            GROUP BY na1o.nameID,na2o.nameID""")
        cooc(f"{pfx}: name←name", f"""
            SELECT na2.name,'name',na1.name,'name',COUNT(*)*{w}
            FROM {tbl} c
            JOIN src.name_occurrences na1o ON c.occ1_type='name' AND c.occ1_id=na1o.nameOccurrenceID
            JOIN src.names na1 ON na1o.nameID=na1.nameID
            JOIN src.name_occurrences na2o ON c.occ2_type='name' AND c.occ2_id=na2o.nameOccurrenceID
            JOIN src.names na2 ON na2o.nameID=na2.nameID
            GROUP BY na2o.nameID,na1o.nameID""")

    # ------------------------------------------------------------------
    # noun_verb_sentence — nouns/names ↔ verbs, both directions
    # ------------------------------------------------------------------
    for etype, occ_tbl, vtbl, occ_pk, vid, vcol in [
        ("noun", "src.noun_occurrences", "src.nouns", "nounOccurrenceID", "nounID", "noun"),
        ("name", "src.name_occurrences", "src.names", "nameOccurrenceID", "nameID", "name"),
    ]:
        cooc(f"Sentence: {etype}→verb", f"""
            SELECT e.{vcol},'{etype}',v.verb,'verb',COUNT(*)*{_S}
            FROM src.noun_verb_sentence nvs
            JOIN {occ_tbl} o  ON nvs.noun_occ_type='{etype}' AND nvs.noun_occ_id=o.{occ_pk}
            JOIN {vtbl} e     ON o.{vid}=e.{vid}
            JOIN src.verb_occurrences vo ON nvs.verb_occ_id=vo.verbOccurrenceID
            JOIN src.verbs v  ON vo.verbID=v.verbID
            GROUP BY o.{vid},vo.verbID""")
        cooc(f"Sentence: verb→{etype}", f"""
            SELECT v.verb,'verb',e.{vcol},'{etype}',COUNT(*)*{_S}
            FROM src.noun_verb_sentence nvs
            JOIN {occ_tbl} o  ON nvs.noun_occ_type='{etype}' AND nvs.noun_occ_id=o.{occ_pk}
            JOIN {vtbl} e     ON o.{vid}=e.{vid}
            JOIN src.verb_occurrences vo ON nvs.verb_occ_id=vo.verbOccurrenceID
            JOIN src.verbs v  ON vo.verbID=v.verbID
            GROUP BY vo.verbID,o.{vid}""")

    # ------------------------------------------------------------------
    # Word frequencies (for global cloud view)
    # ------------------------------------------------------------------
    for kind, occ_tbl, vocab_tbl, id_col, word_col in [
        ("noun", "src.noun_occurrences", "src.nouns", "nounID", "noun"),
        ("name", "src.name_occurrences", "src.names", "nameID", "name"),
        ("verb", "src.verb_occurrences", "src.verbs", "verbID", "verb"),
    ]:
        freq(f"Frequencies: {kind}s", f"""
            SELECT w.{word_col},'{kind}',COUNT(*)
            FROM {occ_tbl} o JOIN {vocab_tbl} w USING({id_col})
            GROUP BY o.{id_col}""")

    return steps
