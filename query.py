"""All database queries for findethedox."""
import sqlite3
from dataclasses import dataclass
from pathlib import Path


@dataclass
class WordScore:
    word: str
    kind: str   # 'noun', 'name', or 'verb'
    score: float


@dataclass
class DocOccurrence:
    filename: str
    folderpath: str
    pagenumber: int
    position: int


def connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def ensure_indexes(conn: sqlite3.Connection):
    """
    Create indexes on the co-occurrence tables if they don't exist yet.
    These are not created by allmydox but are required for sub-second query
    performance on the 48M-row noun_sentence / noun_paragraph tables.
    """
    conn.executescript("""
        CREATE INDEX IF NOT EXISTS idx_noun_occ_noun
            ON noun_occurrences(nounID);
        CREATE INDEX IF NOT EXISTS idx_name_occ_name
            ON name_occurrences(nameID);
        CREATE INDEX IF NOT EXISTS idx_verb_occ_verb
            ON verb_occurrences(verbID);

        CREATE INDEX IF NOT EXISTS idx_noun_sent_occ1
            ON noun_sentence(occ1_type, occ1_id);
        CREATE INDEX IF NOT EXISTS idx_noun_sent_occ2
            ON noun_sentence(occ2_type, occ2_id);

        CREATE INDEX IF NOT EXISTS idx_noun_para_occ1
            ON noun_paragraph(occ1_type, occ1_id);
        CREATE INDEX IF NOT EXISTS idx_noun_para_occ2
            ON noun_paragraph(occ2_type, occ2_id);

        CREATE INDEX IF NOT EXISTS idx_nvs_noun_occ
            ON noun_verb_sentence(noun_occ_type, noun_occ_id);
        CREATE INDEX IF NOT EXISTS idx_nvs_verb_occ
            ON noun_verb_sentence(verb_occ_id);
    """)
    conn.commit()


# ---------------------------------------------------------------------------
# Global word frequencies (shown when no search term is entered)
# ---------------------------------------------------------------------------

def global_frequencies(conn: sqlite3.Connection) -> list[WordScore]:
    """Return the most frequent nouns, names, and verbs across all documents."""
    results: list[WordScore] = []
    for kind, occ_table, vocab_table, id_col, word_col in [
        ("noun", "noun_occurrences", "nouns", "nounID", "noun"),
        ("name", "name_occurrences", "names", "nameID", "name"),
        ("verb", "verb_occurrences", "verbs", "verbID", "verb"),
    ]:
        rows = conn.execute(f"""
            SELECT w.{word_col}, COUNT(*) AS cnt
            FROM {occ_table} o
            JOIN {vocab_table} w USING ({id_col})
            GROUP BY o.{id_col}
            ORDER BY cnt DESC
            LIMIT 150
        """).fetchall()
        results.extend(WordScore(r[0], kind, float(r[1])) for r in rows)
    return results


# ---------------------------------------------------------------------------
# Co-occurrence search
# ---------------------------------------------------------------------------

_SENT_WEIGHT = 1.3
_PARA_WEIGHT = 1.0


def cooccurrences(conn: sqlite3.Connection, search: str) -> list[WordScore]:
    """
    Return all words that co-occur with `search` in sentences or paragraphs.
    Score = sentence_count * 1.3 + paragraph_count.
    Uses JOIN queries so the full work is done inside SQLite with index support.
    """
    s = search.lower()
    scores: dict[tuple[str, str], float] = {}

    def acc(word: str, kind: str, cnt: int, weight: float):
        if word.lower() != s:
            key = (word, kind)
            scores[key] = scores.get(key, 0.0) + cnt * weight

    noun_id = _scalar(conn, "SELECT nounID FROM nouns WHERE lower(noun)=?", s)
    name_id = _scalar(conn, "SELECT nameID FROM names WHERE lower(name)=?", s)
    verb_id = _scalar(conn, "SELECT verbID FROM verbs WHERE lower(verb)=?", s)

    if not any([noun_id, name_id, verb_id]):
        return []

    # For each type the search word belongs to, pull sentence + paragraph partners
    for src_type, src_id in [("noun", noun_id), ("name", name_id)]:
        if src_id is None:
            continue
        occ_tbl = "noun_occurrences" if src_type == "noun" else "name_occurrences"
        occ_pk  = "nounOccurrenceID"  if src_type == "noun" else "nameOccurrenceID"
        vocab_id = "nounID"           if src_type == "noun" else "nameID"

        for cooc_tbl, weight in [("noun_sentence", _SENT_WEIGHT), ("noun_paragraph", _PARA_WEIGHT)]:
            # ---- partner is a noun ----
            for rows in [
                conn.execute(f"""
                    SELECT n2.noun, COUNT(*) FROM {cooc_tbl} c
                    JOIN {occ_tbl} o ON c.occ1_type=? AND c.occ1_id=o.{occ_pk} AND o.{vocab_id}=?
                    JOIN noun_occurrences o2 ON c.occ2_type='noun' AND c.occ2_id=o2.nounOccurrenceID
                    JOIN nouns n2 ON o2.nounID=n2.nounID
                    GROUP BY o2.nounID
                """, (src_type, src_id)).fetchall(),
                conn.execute(f"""
                    SELECT n2.noun, COUNT(*) FROM {cooc_tbl} c
                    JOIN {occ_tbl} o ON c.occ2_type=? AND c.occ2_id=o.{occ_pk} AND o.{vocab_id}=?
                    JOIN noun_occurrences o1 ON c.occ1_type='noun' AND c.occ1_id=o1.nounOccurrenceID
                    JOIN nouns n2 ON o1.nounID=n2.nounID
                    GROUP BY o1.nounID
                """, (src_type, src_id)).fetchall(),
            ]:
                for word, cnt in rows:
                    acc(word, "noun", cnt, weight)

            # ---- partner is a name ----
            for rows in [
                conn.execute(f"""
                    SELECT na2.name, COUNT(*) FROM {cooc_tbl} c
                    JOIN {occ_tbl} o ON c.occ1_type=? AND c.occ1_id=o.{occ_pk} AND o.{vocab_id}=?
                    JOIN name_occurrences na2o ON c.occ2_type='name' AND c.occ2_id=na2o.nameOccurrenceID
                    JOIN names na2 ON na2o.nameID=na2.nameID
                    GROUP BY na2o.nameID
                """, (src_type, src_id)).fetchall(),
                conn.execute(f"""
                    SELECT na2.name, COUNT(*) FROM {cooc_tbl} c
                    JOIN {occ_tbl} o ON c.occ2_type=? AND c.occ2_id=o.{occ_pk} AND o.{vocab_id}=?
                    JOIN name_occurrences na1o ON c.occ1_type='name' AND c.occ1_id=na1o.nameOccurrenceID
                    JOIN names na2 ON na1o.nameID=na2.nameID
                    GROUP BY na1o.nameID
                """, (src_type, src_id)).fetchall(),
            ]:
                for word, cnt in rows:
                    acc(word, "name", cnt, weight)

        # ---- partner is a verb (only in noun_verb_sentence) ----
        rows = conn.execute(f"""
            SELECT v.verb, COUNT(*) FROM noun_verb_sentence nvs
            JOIN {occ_tbl} o ON nvs.noun_occ_type=? AND nvs.noun_occ_id=o.{occ_pk} AND o.{vocab_id}=?
            JOIN verb_occurrences vo ON nvs.verb_occ_id=vo.verbOccurrenceID
            JOIN verbs v ON vo.verbID=v.verbID
            GROUP BY vo.verbID
        """, (src_type, src_id)).fetchall()
        for word, cnt in rows:
            acc(word, "verb", cnt, _SENT_WEIGHT)

    # If search term is a verb: find co-occurring nouns and names
    if verb_id is not None:
        rows = conn.execute("""
            SELECT n.noun, COUNT(*) FROM noun_verb_sentence nvs
            JOIN verb_occurrences vo ON nvs.verb_occ_id=vo.verbOccurrenceID AND vo.verbID=?
            JOIN noun_occurrences o ON nvs.noun_occ_type='noun' AND nvs.noun_occ_id=o.nounOccurrenceID
            JOIN nouns n ON o.nounID=n.nounID
            GROUP BY o.nounID
        """, (verb_id,)).fetchall()
        for word, cnt in rows:
            acc(word, "noun", cnt, _SENT_WEIGHT)

        rows = conn.execute("""
            SELECT na.name, COUNT(*) FROM noun_verb_sentence nvs
            JOIN verb_occurrences vo ON nvs.verb_occ_id=vo.verbOccurrenceID AND vo.verbID=?
            JOIN name_occurrences o ON nvs.noun_occ_type='name' AND nvs.noun_occ_id=o.nameOccurrenceID
            JOIN names na ON o.nameID=na.nameID
            GROUP BY o.nameID
        """, (verb_id,)).fetchall()
        for word, cnt in rows:
            acc(word, "name", cnt, _SENT_WEIGHT)

    return [WordScore(w, k, s_) for (w, k), s_ in scores.items()]


# ---------------------------------------------------------------------------
# Document occurrences (for right-click panel)
# ---------------------------------------------------------------------------

def document_occurrences(conn: sqlite3.Connection, word: str) -> list[DocOccurrence]:
    """Return one entry per (document, page) where `word` occurs."""
    s = search_lower = word.lower()
    results: list[DocOccurrence] = []

    for sql in [
        """SELECT d.filename, d.folderpath, o.pagenumber, o.position
           FROM noun_occurrences o
           JOIN nouns n USING(nounID)
           JOIN documents d USING(fileID)
           WHERE lower(n.noun)=?
           ORDER BY d.filename, o.pagenumber""",
        """SELECT d.filename, d.folderpath, o.pagenumber, o.position
           FROM name_occurrences o
           JOIN names n USING(nameID)
           JOIN documents d USING(fileID)
           WHERE lower(n.name)=?
           ORDER BY d.filename, o.pagenumber""",
        """SELECT d.filename, d.folderpath, o.pagenumber, o.position
           FROM verb_occurrences o
           JOIN verbs v USING(verbID)
           JOIN documents d USING(fileID)
           WHERE lower(v.verb)=?
           ORDER BY d.filename, o.pagenumber""",
    ]:
        for r in conn.execute(sql, (s,)).fetchall():
            results.append(DocOccurrence(r[0], r[1], r[2], r[3]))

    # One entry per (file, page), keeping the first position
    seen: set[tuple] = set()
    unique: list[DocOccurrence] = []
    for d in sorted(results, key=lambda x: (x.filename, x.pagenumber)):
        key = (d.filename, d.folderpath, d.pagenumber)
        if key not in seen:
            seen.add(key)
            unique.append(d)
    return unique


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _scalar(conn, sql: str, *args):
    row = conn.execute(sql, args).fetchone()
    return row[0] if row else None
