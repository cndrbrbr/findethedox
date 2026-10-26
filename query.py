"""All database queries for findethedox."""
import sqlite3
from dataclasses import dataclass
from pathlib import Path, PureWindowsPath


@dataclass
class WordScore:
    word: str
    kind: str   # 'noun', 'name', 'verb' or 'code'
    score: float


@dataclass
class DocOccurrence:
    filename: str
    folderpath: str
    pagenumber: int
    position: int
    source_db: str = ""
    # Two-word search: pairs of the words in one sentence / one paragraph
    # (paragraph hits exclude pairs already counted as sentence hits)
    sentence_hits: int = 0
    paragraph_hits: int = 0


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
    ] + ([
        """SELECT d.filename, d.folderpath, o.pagenumber, o.position
           FROM code_occurrences o
           JOIN codes c USING(codeID)
           JOIN documents d USING(fileID)
           WHERE lower(c.code)=?
           ORDER BY d.filename, o.pagenumber""",
    ] if has_codes(conn) else []):
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
# Two-word search (document panel)
# ---------------------------------------------------------------------------

def has_codes(conn: sqlite3.Connection) -> bool:
    """True if the DB comes from an allmydox version that stores codes."""
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='code_occurrences'"
    ).fetchone() is not None


_CODE_OCCS = """
    UNION ALL
    SELECT 'code', codeOccurrenceID, fileID, pagenumber, position
      FROM code_occurrences
     WHERE codeID IN (SELECT codeID FROM codes WHERE lower(code) = :{w})"""

# Noun/name occurrences of :w as (t, id, f, p, pos); codes are appended
# with _CODE_OCCS when the DB has them
_ENTITY_OCCS = """
    SELECT 'noun', nounOccurrenceID, fileID, pagenumber, position
      FROM noun_occurrences
     WHERE nounID IN (SELECT nounID FROM nouns WHERE lower(noun) = :{w})
    UNION ALL
    SELECT 'name', nameOccurrenceID, fileID, pagenumber, position
      FROM name_occurrences
     WHERE nameID IN (SELECT nameID FROM names WHERE lower(name) = :{w})"""

_VERB_OCCS = """
    SELECT verbOccurrenceID FROM verb_occurrences
     WHERE verbID IN (SELECT verbID FROM verbs WHERE lower(verb) = :{w})"""


def _occs(w: str, codes: bool) -> str:
    return _ENTITY_OCCS.format(w=w) + (_CODE_OCCS.format(w=w) if codes else "")


def _entity_pairs(conn, table: str, w1: str, w2: str) -> list[tuple]:
    """(fileID, page, pos, pair_key) for every row of a noun/name pair table
    (noun_sentence or noun_paragraph) linking w1 and w2, in either order.
    CROSS JOIN fixes the join order: w1's occurrences → pair table via the
    occ1/occ2 index → w2's occurrences (materialized once, auto-indexed)."""
    rows = []
    for here, there in (("occ1", "occ2"), ("occ2", "occ1")):
        rows += conn.execute(f"""
            WITH a(t, id, f, p, pos) AS MATERIALIZED ({_occs("w1", False)}),
                 b(t, id, f, p, pos) AS MATERIALIZED ({_occs("w2", False)})
            SELECT a.f, a.p, a.pos, c.occ1_type, c.occ1_id, c.occ2_type, c.occ2_id
              FROM a CROSS JOIN {table} c CROSS JOIN b
                ON c.{here}_type = a.t AND c.{here}_id = a.id
               AND b.t = c.{there}_type AND b.id = c.{there}_id
        """, {"w1": w1, "w2": w2}).fetchall()
    return [(f, p, pos, frozenset([(t1, i1), (t2, i2)]))
            for f, p, pos, t1, i1, t2, i2 in rows]


def _verb_pairs(conn, w_entity: str, w_verb: str) -> list[tuple]:
    """(fileID, page, pos, pair_key) for noun/name w_entity and verb w_verb
    in one sentence (allmydox stores verbs only at sentence level)."""
    rows = conn.execute(f"""
        WITH a(t, id, f, p, pos) AS MATERIALIZED ({_occs("we", False)}),
             v(id)               AS MATERIALIZED ({_VERB_OCCS.format(w="wv")})
        SELECT a.f, a.p, a.pos, a.t, a.id, nvs.verb_occ_id
          FROM a CROSS JOIN noun_verb_sentence nvs CROSS JOIN v
            ON nvs.noun_occ_type = a.t AND nvs.noun_occ_id = a.id
           AND v.id = nvs.verb_occ_id
    """, {"we": w_entity, "wv": w_verb}).fetchall()
    return [(f, p, pos, frozenset([(t, i), ("verb", v)]))
            for f, p, pos, t, i, v in rows]


def _code_pairs(conn, table: str, w1: str, w2: str) -> list[tuple]:
    """(fileID, page, pos, pair_key) for rows of code_sentence / code_paragraph
    linking w1 and w2.  Each row is (code_occ_id, occ_type, occ_id); the code
    side can be either word, so both join directions are queried."""
    occs = f"""a(t, id, f, p, pos) AS MATERIALIZED ({_occs("w1", True)}),
               b(t, id, f, p, pos) AS MATERIALIZED ({_occs("w2", True)})"""
    rows = conn.execute(f"""
        WITH {occs}
        SELECT a.f, a.p, a.pos, c.code_occ_id, c.occ_type, c.occ_id
          FROM a CROSS JOIN {table} c CROSS JOIN b
            ON a.t = 'code' AND c.code_occ_id = a.id
           AND b.t = c.occ_type AND b.id = c.occ_id
    """, {"w1": w1, "w2": w2}).fetchall()
    rows += conn.execute(f"""
        WITH {occs}
        SELECT a.f, a.p, a.pos, c.code_occ_id, c.occ_type, c.occ_id
          FROM a CROSS JOIN {table} c CROSS JOIN b
            ON c.occ_type = a.t AND c.occ_id = a.id
           AND b.t = 'code' AND b.id = c.code_occ_id
    """, {"w1": w1, "w2": w2}).fetchall()
    return [(f, p, pos, frozenset([("code", ci), (ot, oi)]))
            for f, p, pos, ci, ot, oi in rows]


def pair_occurrences(conn: sqlite3.Connection, word1: str, word2: str) -> list[DocOccurrence]:
    """Documents in which word1 and word2 occur together in one sentence or
    one paragraph, one entry per document, most sentence hits first.

    pagenumber/position point at the first hit.  Sentence pairs come from
    noun_sentence and noun_verb_sentence, paragraph pairs from noun_paragraph
    (which also contains same-sentence pairs, so those are subtracted).
    Codes (numbers, IDs) use code_sentence / code_paragraph when present.
    Two verbs cannot be matched: allmydox stores no verb–verb pairs."""
    w1, w2 = word1.lower(), word2.lower()

    sent = _entity_pairs(conn, "noun_sentence", w1, w2)
    sent += _verb_pairs(conn, w1, w2) + _verb_pairs(conn, w2, w1)
    para = _entity_pairs(conn, "noun_paragraph", w1, w2)
    if has_codes(conn):
        sent += _code_pairs(conn, "code_sentence", w1, w2)
        para += _code_pairs(conn, "code_paragraph", w1, w2)

    sent_keys = {k for *_, k in sent}
    # file → [sentence pairs, paragraph-only pairs, (page, pos) of first hit]
    per_file: dict[int, list] = {}
    for rows, idx in ((sent, 0), (para, 1)):
        for f, p, pos, key in rows:
            if idx == 1 and key in sent_keys:
                continue
            entry = per_file.setdefault(f, [set(), set(), (p, pos)])
            entry[idx].add(key)
            entry[2] = min(entry[2], (p, pos))

    if not per_file:
        return []
    ids = list(per_file)
    docs = {}
    for i in range(0, len(ids), 500):   # stay below SQLite's variable limit
        chunk = ids[i:i + 500]
        docs.update({r[0]: (r[1], r[2]) for r in conn.execute(
            f"SELECT fileID, filename, folderpath FROM documents "
            f"WHERE fileID IN ({','.join('?' * len(chunk))})", chunk)})

    results = [
        DocOccurrence(docs[f][0], docs[f][1], first[0], first[1],
                      sentence_hits=len(s_keys), paragraph_hits=len(p_keys))
        for f, (s_keys, p_keys, first) in per_file.items() if f in docs
    ]
    results.sort(key=lambda d: (-d.sentence_hits, -d.paragraph_hits, d.filename))
    return results


# ---------------------------------------------------------------------------
# Windows drive mapping
# ---------------------------------------------------------------------------

def share_root(db_path: str) -> str:
    """Network share or drive of the stored document paths, e.g. '\\\\server\\share'."""
    try:
        conn = sqlite3.connect(f"{Path(db_path).absolute().as_uri()}?mode=ro", uri=True)
        row = conn.execute("SELECT folderpath FROM documents LIMIT 1").fetchone()
        conn.close()
    except Exception:
        return ""
    return PureWindowsPath(row[0]).drive if row and row[0] else ""


def map_to_drive(folderpath: str, drive: str) -> str:
    """Replace the share (\\\\server\\share) or drive of a stored Windows path
    with drive, e.g. \\\\server\\share\\docs\\x → S:\\docs\\x."""
    p = PureWindowsPath(folderpath)
    if not p.drive:
        return folderpath
    return str(PureWindowsPath(drive + "\\") / p.relative_to(p.anchor))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _scalar(conn, sql: str, *args):
    row = conn.execute(sql, args).fetchone()
    return row[0] if row else None
