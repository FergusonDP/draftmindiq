# backend/tools/mma/canon/suggest_aliases.py
from __future__ import annotations

import argparse
import csv
import re
import sqlite3
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from rapidfuzz import fuzz, process


_WS = re.compile(r"\s+")
_NONALNUM = re.compile(r"[^a-z0-9\s\-\']+")


def norm_name(s: str) -> str:
    s = (s or "").strip().lower()
    s = _NONALNUM.sub(" ", s)
    s = _WS.sub(" ", s).strip()
    return s


def tokens(s: str) -> List[str]:
    return [t for t in norm_name(s).split() if t]


@dataclass
class Candidate:
    fighter_id: str
    canonical_name: str
    canonical_norm: str
    score: int
    reason: str


def connect(db_path: str) -> sqlite3.Connection:
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    return con


def load_dim_fighters(con: sqlite3.Connection) -> List[sqlite3.Row]:
    return con.execute(
        """
        SELECT fighter_id, name, name_norm
        FROM dim_fighter
        ORDER BY name_norm, fighter_id
        """
    ).fetchall()


def load_unmapped_ss_names(con: sqlite3.Connection, min_count: int) -> List[Tuple[str, int]]:
    rows = con.execute(
        """
        SELECT ss_fighter_norm, COUNT(*) AS n
        FROM ss_fighter_map
        WHERE fighter_id IS NULL
          AND ss_fighter_norm IS NOT NULL
          AND TRIM(ss_fighter_norm) <> ''
        GROUP BY ss_fighter_norm
        HAVING COUNT(*) >= ?
        ORDER BY n DESC, ss_fighter_norm
        """,
        (min_count,),
    ).fetchall()
    return [(str(r["ss_fighter_norm"]), int(r["n"])) for r in rows]


def alias_exists(con: sqlite3.Connection, source: str, raw_name: str) -> bool:
    row = con.execute(
        """
        SELECT 1
        FROM mma_name_aliases
        WHERE source = ? AND raw_name = ?
        LIMIT 1
        """,
        (source, raw_name),
    ).fetchone()
    return row is not None


def build_name_maps(
    fighter_rows: Sequence[sqlite3.Row],
) -> Tuple[Dict[str, Tuple[str, str]], List[str]]:
    name_map: Dict[str, Tuple[str, str]] = {}
    for r in fighter_rows:
        name_norm = str(r["name_norm"])
        fighter_id = str(r["fighter_id"])
        name = str(r["name"])
        if name_norm and name_norm not in name_map:
            name_map[name_norm] = (fighter_id, name)
    return name_map, list(name_map.keys())


def top_fuzzy_candidates(
    query: str,
    name_map: Dict[str, Tuple[str, str]],
    choices: Sequence[str],
    limit: int = 8,
) -> List[Candidate]:
    hits = process.extract(query, choices, scorer=fuzz.token_sort_ratio, limit=limit) or []
    out: List[Candidate] = []
    for cand_norm, score, _ in hits:
        fighter_id, canonical_name = name_map[cand_norm]
        out.append(
            Candidate(
                fighter_id=fighter_id,
                canonical_name=canonical_name,
                canonical_norm=cand_norm,
                score=int(score),
                reason="fuzzy",
            )
        )
    return out


def married_name_candidates(
    query: str,
    fighter_rows: Sequence[sqlite3.Row],
    limit: int = 8,
) -> List[Candidate]:
    """
    Heuristic:
    - same first token
    - at least one overlapping non-trivial token, OR query canonical is prefix-like
    - useful for:
      michelle waterson -> michelle waterson gomez
      yana kunitskaya -> yana santos
      katlyn chookagian -> katlyn cerminara  (same first name only, will usually rely on fuzzy)
    """
    q_norm = norm_name(query)
    q_tokens = tokens(q_norm)
    if not q_tokens:
        return []

    q_first = q_tokens[0]
    q_rest = set(q_tokens[1:])

    out: List[Candidate] = []
    for r in fighter_rows:
        c_norm = str(r["name_norm"])
        c_tokens = tokens(c_norm)
        if not c_tokens:
            continue

        c_first = c_tokens[0]
        c_rest = set(c_tokens[1:])

        if q_first != c_first:
            continue

        overlap = len(q_rest & c_rest)

        # same first name plus any overlap is a strong sign
        if overlap > 0:
            score = 95 + min(overlap, 3)
            out.append(
                Candidate(
                    fighter_id=str(r["fighter_id"]),
                    canonical_name=str(r["name"]),
                    canonical_norm=c_norm,
                    score=score,
                    reason="same_first_plus_overlap",
                )
            )
            continue

        # same first name and canonical extends query, e.g. "michelle waterson" -> "michelle waterson gomez"
        if c_norm.startswith(q_norm + " "):
            out.append(
                Candidate(
                    fighter_id=str(r["fighter_id"]),
                    canonical_name=str(r["name"]),
                    canonical_norm=c_norm,
                    score=97,
                    reason="canonical_extends_query",
                )
            )
            continue

        # same first name and query extends canonical
        if q_norm.startswith(c_norm + " "):
            out.append(
                Candidate(
                    fighter_id=str(r["fighter_id"]),
                    canonical_name=str(r["name"]),
                    canonical_norm=c_norm,
                    score=94,
                    reason="query_extends_canonical",
                )
            )

    out.sort(key=lambda x: (-x.score, x.canonical_norm, x.fighter_id))
    deduped: List[Candidate] = []
    seen = set()
    for c in out:
        key = (c.fighter_id, c.canonical_norm)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(c)
    return deduped[:limit]


def same_firstname_candidates(
    query: str,
    fighter_rows: Sequence[sqlite3.Row],
    limit: int = 8,
) -> List[Candidate]:
    q_tokens = tokens(query)
    if not q_tokens:
        return []

    q_first = q_tokens[0]
    out: List[Candidate] = []

    for r in fighter_rows:
        c_norm = str(r["name_norm"])
        c_tokens = tokens(c_norm)
        if not c_tokens or c_tokens[0] != q_first:
            continue

        score = fuzz.token_sort_ratio(query, c_norm)
        if score >= 70:
            out.append(
                Candidate(
                    fighter_id=str(r["fighter_id"]),
                    canonical_name=str(r["name"]),
                    canonical_norm=c_norm,
                    score=int(score),
                    reason="same_firstname_fuzzy",
                )
            )

    out.sort(key=lambda x: (-x.score, x.canonical_norm, x.fighter_id))
    deduped: List[Candidate] = []
    seen = set()
    for c in out:
        key = (c.fighter_id, c.canonical_norm)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(c)
    return deduped[:limit]


def merge_candidates(*groups: Iterable[Candidate], limit: int = 8) -> List[Candidate]:
    best: Dict[Tuple[str, str], Candidate] = {}
    for group in groups:
        for c in group:
            key = (c.fighter_id, c.canonical_norm)
            prev = best.get(key)
            if prev is None or c.score > prev.score:
                best[key] = c

    out = list(best.values())
    out.sort(key=lambda x: (-x.score, x.reason, x.canonical_norm, x.fighter_id))
    return out[:limit]


def auto_pick(
    cands: Sequence[Candidate], auto_threshold: int, gap_threshold: int
) -> Optional[Candidate]:
    if not cands:
        return None
    top = cands[0]
    second_score = cands[1].score if len(cands) > 1 else -999

    if top.score >= auto_threshold and (top.score - second_score) >= gap_threshold:
        return top

    # extra-safe auto-pick for "canonical extends query"
    if top.reason in {"canonical_extends_query", "same_first_plus_overlap"} and top.score >= 95:
        return top

    return None


def main(argv: List[str]) -> int:
    ap = argparse.ArgumentParser(
        description="Suggest alias mappings for unmapped SS fighter names."
    )
    ap.add_argument("--db", default="data/beast.sqlite", help="Path to beast.sqlite")
    ap.add_argument(
        "--out",
        default="data/canon/alias_suggestions.csv",
        help="CSV output path",
    )
    ap.add_argument(
        "--source",
        default="ss",
        help="Alias source value to generate (default: ss)",
    )
    ap.add_argument(
        "--min-count",
        type=int,
        default=1,
        help="Only include unmapped SS names appearing at least this many times",
    )
    ap.add_argument(
        "--candidate-limit",
        type=int,
        default=5,
        help="Number of candidate columns to emit",
    )
    ap.add_argument(
        "--auto-threshold",
        type=int,
        default=93,
        help="Auto-pick threshold score",
    )
    ap.add_argument(
        "--gap-threshold",
        type=int,
        default=4,
        help="Minimum score gap over 2nd candidate for auto-pick",
    )
    args = ap.parse_args(argv)

    db_path = Path(args.db).resolve()
    out_path = Path(args.out).resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if not db_path.exists():
        print(f"ERROR: DB not found: {db_path}", file=sys.stderr)
        return 2

    con = connect(str(db_path))
    try:
        fighter_rows = load_dim_fighters(con)
        name_map, choices = build_name_maps(fighter_rows)
        unmapped = load_unmapped_ss_names(con, args.min_count)

        fieldnames = [
            "source",
            "raw_name",
            "count_in_ss_unmapped",
            "fighter_id",
            "canonical_name",
            "method",
            "confidence",
            "status",
        ] + [f"candidate_{i}" for i in range(1, args.candidate_limit + 1)]

        written = 0
        with out_path.open("w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()

            for raw_name, n in unmapped:
                # Skip if alias already exists
                if alias_exists(con, args.source, raw_name):
                    continue

                married = married_name_candidates(
                    raw_name, fighter_rows, limit=args.candidate_limit
                )
                same_first = same_firstname_candidates(
                    raw_name, fighter_rows, limit=args.candidate_limit
                )
                fuzzy = top_fuzzy_candidates(
                    raw_name, name_map, choices, limit=args.candidate_limit
                )

                candidates = merge_candidates(
                    married, same_first, fuzzy, limit=args.candidate_limit
                )
                picked = auto_pick(candidates, args.auto_threshold, args.gap_threshold)

                row = {
                    "source": args.source,
                    "raw_name": raw_name,
                    "count_in_ss_unmapped": n,
                    "fighter_id": picked.fighter_id if picked else "",
                    "canonical_name": picked.canonical_name if picked else "",
                    "method": picked.reason if picked else "suggest",
                    "confidence": picked.score if picked else "",
                    "status": "auto" if picked else "review",
                }

                for i in range(args.candidate_limit):
                    if i < len(candidates):
                        c = candidates[i]
                        row[f"candidate_{i+1}"] = (
                            f"{c.fighter_id} | {c.canonical_name} | {c.score} | {c.reason}"
                        )
                    else:
                        row[f"candidate_{i+1}"] = ""

                writer.writerow(row)
                written += 1

        print(f"Wrote: {out_path}")
        print(f"Rows: {written}")
        print("")
        print("Next steps:")
        print("1) Open the CSV and review rows with status=review")
        print("2) Keep/adjust fighter_id + canonical_name for correct matches")
        print("3) Feed the reviewed CSV into build_beast.py as --aliases_csv")
        return 0

    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
