import re
import pandas as pd


def _slug(s: str) -> str:
    s = re.sub(r"[^A-Za-z0-9]+", "_", str(s).strip())
    return re.sub(r"_+", "_", s).strip("_").lower()


def _parse_game_info(gi: str):
    """
    Handles common DK MMA formats:
      - "Fighter A vs Fighter B"
      - "FighterA@FighterB 03/07/2026 07:40PM ET"
      - "Fighter A @ Fighter B (03/07/2026 07:40PM ET)"
      - Variants with extra spacing / parentheses
    Returns: (name1, name2, time_tag)
    """
    s = str(gi).strip()

    # Pull time-ish tag if present (keeps fight_id stable per slate)
    # Examples: "03/07/2026 07:40PM ET"
    m_time = re.search(r"(\d{2}/\d{2}/\d{4}\s+\d{1,2}:\d{2}[AP]M\s+ET)", s)
    time_tag = _slug(m_time.group(1)) if m_time else ""

    # Remove time and parentheses for easier name parsing
    s_clean = re.sub(r"\(.*?\)", " ", s)
    s_clean = re.sub(r"\d{2}/\d{2}/\d{4}\s+\d{1,2}:\d{2}[AP]M\s+ET", " ", s_clean)
    s_clean = re.sub(r"\s+", " ", s_clean).strip()

    if " vs " in s_clean.lower():
        parts = re.split(r"\s+vs\s+", s_clean, flags=re.IGNORECASE, maxsplit=1)
        if len(parts) == 2:
            return parts[0].strip(), parts[1].strip(), time_tag

    if " @ " in s_clean:
        a, b = [x.strip() for x in s_clean.split(" @ ", 1)]
        return a, b, time_tag

    if "@" in s_clean:
        a, b = [x.strip() for x in s_clean.split("@", 1)]
        return a, b, time_tag

    # Fallback: no parse; treat entire string as label
    return s_clean, "", time_tag


def load_dk_mma_slate(csv_path: str):
    df = pd.read_csv(csv_path)

    df["ID"] = df["ID"].astype(str)
    df["Salary"] = df["Salary"].astype(int)
    df["Name"] = df["Name"].astype(str)
    df["TeamAbbrev"] = df["TeamAbbrev"].astype(str)
    df["Game Info"] = df["Game Info"].astype(str)
    df["AvgPointsPerGame"] = pd.to_numeric(df["AvgPointsPerGame"], errors="coerce").fillna(0.0)

    fighters = []
    fights = {}

    for _, r in df.iterrows():
        gi = r["Game Info"]
        a, b, time_tag = _parse_game_info(gi)

        a_slug, b_slug = _slug(a), _slug(b) if b else ""
        pair = sorted([p for p in [a_slug, b_slug] if p])
        if len(pair) == 2:
            fight_core = f"{pair[0]}__vs__{pair[1]}"
        else:
            fight_core = _slug(gi)

        fight_id = f"{fight_core}__{time_tag}" if time_tag else fight_core

        row = {
            "player_id": r["ID"],
            "name": r["Name"].strip(),
            "salary": int(r["Salary"]),
            "fight_id": fight_id,
            "team": r["TeamAbbrev"].strip(),
            "avg_points": float(r["AvgPointsPerGame"]),
            "game_info": gi,
        }
        fighters.append(row)
        fights.setdefault(fight_id, []).append(r["ID"])

    return {
        "ok": True,
        "fighters": fighters,
        "fights": fights,
        "meta": {"rows": len(fighters), "fights": len(fights)},
    }


def attach_temp_projection(fighters):
    out = []
    for f in fighters:
        base = float(f.get("avg_points", 0.0))
        if base <= 0:
            base = (int(f["salary"]) / 1000.0) * 6.5
        f2 = dict(f)
        f2["proj"] = round(base, 2)
        out.append(f2)
    return out
