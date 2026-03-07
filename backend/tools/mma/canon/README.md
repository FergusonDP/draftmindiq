# DraftMindIQ — MMA Canon DB "Beast" Build

This folder gives you a **single canonical** MMA SQLite database built from:

1) UFCStats-style CSVs you already have (fighters/events/fights/fighter_stats/features)
2) Sports-Statistics "marts" DB you already scraped (round-by-round + breakdowns)

Output DB (default):
- backend/data/mma_canon.sqlite

## What you get

### Dimensions
- dim_fighter
- dim_event
- dim_fight

### Facts
- fact_fighter_fight_stats   (2 rows per fight)
- fact_fighter_round_stats   (round-by-round; long + wide core pivot)

### Modeling marts
- mart_fighter_round_splits
- mart_fighter_round_trends
- mart_fighter_rollups

### Alias + mapping
- mma_name_aliases (your manual overrides)
- ss_fighter_map   (how SS fighters map to canonical fighter_id)

## One command build (PowerShell, from backend/)
```powershell
python tools/mma/canon/build_beast.py `
  --out data/mma_canon.sqlite `
  --fighters_csv tools/mma/archives/data/fighters.csv `
  --events_csv tools/mma/archives/data/events.csv `
  --fights_csv tools/mma/archives/data/event_data.csv `
  --fighter_stats_csv tools/mma/archives/data/fighter_stats.csv `
  --features_v1_csv tools/mma/archives/data/feature_1.csv `
  --features_v2_csv tools/mma/archives/data/feature_2.csv `
  --features_v3_csv tools/mma/archives/data/feature_3.csv `
  --aliases_csv tools/mma/archives/data/aliases.csv `
  --ss_db data/marts/mma_historical_ss_full.sqlite
```

## If SS DB is large
You can build just dims + UFCStats facts first:
```powershell
python tools/mma/canon/build_beast.py --skip_ss_rounds ...
```

## Inspect what was built
```powershell
python tools/mma/canon/inspect_db.py --db data/mma_canon.sqlite
```

## Build alias suggestions for missing fighters (from SS -> canonical)
```powershell
python tools/mma/canon/suggest_aliases.py --db data/mma_canon.sqlite --limit 200
```
