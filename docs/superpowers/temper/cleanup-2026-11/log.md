# cleanup-2026-11: log

## 2026-11-05: launch

- Base `main` 5e32265. Epic branch `epic/cleanup-2026-11`.
- No build stage: a stabilisation pass, so the run starts at stage 4 (`temper:stabilise`), then stage 5.
- Build host beast; every gate runs as a jobq job (`~/agents/CLAUDE.md`, 31 Oct): agents submit with `jobq submit` and wait with `jobq wait`.
- Overnight: `overnight_fix_default` critical+important; severe items and owner items go to the morning report (`severe_contact: report`).
