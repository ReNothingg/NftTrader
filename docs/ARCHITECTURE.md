# Multi-Market Sniper Architecture

## Layout

```text
.
├── main.py                          # root wrapper -> src.launcher.manager
├── configs/
│   └── snipers.json                # launcher process config
├── docs/
│   ├── ARCHITECTURE.md
│   └── markets/
│       ├── portal.md
│       ├── tonnel.md
│       └── mrkt.md
├── src/
│   ├── launcher/
│   │   └── manager.py              # process orchestrator
│   └── services/
│       ├── portal/
│       │   ├── sniper.py
│       │   ├── README.md
│       │   └── config/
│       │       └── strategy.json
│       ├── tonnel/
│       │   ├── sniper.py
│       │   ├── README.md
│       │   └── config/
│       │       └── settings.json
│       └── mrkt/
│           ├── sniper.py
│           ├── README.md
│           └── config/
│               └── settings.json
├── portal_sniper.py                # compatibility wrapper
├── tonnel_sniper.py                # compatibility wrapper
└── mrkt_sniper.py                  # compatibility wrapper
```

## Runtime model

- One process per market.
- `src/launcher/manager.py` starts all enabled markets from `configs/snipers.json`.
- Crash isolation + optional auto-restart per market.
- Unified prefixed logs: `[market_name]`.

## Why this structure

- clear boundary per market (code/config/docs)
- easy onboarding for new market module
- independent deployment/testing of each service
- minimal coupling between market implementations

## Add new market

1. Create `src/services/<market>/sniper.py`.
2. Add `src/services/<market>/config/*` and `README.md`.
3. Add market entry to `configs/snipers.json`.
4. Optionally add root wrapper `<market>_sniper.py`.
