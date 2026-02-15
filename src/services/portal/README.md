# Portals Service

## Entry points

- `python -m src.services.portal.sniper`
- `python portal_sniper.py`

## Architecture

- `src/services/portal/config_loader.py` - loading strategy/accounts/runtime config.
- `src/services/portal/client.py` - Portals HTTP client (offers/orders/listings/activity).
- `src/services/portal/strategy.py` - selectors, pricing, outbid/reprice logic.
- `src/services/portal/storage.py` - SQLite ledger for trades and PnL.
- `src/services/portal/telegram_bot.py` - Telegram bot on `aiogram`.
- `src/services/portal/engine.py` - multi-account orchestrator and infinite runtime loops.

## Config files

- Strategy: `src/services/portal/config/strategy.json`
- Accounts: `configs/portal_accounts.json`
- Legacy single-account auth fallback: `auth.txt` or `PORTAL_AUTH`
- Telegram token/chat ids can be passed from env: `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_IDS`, `TELEGRAM_ENABLED`

## CLI

```bash
pip install -r requirements.txt
python -m src.services.portal.sniper --strategy-file src/services/portal/config/strategy.json --accounts-file configs/portal_accounts.json --live
```
