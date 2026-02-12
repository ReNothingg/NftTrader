# Служба портала

Расположение: `src/services/portal`

## Точки входа

- Модуль: `python -m src.services.portal.sniper`.
- Корневая обертка: `python portal_sniper.py`

## Конфигурация

- Стратегия: `src/services/portal/config/strategy.json`.
- Auth: `auth.txt` в корне проекта (или `PORTAL_AUTH` env)

## Примечания

Portal на данный момент является единственной живой реализацией снайпера.
