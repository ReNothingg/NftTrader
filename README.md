# Многорыночные снайперы NFT

Сервис-ориентированная структура с одной пусковой установкой и отдельными рыночными модулями.

## Запуск всех рынков

``bash
python .\main.py

```

```

Конфиг пусковой установки: `configs/snipers.json`

## Запуск одиночного рынка

``bash
python .\portal_sniper.py
python .\tonnel_sniper.py
python .\mrkt_sniper.py

```

Или в виде модуля:

``bash
python -m src.services.portal.sniper
python -m src.services.tonnel.sniper --mode mock
python -m src.services.mrkt.sniper --mode mock
```

## Структура

- `src/launcher/manager.py` - менеджер процессов.
- `src/services/portal` - реализация портала.
- `src/services/tonnel` - скелет тоннеля.
- `src/services/mrkt` - скелет mrkt.
- `configs/snipers.json` - включенные рынки + команды + политика перезапуска.
- `docs/ARCHITECTURE.md` - детали архитектуры.

## Portal auth

Поместите значение заголовка auth в `auth.txt` (одна строка) или используйте env `PORTAL_AUTH`.

## Примечания

- Портал - это производственная логика на данный момент.
- Tonnel/MRKT - это подмостки, готовые к интеграции с API.
