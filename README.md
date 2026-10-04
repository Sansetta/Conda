# Apify Store tracker → MySQL

Раз в день собирает Actor'ов из Apify Store, кладёт историю в MySQL, выделяет **ниши** (instagram, google maps…),
внутри каждой ниши — **услуги** (Post Scraper, Followers Scraper, Reviews Scraper…) и показывает
**топ-3 самых выгодных Actor'ов в каждой услуге**.

## Быстрый старт

```bash
cp .env.example .env            # впишите пароли
docker compose up -d mysql      # MySQL 8
pip install -r requirements.txt
export MYSQL_DSN="mysql://apify:change-me@192.168.1.45:3306/apify_tracker"

python -m tracker --demo 3000   # проверка без обхода Apify: 3000 синтетических Actor'ов
python -m tracker               # настоящий сбор (долго), схема создаётся сама
```
Всё в одном: `docker compose up -d` поднимает MySQL + ежедневный сбор в 03:00.

## Архитектура

```
Apify API ──► tracker (Collector, как раньше) ──► MySQL ──► ваш бэкенд/фронт (SELECT из v_*)
                    │                              ▲
                    └─► niches.py → services.py → scoring.py (пересчитываемый слой)
```

| Слой | Таблицы | Правило |
|---|---|---|
| Факты (история) | `runs`, `actors`, `actor_snapshots`, `categories`, `actor_categories` | пишутся один раз за прогон, не пересчитываются |
| Справочники | `niches`, `services` | ключ ниши = slug, услуги уникальны в паре (ниша, service_key) |
| Производные | `actor_metrics`, `actor_services`, `service_top`, `niche_stats`, `service_stats` | целиком перестраиваются командой `--rescore`, без повторного обхода Apify |
| Витрина для фронта | представления `v_*` | фронт читает только их |

Ключевые решения:

* **Атомарная публикация.** Сырой снимок и расчёт — разные транзакции. Снимок становится видимым фронту только после
  заполнения `runs.scored_at`; если расчёт упал, фронт продолжает видеть вчерашний полный снимок.
* **Пересчёт без обхода.** Поменяли веса, словарь услуг или `--niches-file` → `python -m tracker --rescore` (секунды).
  Параметры расчёта сохраняются в `runs.config` (JSON).
* **История.** `actor_snapshots` (пользователи/оценки/цены по дням) и `service_top` (кто был в топе каждый день).
  `--keep-days 365` чистит старое (каскадно).
* **Витрина.** `v_service_top_latest` — плоская таблица «ниша → услуга → место → Actor + метрики», `v_niche_overview_latest`,
  `v_service_overview_latest`, `v_actors_latest`, `v_actor_services`, `v_latest_run`. Если сменится схема таблиц,
  фронт и API не ломаются, пока представления сохраняют колонки.

## Как определяются услуги

1. Из текста «title + slug» вырезается сама ниша (иначе «instagram» в нише instagram стал бы услугой).
2. Текст сверяется со справочником `SERVICE_TYPES` (`tracker/services.py`): followers, comments, reviews, likes, posts,
   videos, stories, hashtags, profiles, search, places, products, prices, jobs, contacts, ads, transcripts, downloader, images.
   Actor может попасть в несколько услуг («Profile & Post Scraper»).
3. **Автоподбор** для услуг, которых нет в справочнике: слово, которое встречается в названиях ≥ `--min-discovered-service`
   Actor'ов ниши (и ≥ 2% ниши), становится услугой (`Instagram Location Scraper`). Выключается `--no-auto-services`.
4. Услуга, где меньше `--min-service-actors` (3) Actor'ов, не считается самостоятельной.
5. Всё остальное — универсальная услуга `general` («Instagram Scraper (all-in-one)»).

Правки без изменения кода — `--niches-file niches.example.json` (свои ниши, исключения, свои услуги и regex).

## «Условная выгода» внутри услуги

Формула прежняя: `Value score = 100 × (45% цена + 20% пользователи + 20% оценка + 15% число оценок)`, цена приводится
к «$ за 1000 результатов». Что изменилось: нормировка идёт **внутри услуги** (сравнивать цену followers-скрейпера
с post-скрейпером некорректно). Если в услуге подходящих Actor'ов меньше `--min-service-pool` (5), шкалы берутся
по всей нише, иначе на 1–3 Actor'ах нормировка вырождается. Какой пул использован, видно в `service_top.score_scope`.
Из каждой услуги берётся максимум `--per-service` (3) лучших. Score относителен внутри пула и между услугами не сравнивается.

## Чтение данных (интерфейс для фронта)

Читайте только представления `v_*`: они всегда указывают на последний полностью посчитанный снимок.

| Представление | Содержимое |
|---|---|
| `v_niche_overview_latest` | ниши: число Actor'ов и услуг, пользователи, медианная цена |
| `v_service_overview_latest` | услуги ниши + лидер (rank 1) |
| `v_service_top_latest` | **главное**: ниша → услуга → место (топ-3) → Actor и все метрики |
| `v_actors_latest` | все Actor'ы последнего снимка |
| `v_actor_services` | в какие ниши/услуги входит Actor |
| `v_latest_run` | дата и объём последнего снимка |

История: `actor_snapshots` (по дням) и `service_top` (кто был в топе каждый день), связь через `runs.snapshot_date`.
Пользователю фронта достаточно прав только на чтение представлений, например:
`GRANT SELECT ON apify_tracker.v_service_top_latest TO 'front'@'%';` (по одному на каждое нужное представление).


```sql
SELECT service_display_name, rank_no, actor_title, value_score, cost_1000, total_users, rating
FROM v_service_top_latest WHERE niche_slug = 'instagram' ORDER BY service_id, rank_no;
```

## Ограничения

* Оценка `$ / 1000` для pay-per-event и аренды — допущение (см. `--compute-cost`, `--monthly-results`).
* Рейтинги считаются только по Actor'ам с известной ценой и ≥ `--min-users` пользователей.
* Классификация услуг по названиям: часть Actor'ов с «нестандартными» заголовками попадёт в `general`.
  Для точной настройки смотрите `v_service_overview_latest` и дополняйте словарь через `--niches-file`.

## Рерайт услуг для продажи (`tracker/content.py`)

Для каждой услуги каждой ниши («Instagram Post Scraper», «Instagram Followers Scraper»…) строится собственная карточка
в структуре страницы Apify Store: tagline, короткое описание, «что делает», возможности, сценарии, шаги, вход/выход, FAQ, SEO.

```bash
export ANTHROPIC_API_KEY=...                       # для LLM-рерайта
python -m tracker --rewrite-probe apify/instagram-scraper     # один раз: что реально отдаёт API по описанию
python -m tracker --rewrite --rewrite-niche instagram --rewrite-limit 5     # пробный прогон
python -m tracker --rewrite                        # одна итерация: окно 3 часа, публикация раз в 15 минут
python -m tracker --rewrite --rewrite-provider template   # без ключа: проверка конвейера (однотипные тексты)
python -m tracker --flush-content                  # опубликовать то, что осталось в очереди после аварии
python -m tracker --daemon --with-rewrite          # ежедневно: сбор -> рейтинги -> рерайт
```

Как это работает:

1. Источники услуги: до `--rewrite-sources` (5) самых популярных Actor'ов услуги из последнего **опубликованного** снимка.
   Описания (`description`, `seoDescription`, фрагмент README) берутся из API Apify и кэшируются в `actor_details`
   (`--details-ttl-days`, 14). Только для Actor'ов в источниках - это сотни запросов, а не десятки тысяч.
2. `source_hash` = хэш исходных текстов + язык + провайдер + версия промпта. Не изменился хэш или услуге меньше
   `--rewrite-min-age-days` (7) - пропуск. Поэтому ежедневный прогон переписывает только то, что реально изменилось.
3. Проверка уникальности: доля 4-грамм карточки (всей и каждого абзаца отдельно), совпавших с исходниками, должна быть
   не выше `--rewrite-max-sim` (0.20). Иначе одна повторная попытка, затем статус `too_similar` (в базу не пишется).
   Результат пишется в `service_content.similarity`.
4. Цены и метрики рынка в карточку добавляет код (блок `market` из `service_stats`), модель их не пишет.
5. **Окно и накопление.** За одну итерацию (`--rewrite-window-hours`, 3) новые услуги запускаются, пока окно открыто.
   Каждый готовый рерайт сразу падает в `service_content_staging` (переживёт падение процесса), а в `service_content`
   переезжает пачкой раз в `--flush-minutes` (15) одной транзакцией + в конце окна и при сбое. Необработанный хвост
   возвращается в очередь следующего прогона. Фронт читает только `v_service_content_latest`.

```sql
SELECT niche_slug, service_display_name, tagline, content FROM v_service_content_latest WHERE niche_slug='instagram';
```

## Две итоговые таблицы для продажи

Ключ обеих - постоянный **Apify ID** актора (17 символов), а не `username/name`: при переименовании актор остаётся тем же.

| Таблица | Что хранит | Когда меняется |
|---|---|---|
| `actor_price_monitor` | **все** акторы Store (без фильтров, в т.ч. дорогие): оригинальное название, цена, оценка $/1000, `prev_cost_1000`, `price_changed_at`, `change_pct` (минус = скидка), `peak_cost_1000`, пользователи, рейтинг | каждый проход обхода: цена обновляется на месте |
| `actor_catalog` | **уникальные типы**: новое название, tagline, карточка (JSON), ниша, услуга. **Цен нет** | запись создаётся один раз, когда появился новый ID. Существующие ID не переписываются (`INSERT IGNORE`) |

Читать: `v_actor_catalog_latest` (рерайт + актуальная цена из мониторинга). Остальные таблицы (`actors`, `actor_snapshots`,
`service_top`, `service_content` ...) остаются внутренними: из них считаются ниши, услуги и рейтинги.

```bash
python -m tracker --probe-ids                       # один раз на живой сети: откуда реально берутся ID
python -m tracker --monitor-prices                  # один проход мониторинга цен (обход Store -> ID -> actor_price_monitor)
python -m tracker --monitor-prices --monitor-loop-hours 3     # то же каждые 3 часа
python -m tracker --catalog --catalog-scope top --rewrite-limit 5      # пробный рерайт новых ID
python -m tracker --catalog                         # одна итерация: окно 3 часа, запись пачкой раз в 15 минут
python -m tracker --daemon --with-catalog           # ежедневно: сбор -> рейтинги -> цены -> рерайт новых ID
docker compose --profile monitor up -d              # отдельный контейнер мониторинга цен раз в 3 часа
```

Как получаются ID (`tracker/ids.py`): поле `id` из Store API -> уже известное в `actor_price_monitor` -> JSON в коде страниц
`https://apify.com/store/categories` и страниц категорий -> `GET /v2/acts/{user}~{name}`. После первого прогона почти все ID
берутся из базы, новые акторы добираются точечно. HTML сайта отдаёт только часть Store, полное покрытие даёт API.

Область каталога `--catalog-scope`: `top` (акторы из топов услуг, по умолчанию), `eligible` (с известной ценой и
`--min-users`), `all` (вообще все; это тысячи LLM-вызовов). Рерайт идёт только для актора, у которого уже есть описание и
ниша/услуга из последнего снимка, поэтому совсем новый актор попадает в каталог после ближайшего ежедневного прогона.

Скидки: `SELECT title, prev_cost_1000, cost_1000, change_pct FROM actor_price_monitor WHERE price_changed_at > NOW() - INTERVAL 1 DAY AND change_pct <= -20;`
