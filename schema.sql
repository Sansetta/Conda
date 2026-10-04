-- Схема MySQL 8.0+ (совместима с MariaDB 10.5+). Применяется автоматически: python -m tracker --init-db
-- Принцип: «факты» (actors, actor_snapshots, categories) отделены от «производных» таблиц
-- (actor_metrics, service_top, *_stats, actor_services), которые целиком пересчитываются из фактов
-- (python -m tracker --rescore) без повторного обхода Apify.
-- Фронт читает ТОЛЬКО представления v_* (они всегда указывают на последний полностью посчитанный снимок).

CREATE TABLE IF NOT EXISTS runs (
    run_id        INT UNSIGNED NOT NULL AUTO_INCREMENT,
    snapshot_date DATE NOT NULL,
    collected     INT UNSIGNED NOT NULL DEFAULT 0,
    store_total   INT UNSIGNED NOT NULL DEFAULT 0,
    finished_at   DATETIME NULL COMMENT 'когда сохранён сырой снимок',
    scored_at     DATETIME NULL COMMENT 'когда посчитаны ниши/услуги/рейтинги, NULL = снимок ещё не опубликован',
    config        JSON NULL COMMENT 'параметры расчёта (веса, допущения) этого прогона',
    PRIMARY KEY (run_id),
    UNIQUE KEY uq_runs_date (snapshot_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS actors (
    actor_pk   INT UNSIGNED NOT NULL AUTO_INCREMENT,
    actor_key  VARCHAR(400) NOT NULL COMMENT 'username/name из Apify',
    username   VARCHAR(128) NOT NULL,
    name       VARCHAR(255) NOT NULL,
    title      VARCHAR(512) NOT NULL,
    url        VARCHAR(512) NOT NULL,
    first_seen DATE NOT NULL,
    last_seen  DATE NOT NULL,
    PRIMARY KEY (actor_pk),
    UNIQUE KEY uq_actor_key (actor_key),
    KEY idx_username (username),
    KEY idx_last_seen (last_seen),
    FULLTEXT KEY ft_actor_title (title, name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS categories (
    category_id SMALLINT UNSIGNED NOT NULL AUTO_INCREMENT,
    code        VARCHAR(64) NOT NULL,
    PRIMARY KEY (category_id),
    UNIQUE KEY uq_category_code (code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS actor_categories (
    actor_pk    INT UNSIGNED NOT NULL,
    category_id SMALLINT UNSIGNED NOT NULL,
    PRIMARY KEY (actor_pk, category_id),
    KEY idx_category (category_id),
    CONSTRAINT fk_ac_actor FOREIGN KEY (actor_pk) REFERENCES actors (actor_pk) ON DELETE CASCADE,
    CONSTRAINT fk_ac_category FOREIGN KEY (category_id) REFERENCES categories (category_id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Ежедневный снимок: история пользователей, оценок, цен
CREATE TABLE IF NOT EXISTS actor_snapshots (
    run_id            INT UNSIGNED NOT NULL,
    actor_pk          INT UNSIGNED NOT NULL,
    rating            DECIMAL(4,3) NULL,
    review_count      INT UNSIGNED NOT NULL DEFAULT 0,
    total_users       INT UNSIGNED NOT NULL DEFAULT 0,
    users_30d         INT UNSIGNED NOT NULL DEFAULT 0,
    total_runs        BIGINT UNSIGNED NOT NULL DEFAULT 0,
    pricing_model     VARCHAR(32) NULL,
    pricing_price_usd DECIMAL(16,8) NULL,
    pricing_unit      VARCHAR(64) NULL,
    pricing_trial_min INT NULL,
    pricing_summary   VARCHAR(1024) NULL,
    pricing_events    JSON NULL,
    PRIMARY KEY (run_id, actor_pk),
    KEY idx_actor_run (actor_pk, run_id),
    KEY idx_run_users (run_id, total_users),
    CONSTRAINT fk_as_run FOREIGN KEY (run_id) REFERENCES runs (run_id) ON DELETE CASCADE,
    CONSTRAINT fk_as_actor FOREIGN KEY (actor_pk) REFERENCES actors (actor_pk) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ---------------------------------------------------------------- производные таблицы
CREATE TABLE IF NOT EXISTS actor_metrics (
    run_id       INT UNSIGNED NOT NULL,
    actor_pk     INT UNSIGNED NOT NULL,
    cost_1000    DECIMAL(14,4) NULL COMMENT 'оценка: USD за 1000 результатов',
    cost_basis   VARCHAR(96) NULL COMMENT 'как получена оценка',
    bayes_rating DECIMAL(5,3) NULL,
    PRIMARY KEY (run_id, actor_pk),
    CONSTRAINT fk_am_run FOREIGN KEY (run_id) REFERENCES runs (run_id) ON DELETE CASCADE,
    CONSTRAINT fk_am_actor FOREIGN KEY (actor_pk) REFERENCES actors (actor_pk) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS niches (
    niche_id SMALLINT UNSIGNED NOT NULL AUTO_INCREMENT,
    slug     VARCHAR(96) NOT NULL,
    name     VARCHAR(128) NOT NULL,
    kind     ENUM('seed','auto','custom') NOT NULL DEFAULT 'seed',
    PRIMARY KEY (niche_id),
    UNIQUE KEY uq_niche_slug (slug)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS services (
    service_id   INT UNSIGNED NOT NULL AUTO_INCREMENT,
    niche_id     SMALLINT UNSIGNED NOT NULL,
    service_key  VARCHAR(96) NOT NULL COMMENT 'followers, posts, reviews, general, kw-location ...',
    name         VARCHAR(160) NOT NULL COMMENT 'Followers Scraper',
    display_name VARCHAR(255) NOT NULL COMMENT 'Instagram Followers Scraper',
    kind         ENUM('general','taxonomy','discovered') NOT NULL DEFAULT 'taxonomy',
    PRIMARY KEY (service_id),
    UNIQUE KEY uq_service (niche_id, service_key),
    CONSTRAINT fk_svc_niche FOREIGN KEY (niche_id) REFERENCES niches (niche_id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Текущая принадлежность Actor'ов услугам (пересобирается при каждом расчёте)
CREATE TABLE IF NOT EXISTS actor_services (
    actor_pk   INT UNSIGNED NOT NULL,
    service_id INT UNSIGNED NOT NULL,
    PRIMARY KEY (actor_pk, service_id),
    KEY idx_service (service_id),
    CONSTRAINT fk_asv_actor FOREIGN KEY (actor_pk) REFERENCES actors (actor_pk) ON DELETE CASCADE,
    CONSTRAINT fk_asv_service FOREIGN KEY (service_id) REFERENCES services (service_id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Топ-3 (или --per-service) «самых выгодных» Actor'ов в каждой услуге, с историей по дням
CREATE TABLE IF NOT EXISTS service_top (
    run_id      INT UNSIGNED NOT NULL,
    service_id  INT UNSIGNED NOT NULL,
    rank_no     TINYINT UNSIGNED NOT NULL,
    actor_pk    INT UNSIGNED NOT NULL,
    value_score DECIMAL(6,3) NOT NULL,
    s_price     DECIMAL(4,3) NOT NULL,
    s_users     DECIMAL(4,3) NOT NULL,
    s_rating    DECIMAL(4,3) NOT NULL,
    s_reviews   DECIMAL(4,3) NOT NULL,
    score_scope ENUM('service','niche') NOT NULL,
    pool_size   SMALLINT UNSIGNED NOT NULL,
    PRIMARY KEY (run_id, service_id, rank_no),
    KEY idx_actor_run (actor_pk, run_id),
    KEY idx_service_run (service_id, run_id),
    CONSTRAINT fk_st_run FOREIGN KEY (run_id) REFERENCES runs (run_id) ON DELETE CASCADE,
    CONSTRAINT fk_st_service FOREIGN KEY (service_id) REFERENCES services (service_id) ON DELETE CASCADE,
    CONSTRAINT fk_st_actor FOREIGN KEY (actor_pk) REFERENCES actors (actor_pk) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS niche_stats (
    run_id           INT UNSIGNED NOT NULL,
    niche_id         SMALLINT UNSIGNED NOT NULL,
    actors           INT UNSIGNED NOT NULL,
    priced           INT UNSIGNED NOT NULL,
    eligible         INT UNSIGNED NOT NULL,
    services         SMALLINT UNSIGNED NOT NULL,
    total_users      BIGINT UNSIGNED NOT NULL,
    median_cost_1000 DECIMAL(14,4) NULL,
    min_cost_1000    DECIMAL(14,4) NULL,
    median_rating    DECIMAL(4,3) NULL,
    PRIMARY KEY (run_id, niche_id),
    CONSTRAINT fk_ns_run FOREIGN KEY (run_id) REFERENCES runs (run_id) ON DELETE CASCADE,
    CONSTRAINT fk_ns_niche FOREIGN KEY (niche_id) REFERENCES niches (niche_id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS service_stats (
    run_id           INT UNSIGNED NOT NULL,
    service_id       INT UNSIGNED NOT NULL,
    actors           INT UNSIGNED NOT NULL,
    eligible         INT UNSIGNED NOT NULL,
    total_users      BIGINT UNSIGNED NOT NULL,
    median_cost_1000 DECIMAL(14,4) NULL,
    min_cost_1000    DECIMAL(14,4) NULL,
    PRIMARY KEY (run_id, service_id),
    CONSTRAINT fk_ss_run FOREIGN KEY (run_id) REFERENCES runs (run_id) ON DELETE CASCADE,
    CONSTRAINT fk_ss_service FOREIGN KEY (service_id) REFERENCES services (service_id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ---------------------------------------------------------------- представления для фронта
-- Последний снимок, который полностью посчитан (scored_at заполнен)
CREATE OR REPLACE VIEW v_latest_run AS
SELECT r.run_id, r.snapshot_date, r.collected, r.store_total, r.finished_at, r.scored_at
FROM runs r
WHERE r.snapshot_date = (SELECT MAX(snapshot_date) FROM runs WHERE scored_at IS NOT NULL);

-- Ниши: одна строка на нишу
CREATE OR REPLACE VIEW v_niche_overview_latest AS
SELECT n.niche_id, n.slug AS niche_slug, n.name AS niche_name, n.kind AS niche_kind,
       ns.actors, ns.priced, ns.eligible, ns.services AS services_count, ns.total_users,
       ns.median_cost_1000, ns.min_cost_1000, ns.median_rating, lr.snapshot_date
FROM v_latest_run lr
JOIN niche_stats ns ON ns.run_id = lr.run_id
JOIN niches n ON n.niche_id = ns.niche_id;

-- Услуги: одна строка на услугу + лидер (rank 1)
CREATE OR REPLACE VIEW v_service_overview_latest AS
SELECT n.niche_id, n.slug AS niche_slug, n.name AS niche_name,
       s.service_id, s.service_key, s.name AS service_name, s.display_name AS service_display_name, s.kind AS service_kind,
       ss.actors, ss.eligible, ss.total_users, ss.median_cost_1000, ss.min_cost_1000,
       t.actor_pk AS best_actor_pk, a.title AS best_actor_title, t.value_score AS best_value_score,
       lr.snapshot_date
FROM v_latest_run lr
JOIN service_stats ss ON ss.run_id = lr.run_id
JOIN services s ON s.service_id = ss.service_id
JOIN niches n ON n.niche_id = s.niche_id
LEFT JOIN service_top t ON t.run_id = ss.run_id AND t.service_id = ss.service_id AND t.rank_no = 1
LEFT JOIN actors a ON a.actor_pk = t.actor_pk;

-- Главная витрина: топ Actor'ов в каждой услуге каждой ниши (одна строка = одно место в топе)
CREATE OR REPLACE VIEW v_service_top_latest AS
SELECT n.niche_id, n.slug AS niche_slug, n.name AS niche_name,
       s.service_id, s.service_key, s.name AS service_name, s.display_name AS service_display_name,
       t.rank_no, t.value_score, t.s_price, t.s_users, t.s_rating, t.s_reviews, t.score_scope, t.pool_size,
       a.actor_pk, a.actor_key, a.title AS actor_title, a.username AS developer, a.url AS actor_url,
       sn.rating, sn.review_count, sn.total_users, sn.users_30d, sn.total_runs,
       sn.pricing_model, sn.pricing_summary, m.cost_1000, m.cost_basis,
       lr.run_id, lr.snapshot_date
FROM v_latest_run lr
JOIN service_top t ON t.run_id = lr.run_id
JOIN services s ON s.service_id = t.service_id
JOIN niches n ON n.niche_id = s.niche_id
JOIN actors a ON a.actor_pk = t.actor_pk
JOIN actor_snapshots sn ON sn.run_id = t.run_id AND sn.actor_pk = t.actor_pk
LEFT JOIN actor_metrics m ON m.run_id = t.run_id AND m.actor_pk = t.actor_pk;

-- Все Actor'ы последнего снимка с метриками
CREATE OR REPLACE VIEW v_actors_latest AS
SELECT a.actor_pk, a.actor_key, a.title, a.username AS developer, a.name AS slug, a.url,
       sn.rating, sn.review_count, sn.total_users, sn.users_30d, sn.total_runs,
       sn.pricing_model, sn.pricing_price_usd, sn.pricing_unit, sn.pricing_trial_min, sn.pricing_summary,
       m.cost_1000, m.cost_basis, m.bayes_rating, lr.run_id, lr.snapshot_date
FROM v_latest_run lr
JOIN actor_snapshots sn ON sn.run_id = lr.run_id
JOIN actors a ON a.actor_pk = sn.actor_pk
LEFT JOIN actor_metrics m ON m.run_id = sn.run_id AND m.actor_pk = sn.actor_pk;

-- В какие ниши/услуги входит Actor
CREATE OR REPLACE VIEW v_actor_services AS
SELECT asv.actor_pk, n.slug AS niche_slug, n.name AS niche_name,
       s.service_id, s.service_key, s.display_name AS service_display_name
FROM actor_services asv
JOIN services s ON s.service_id = asv.service_id
JOIN niches n ON n.niche_id = s.niche_id;

-- ---------------------------------------------------------------- рерайт описаний услуг (tracker/content.py)
-- Кэш описаний Actor'ов с сайта/API Apify (чтобы не ходить за ними при каждом прогоне)
CREATE TABLE IF NOT EXISTS actor_details (
    actor_pk        INT UNSIGNED NOT NULL,
    description     TEXT NULL,
    seo_title       VARCHAR(512) NULL,
    seo_description TEXT NULL,
    readme_excerpt  MEDIUMTEXT NULL,
    text_hash       CHAR(64) NOT NULL,
    fetched_at      DATETIME NOT NULL,
    PRIMARY KEY (actor_pk),
    CONSTRAINT fk_ad_actor FOREIGN KEY (actor_pk) REFERENCES actors (actor_pk) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Опубликованный (живой) рерайт услуги: его читает фронт через v_service_content_latest
CREATE TABLE IF NOT EXISTS service_content (
    service_id   INT UNSIGNED NOT NULL,
    source_hash  CHAR(64) NOT NULL COMMENT 'хэш исходных описаний + язык + провайдер + версия промпта',
    provider     VARCHAR(32) NOT NULL COMMENT 'llm или template',
    model        VARCHAR(64) NULL,
    lang         CHAR(2) NOT NULL DEFAULT 'en',
    headline     VARCHAR(255) NOT NULL,
    tagline      VARCHAR(512) NOT NULL,
    content      JSON NOT NULL COMMENT 'полная карточка в структуре страницы Apify Store',
    similarity   DECIMAL(4,3) NULL COMMENT 'доля 4-грамм текста, совпавших с исходниками (меньше = уникальнее)',
    version      INT UNSIGNED NOT NULL DEFAULT 1,
    rewritten_at DATETIME NOT NULL,
    published_at DATETIME NOT NULL,
    PRIMARY KEY (service_id),
    CONSTRAINT fk_sc_service FOREIGN KEY (service_id) REFERENCES services (service_id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Очередь накопленных изменений: рерайты кладутся сюда сразу, в service_content переезжают пачкой раз в N минут
CREATE TABLE IF NOT EXISTS service_content_staging (
    service_id   INT UNSIGNED NOT NULL,
    source_hash  CHAR(64) NOT NULL,
    provider     VARCHAR(32) NOT NULL,
    model        VARCHAR(64) NULL,
    lang         CHAR(2) NOT NULL DEFAULT 'en',
    headline     VARCHAR(255) NOT NULL,
    tagline      VARCHAR(512) NOT NULL,
    content      JSON NOT NULL,
    similarity   DECIMAL(4,3) NULL,
    rewritten_at DATETIME NOT NULL,
    PRIMARY KEY (service_id),
    CONSTRAINT fk_scs_service FOREIGN KEY (service_id) REFERENCES services (service_id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Витрина: карточка услуги + рыночные метрики последнего снимка
CREATE OR REPLACE VIEW v_service_content_latest AS
SELECT o.niche_slug, o.niche_name, o.service_id, o.service_key, o.service_display_name,
       o.actors, o.eligible, o.total_users, o.median_cost_1000, o.min_cost_1000,
       c.headline, c.tagline, c.content, c.lang, c.provider, c.version, c.similarity, c.published_at
FROM v_service_overview_latest o
JOIN service_content c ON c.service_id = o.service_id;

-- ---------------------------------------------------------------- ДВЕ ИТОГОВЫЕ ТАБЛИЦЫ для продажи (tracker/monitor.py, tracker/catalog.py)
-- Ключ обеих - постоянный Apify ID актора (не username/name: при переименовании актор остаётся тем же)

-- 1) Мониторинг цен: ВСЕ акторы Store (в т.ч. дорогие - вдруг скидка), одна строка на актора, обновляется постоянно
CREATE TABLE IF NOT EXISTS actor_price_monitor (
    apify_id             VARCHAR(32) NOT NULL,
    actor_key            VARCHAR(400) NOT NULL COMMENT 'username/name на момент последней проверки',
    username             VARCHAR(128) NOT NULL,
    name                 VARCHAR(255) NOT NULL,
    title                VARCHAR(512) NOT NULL COMMENT 'оригинальное название в Store (не рерайт)',
    url                  VARCHAR(512) NOT NULL,
    pricing_model        VARCHAR(32) NULL,
    pricing_unit         VARCHAR(64) NULL,
    price_usd            DECIMAL(16,8) NULL,
    pricing_summary      VARCHAR(1024) NULL,
    cost_1000            DECIMAL(14,4) NULL COMMENT 'оценка: USD за 1000 результатов',
    cost_basis           VARCHAR(96) NULL,
    prev_cost_1000       DECIMAL(14,4) NULL COMMENT 'цена до последнего изменения',
    prev_pricing_summary VARCHAR(1024) NULL,
    price_changed_at     DATETIME NULL COMMENT 'когда зафиксировано последнее изменение цены',
    change_pct           DECIMAL(10,2) NULL COMMENT 'изменение cost_1000 при последней смене, % (минус = скидка)',
    peak_cost_1000       DECIMAL(14,4) NULL COMMENT 'максимальная цена за всё время наблюдений',
    rating               DECIMAL(4,3) NULL,
    review_count         INT UNSIGNED NOT NULL DEFAULT 0,
    total_users          INT UNSIGNED NOT NULL DEFAULT 0,
    users_30d            INT UNSIGNED NOT NULL DEFAULT 0,
    is_active            TINYINT(1) NOT NULL DEFAULT 1 COMMENT '0 = актор пропал из Store',
    first_seen_at        DATETIME NOT NULL,
    last_seen_at         DATETIME NOT NULL,
    PRIMARY KEY (apify_id),
    KEY idx_mon_key (actor_key(191)),
    KEY idx_mon_changed (price_changed_at),
    KEY idx_mon_users (total_users)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 2) Каталог уникальных типов: рерайт делается ОДИН раз, когда появился новый apify_id. Цены здесь не хранятся
CREATE TABLE IF NOT EXISTS actor_catalog (
    apify_id                 VARCHAR(32) NOT NULL,
    actor_key                VARCHAR(400) NOT NULL,
    niche_slug               VARCHAR(96) NOT NULL,
    niche_name               VARCHAR(128) NOT NULL,
    service_key              VARCHAR(96) NOT NULL,
    service_display_name     VARCHAR(255) NOT NULL,
    title                    VARCHAR(255) NOT NULL COMMENT 'новое название (рерайт)',
    tagline                  VARCHAR(512) NOT NULL,
    content                  JSON NOT NULL COMMENT 'карточка в структуре Apify Store, без цен',
    source_title             VARCHAR(512) NOT NULL,
    source_hash              CHAR(64) NOT NULL,
    provider                 VARCHAR(32) NOT NULL,
    model                    VARCHAR(64) NULL,
    lang                     CHAR(2) NOT NULL DEFAULT 'en',
    similarity               DECIMAL(4,3) NULL,
    created_at               DATETIME NOT NULL,
    PRIMARY KEY (apify_id),
    KEY idx_cat_niche (niche_slug, service_key),
    KEY idx_cat_title (title(191))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Витрина: рерайт + актуальная цена из мониторинга (цена меняется, текст остаётся)
CREATE OR REPLACE VIEW v_actor_catalog_latest AS
SELECT c.apify_id, c.niche_slug, c.niche_name, c.service_key, c.service_display_name,
       c.title, c.tagline, c.content, c.created_at,
       m.url AS source_url, m.pricing_model, m.pricing_summary, m.cost_1000, m.prev_cost_1000,
       m.price_changed_at, m.change_pct, m.peak_cost_1000, m.rating, m.review_count,
       m.total_users, m.users_30d, m.is_active, m.last_seen_at
FROM actor_catalog c
JOIN actor_price_monitor m ON m.apify_id = c.apify_id;
