-- Kitchen-sink seed v1 for the MySQL restore-parity oracle (roadmap
-- item 51, Phase 2). Mirrors testdata/dump_parity_seed.sql for the
-- MySQL feature surface. Keep the object/column counts in sync with the
-- vacuous-pass floor consts in
-- migrate_dump_parity_mysql_integration_test.go.
--
-- Feature checklist (one line per class):
--   * BIGINT UNSIGNED AUTO_INCREMENT PK             (every table's id)
--   * ENUM column + enum default                    (customers.status)
--   * SET column + set default                      (customers.tags)
--   * non-default charset + collation on a column   (customers.region_code, latin1/latin1_bin)
--   * TIMESTAMP DEFAULT CURRENT_TIMESTAMP           (customers.created_at)
--   * DATETIME(6) explicit fractional precision     (customers.updated_at)
--   * TEXT column + table/column COMMENT            (customers.notes / table comment)
--   * UNIQUE KEY (single + composite)               (customers_email_uidx, line_items_order_sku_uidx)
--   * secondary KEY                                  (orders_customer_idx, orders_placed_idx)
--   * prefix index                                  (line_items_sku_prefix_idx, sku(16))
--   * STORED generated column                       (orders.total_cents)
--   * VIRTUAL generated column                      (blobs.data_len)
--   * DECIMAL(p,s)                                   (orders.discount_pct, line_items.unit_price)
--   * TINYINT(1) boolean + default                  (orders.status_flag)
--   * JSON column                                    (orders.payload)
--   * named CHECK constraint                        (orders_subtotal_positive)
--   * FK ON DELETE CASCADE                          (orders_customer_fk, line_items_order_fk)
--   * BLOB / BINARY / BIT column + bit-literal dflt (blobs.data / blobs.fixed / blobs.flags)
--   * MEDIUMINT UNSIGNED                            (line_items.qty)
--   * a few rows so the sluice leg exercises the realistic bulk-copy
--     path; the parity comparison itself is schema-only.
--
-- Deliberately absent in v1 (documented limitations, not oversights):
-- stored routines / triggers / views (BEGIN…END bodies, DELIMITER
-- changes) and spatial types (the PostGIS/spatial legs are a follow-up
-- per the roadmap entry).

CREATE TABLE customers (
    id          BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    email       VARCHAR(255) NOT NULL,
    full_name   VARCHAR(255) NOT NULL,
    status      ENUM('active','inactive','pending') NOT NULL DEFAULT 'active',
    tags        SET('vip','beta','internal') NOT NULL DEFAULT 'vip',
    region_code CHAR(2) CHARACTER SET latin1 COLLATE latin1_bin NOT NULL DEFAULT 'zz',
    balance_cents BIGINT NOT NULL DEFAULT 0,
    created_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at  DATETIME(6) NULL,
    notes       TEXT COMMENT 'free-form account notes',
    PRIMARY KEY (id),
    UNIQUE KEY customers_email_uidx (email),
    KEY customers_status_idx (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='registered customers';

CREATE TABLE orders (
    id             BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    customer_id    BIGINT UNSIGNED NOT NULL,
    subtotal_cents INT NOT NULL,
    tax_cents      INT NOT NULL DEFAULT 0,
    total_cents    INT GENERATED ALWAYS AS ((subtotal_cents + tax_cents)) STORED,
    discount_pct   DECIMAL(5,2) NOT NULL DEFAULT 0.00,
    status_flag    TINYINT(1) NOT NULL DEFAULT 1,
    payload        JSON DEFAULT NULL,
    placed_on      DATE NOT NULL,
    PRIMARY KEY (id),
    KEY orders_customer_idx (customer_id),
    KEY orders_placed_idx (placed_on),
    CONSTRAINT orders_customer_fk FOREIGN KEY (customer_id)
        REFERENCES customers (id) ON DELETE CASCADE,
    CONSTRAINT orders_subtotal_positive CHECK ((subtotal_cents >= 0))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE line_items (
    id         BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    order_id   BIGINT UNSIGNED NOT NULL,
    sku        VARCHAR(64) NOT NULL,
    qty        MEDIUMINT UNSIGNED NOT NULL DEFAULT 1,
    unit_price DECIMAL(12,4) NOT NULL,
    descr      VARCHAR(500) NULL,
    PRIMARY KEY (id),
    UNIQUE KEY line_items_order_sku_uidx (order_id, sku),
    KEY line_items_sku_prefix_idx (sku(16)),
    CONSTRAINT line_items_order_fk FOREIGN KEY (order_id)
        REFERENCES orders (id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE blobs (
    id       INT NOT NULL AUTO_INCREMENT,
    data     BLOB,
    data_len INT GENERATED ALWAYS AS (length(data)) VIRTUAL,
    fixed    BINARY(16) NULL,
    flags    BIT(8) NOT NULL DEFAULT b'00000001',
    PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO customers (email, full_name, status, tags, region_code, balance_cents, notes) VALUES
    ('alice@example.com', 'Alice Example', 'active',  'vip,beta', 'us', 12500, 'founding member'),
    ('bob@example.com',   'Bob Example',   'pending', 'internal', 'mx',     0, NULL);

INSERT INTO orders (customer_id, subtotal_cents, tax_cents, discount_pct, status_flag, payload, placed_on) VALUES
    (1, 12500, 1000, 5.00, 1, JSON_OBJECT('coupon', 'WELCOME'), '2026-02-01'),
    (2,  9900,    0, 0.00, 0, NULL,                              '2026-02-02');

INSERT INTO line_items (order_id, sku, qty, unit_price, descr) VALUES
    (1, 'SKU-1', 2, 49.9900, 'widget'),
    (1, 'SKU-2', 1, 25.0000, NULL);

INSERT INTO blobs (data, fixed, flags) VALUES
    (_binary 'hello', _binary '0123456789abcdef', b'00000010');
