# Translator coverage: candidate MySQL → Postgres rewrite rules

This document is a research catalog of MySQL → Postgres expression
translations that **sluice does not currently implement**. It is a
deliberate proactive sweep against three external catalogs (sqlglot's
dialect transforms, pgloader's CAST rules, and the AWS DMS docs) plus
the MySQL function reference, scoped to expressions that show up in
DDL bodies — `GENERATED ALWAYS AS (…)`, `CHECK (…)`, `DEFAULT …`, and
index expression bodies — since those are what sluice's writer-side
translator (`internal/engines/postgres/expr_translate.go`) operates on.
Query-level translation is out of scope.

The deliverable is a triage list, not a backlog. Each entry below has
enough detail (MySQL form, PG form, semantic notes, citation, importance)
that a future contributor can decide whether to land it without
re-doing the research. The "How to land a rule" section at the bottom
documents the existing implementation pattern.

Importance is measured by **how often the construct appears in real-
world MySQL DDL**, not by general usefulness. A function that's common
in `SELECT` queries but vanishingly rare in `GENERATED ALWAYS AS (…)`
gets a low score here; the bar is "would this show up in a column
default or a check constraint."

## Candidate rules

Ordered high → low priority. Within each priority tier, ordered by
how syntactically simple the rewrite is (one-liners first, conditional
shapes later).

### High priority — common in DDL bodies

These are constructs that show up frequently in production MySQL
schemas in `DEFAULT`, `GENERATED`, or `CHECK` contexts and have a
clean PG equivalent.

#### 1. `CURRENT_TIMESTAMP()` / `NOW()` → `CURRENT_TIMESTAMP`

| Field | Value |
| --- | --- |
| MySQL | `CURRENT_TIMESTAMP()`, `NOW()`, `LOCALTIMESTAMP()`, `LOCALTIME()` |
| PG | `CURRENT_TIMESTAMP` (no parens), `LOCALTIMESTAMP` |
| Notes | PG accepts `CURRENT_TIMESTAMP` as a bare keyword (no parens). It will reject `NOW()` outright. PG's `CURRENT_TIMESTAMP` returns `timestamptz`; MySQL's returns `datetime`. For default values, both are immutable-enough for the column DEFAULT machinery. The bare-keyword form is what PG uses internally when reading back its own DEFAULTs, so the rewrite normalizes round-trips. |
| Source | sqlglot (`exp.CurrentTimestamp` → bare keyword in postgres generator); MySQL ref manual function list (NOW/CURRENT_TIMESTAMP/LOCALTIMESTAMP are synonyms) |
| Importance | High — `DEFAULT CURRENT_TIMESTAMP` and `DEFAULT NOW()` are the single most common non-literal default in MySQL schemas |

#### 2. `UNIX_TIMESTAMP(x)` → `EXTRACT(EPOCH FROM x)::bigint`

| Field | Value |
| --- | --- |
| MySQL | `UNIX_TIMESTAMP(ts_col)` |
| PG | `EXTRACT(EPOCH FROM ts_col)::bigint` (or `floor(extract(epoch from ts_col))::bigint`) |
| Notes | MySQL returns an integer (or decimal with fractional second precision); PG's `extract(epoch from …)` returns `double precision`, so an explicit cast is needed to match MySQL semantics in a generated column. Bare `UNIX_TIMESTAMP()` (no args) returns the current epoch — translate that to `EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::bigint`. Genuine immutability concerns apply when the source is a timestamp without timezone — PG treats `extract(epoch from timestamp)` as `STABLE`, not `IMMUTABLE`, which blocks generated columns. Worth a regression test before landing. |
| Source | sqlglot `exp.TimeStrToUnix → UNIX_TIMESTAMP` in MySQL, `DATE_PART('epoch', …)` in PG |
| Importance | High — MySQL apps frequently store epochs as derived columns |

#### 3. `FROM_UNIXTIME(x)` → `TO_TIMESTAMP(x)`

| Field | Value |
| --- | --- |
| MySQL | `FROM_UNIXTIME(epoch_col)` |
| PG | `TO_TIMESTAMP(epoch_col)` |
| Notes | Direct rename, single-arg form. The two-arg form `FROM_UNIXTIME(epoch, fmt)` has no clean PG equivalent (it returns a formatted string in MySQL); should NOT rewrite — fall through to the loud-failure tenet. PG returns `timestamptz`, MySQL returns `datetime`; in a generated column this is usually fine because the surrounding column type drives the cast. |
| Source | sqlglot (`exp.UnixToTime` → `TO_TIMESTAMP` in postgres generator) |
| Importance | High — pairs with UNIX_TIMESTAMP; both common in epoch-storing schemas |

#### 4. `CONCAT_WS(sep, a, b, …)` → `CONCAT_WS(sep, a, b, …)` (passthrough) but document NULL-handling

| Field | Value |
| --- | --- |
| MySQL | `CONCAT_WS(',', a, b, c)` |
| PG | `CONCAT_WS(',', a, b, c)` — same name |
| Notes | PG has `CONCAT_WS` natively, but it is `STABLE` (not `IMMUTABLE`), which blocks it from generated columns just like bare `concat()`. MySQL's `CONCAT_WS` skips NULL args; PG's also skips them. The portable rewrite is `array_to_string(ARRAY[a, b, c]::text[], ',')` which IS immutable-safe in PG when the args are immutable. **Open question:** is this needed in practice? Verbatim passthrough may already work in 99% of cases. Lands only if a real schema hits the immutability error. |
| Source | sqlglot (`CONCAT_WS` is in PG's function set but lacks an immutability gate) |
| Importance | High — frequently used to build composite keys / address strings in generated columns |

#### 5. `DATE_FORMAT(x, fmt)` → `TO_CHAR(x, pg_fmt)`

| Field | Value |
| --- | --- |
| MySQL | `DATE_FORMAT(x, '%Y-%m-%d')` |
| PG | `TO_CHAR(x, 'YYYY-MM-DD')` |
| Notes | Format-string translation is non-trivial — MySQL's `%Y/%m/%d/%H/%i/%s/%T` and friends map to PG's `YYYY/MM/DD/HH24/MI/SS`. sqlglot has a full mapping table (`TIME_MAPPING` in both dialects). For sluice's purposes, support a closed set of common format strings (`'%Y-%m-%d'`, `'%Y-%m-%d %H:%i:%s'`, `'%H:%i:%s'`) and fall through verbatim for anything else. **Tricky:** PG's `TO_CHAR` is `STABLE`, not `IMMUTABLE`, so a generated column using it will fail with "generation expression is not immutable". A wrapper using `to_char(x at time zone 'UTC', …)` in an immutable function is the typical workaround, but that's a per-deployment decision sluice can't make. Practical guidance: rewrite the format string but accept that the column may still need `--expr-override`. |
| Source | sqlglot (`exp.TimeToStr` ↔ `DATE_FORMAT` ↔ `TO_CHAR`, with `TIME_MAPPING` table) |
| Importance | High — common in display-derived generated columns; medium-high in defaults |

#### 6. `CHAR_LENGTH(x)` / `CHARACTER_LENGTH(x)` → `LENGTH(x)`

| Field | Value |
| --- | --- |
| MySQL | `CHAR_LENGTH(x)`, `CHARACTER_LENGTH(x)` |
| PG | `LENGTH(x)` (PG's LENGTH counts characters for `text`/`varchar`) |
| Notes | MySQL's `LENGTH(x)` returns BYTE length; `CHAR_LENGTH(x)` returns CHARACTER length. PG's `LENGTH(x)` on text returns characters (matching MySQL's `CHAR_LENGTH`); on `bytea` it returns bytes. The right rewrite is `CHAR_LENGTH → LENGTH` (semantically identical for text). The reverse — MySQL `LENGTH(x)` (bytes) → PG `OCTET_LENGTH(x)` — is a separate rule with different semantics; only rewrite when sluice can confirm the column is text-typed (which it can — that's available context). |
| Source | sqlglot (PG parser maps `LENGTH` and `CHAR_LENGTH` to the same expression) |
| Importance | High — common in `CHECK (CHAR_LENGTH(name) >= 3)` patterns |

#### 7. `LCASE(x)` / `UCASE(x)` → `LOWER(x)` / `UPPER(x)`

| Field | Value |
| --- | --- |
| MySQL | `LCASE(x)`, `UCASE(x)` |
| PG | `LOWER(x)`, `UPPER(x)` |
| Notes | Direct rename. MySQL accepts both `LCASE`/`LOWER` and `UCASE`/`UPPER`; PG only knows `LOWER`/`UPPER`. Fully immutable on both sides. Trivial single-call rewrite. |
| Source | MySQL function list (LCASE/UCASE are synonyms) |
| Importance | High — case-folded generated columns are common (e.g., for case-insensitive uniqueness) |

#### 8. `SUBSTR(x, …)` → `SUBSTRING(x, …)`

| Field | Value |
| --- | --- |
| MySQL | `SUBSTR(x, 1, 5)`, `MID(x, 1, 5)` |
| PG | `SUBSTRING(x FROM 1 FOR 5)` or `SUBSTRING(x, 1, 5)` (PG accepts both) |
| Notes | `SUBSTR` is a synonym for `SUBSTRING` in MySQL; `MID` is a third synonym. PG accepts `SUBSTRING(x, start, length)` with comma syntax, so a direct `SUBSTR → SUBSTRING` and `MID → SUBSTRING` rename is sufficient — the FROM/FOR keyword form is optional in PG. |
| Source | sqlglot `SUBSTR` parser in MySQL → `exp.Substring` |
| Importance | High — common in derived columns that take a prefix/suffix |

#### 9. `RAND()` → `RANDOM()`

| Field | Value |
| --- | --- |
| MySQL | `RAND()` |
| PG | `RANDOM()` |
| Notes | Both are `VOLATILE` and CANNOT sit in a generated column on either side. They CAN appear in a DEFAULT expression (DEFAULTs are evaluated per row, not stored in the catalog as immutable). Worth rewriting because it's mechanical and unambiguous. |
| Source | sqlglot (`exp.Rand → RANDOM` in postgres generator) |
| Importance | High — common in DEFAULT expressions for tokens / random initial values |

#### 10. `MD5(x)` / `SHA1(x)` / `SHA2(x, n)` → `digest(x, …)` (with caveat)

| Field | Value |
| --- | --- |
| MySQL | `MD5(x)`, `SHA1(x)`, `SHA2(x, 256)` |
| PG | `MD5(x)` works natively. `SHA*` requires the `pgcrypto` extension and uses `DIGEST(x, 'sha256')` returning `bytea`, not the hex string MySQL returns. |
| Notes | `MD5(x) → MD5(x)` is a passthrough — PG has it built in and returns hex text matching MySQL. `SHA1` / `SHA256` need `pgcrypto`'s `DIGEST` plus `ENCODE(…, 'hex')`: `ENCODE(DIGEST(x, 'sha256'), 'hex')`. The pgcrypto requirement is a tenet violation (extension dependency) — sluice should NOT rewrite SHA* automatically, but should document that the verbatim passthrough will fail loudly with "function sha256 does not exist" and the operator-side fix is `--expr-override` or extension installation. Track only `MD5` as a candidate; reject `SHA*` as out-of-scope. |
| Source | MySQL function list; PG `pgcrypto` docs |
| Importance | Medium-High — MD5 is common in derived columns; SHA* less so |

### Medium priority — appears in DDL but less frequently

#### 11. `GREATEST(a, b, …)` / `LEAST(a, b, …)` — passthrough

| Field | Value |
| --- | --- |
| MySQL | `GREATEST(a, b, c)`, `LEAST(a, b, c)` |
| PG | Same — `GREATEST(a, b, c)`, `LEAST(a, b, c)` |
| Notes | NOP — both engines have these natively with the same name and arity. **The reason this is in the catalog at all** is the NULL semantics: MySQL returns NULL if any arg is NULL; PG ignores NULLs and returns the GREATEST/LEAST of the non-null args (PG 9.5+). For CHECK constraints this matters; for generated columns it can produce silently different output between source and target. **Don't rewrite — flag in docs.** |
| Source | sqlglot `max_or_greatest` / `min_or_least` shared transforms |
| Importance | Medium — common in numeric clamp constraints; NULL difference is a known sharp edge |

#### 12. `INSTR(haystack, needle)` / `LOCATE(needle, haystack)` → `STRPOS(haystack, needle)` or `POSITION(needle IN haystack)`

| Field | Value |
| --- | --- |
| MySQL | `INSTR(s, sub)`, `LOCATE(sub, s)`, `LOCATE(sub, s, start)` |
| PG | `STRPOS(s, sub)` (single-position-only), `POSITION(sub IN s)` |
| Notes | Argument order differs: MySQL `LOCATE` is `(needle, haystack)`, MySQL `INSTR` and PG `STRPOS` are `(haystack, needle)`. `LOCATE` with three args (start position) has NO single-call PG equivalent — would require a `SUBSTRING` + `STRPOS` composition. Single-call shapes only: `INSTR(s, sub) → STRPOS(s, sub)`, `LOCATE(sub, s) → STRPOS(s, sub)` (with arg swap). |
| Source | sqlglot `LOCATE` / `STRPOS` / `POSITION` mapping |
| Importance | Medium — appears in derived prefix-detection columns |

#### 13. `REGEXP_LIKE(x, pat)` / `x REGEXP pat` / `x RLIKE pat` → `x ~ pat`

| Field | Value |
| --- | --- |
| MySQL | `x REGEXP 'pattern'`, `x RLIKE 'pattern'`, `REGEXP_LIKE(x, pattern)` |
| PG | `x ~ 'pattern'` (case-sensitive), `x ~* 'pattern'` (case-insensitive) |
| Notes | The infix `REGEXP` / `RLIKE` operator is harder to detect than a function call — needs token-level walking, not function-call replacement. `REGEXP_LIKE` is the function form (MySQL 8.0+) and is mechanically rewritable. Skip the infix forms in v1; add a rule for `REGEXP_LIKE(x, pat) → (x ~ pat)`. **Important:** MySQL uses ICU regex, PG uses POSIX — a meaningful subset of patterns work the same, but lookaheads/lookbehinds and named captures don't translate. Do NOT silently rewrite: document the divergence and lean on `--expr-override`. |
| Source | sqlglot (`exp.RegexpLike → ~`); MySQL function list |
| Importance | Medium — appears in CHECK constraints validating string formats |

#### 14. `REGEXP_REPLACE(x, pat, repl)` → `REGEXP_REPLACE(x, pat, repl, 'g')`

| Field | Value |
| --- | --- |
| MySQL | `REGEXP_REPLACE(x, pat, repl)` |
| PG | `REGEXP_REPLACE(x, pat, repl, 'g')` (note the global flag) |
| Notes | MySQL replaces all matches by default; PG replaces only the first match unless the `'g'` flag is supplied. The mechanical rewrite is to add `'g'` as a fourth argument when MySQL had three. The 4-arg MySQL form takes a `pos` argument with different semantics — fall through verbatim. |
| Source | sqlglot `regexp_replace_global_modifier` helper |
| Importance | Medium — used in cleanup/normalization generated columns |

#### 15. `DATE_ADD(d, INTERVAL n unit)` / `DATE_SUB(d, INTERVAL n unit)` → `d + INTERVAL '… unit'` / `d - INTERVAL '… unit'`

| Field | Value |
| --- | --- |
| MySQL | `DATE_ADD(d, INTERVAL 7 DAY)`, `DATE_SUB(d, INTERVAL 1 MONTH)` |
| PG | `(d + INTERVAL '7 day')`, `(d - INTERVAL '1 month')` |
| Notes | MySQL also accepts the operator form `d + INTERVAL 7 DAY`, which works in PG with quoted-interval syntax. The function form `DATE_ADD/DATE_SUB` is a candidate for a function-call rewrite. **MySQL has compound interval units** (`HOUR_MINUTE`, `DAY_HOUR`, etc.) that PG doesn't support — those should fall through. |
| Source | sqlglot `build_date_delta_with_interval` / `_date_add_sql` |
| Importance | Medium — appears in `DEFAULT (CURRENT_TIMESTAMP + INTERVAL 7 DAY)` and similar TTL patterns |

#### 16. `TIMESTAMPDIFF(unit, a, b)` → equivalent `EXTRACT`/arithmetic expression

| Field | Value |
| --- | --- |
| MySQL | `TIMESTAMPDIFF(MINUTE, a, b)` |
| PG | `EXTRACT(EPOCH FROM (b - a))/60` (for MINUTE) or `(b - a)::interval` arithmetic |
| Notes | No clean one-call PG equivalent; the rewrite is unit-specific. Likely **not worth implementing**: the cross-product of allowed MySQL units (`MICROSECOND`, `SECOND`, `MINUTE`, `HOUR`, `DAY`, `WEEK`, `MONTH`, `QUARTER`, `YEAR`) × the unit-specific PG expression makes the rule table unwieldy. Lean on `--expr-override`. **Catalog only.** |
| Source | sqlglot `TIMESTAMPDIFF` parser; PG has `AGE()` and `EXTRACT(EPOCH FROM …)` as building blocks |
| Importance | Medium |

#### 17. `IFNULL` already covered; `ISNULL(x)` (MySQL) → `(x IS NULL)`

| Field | Value |
| --- | --- |
| MySQL | `ISNULL(x)` (function form, returns 1 or 0) |
| PG | `(x IS NULL)` (returns boolean) |
| Notes | MySQL's `ISNULL(x)` returns an integer (1 or 0); PG returns boolean. In CHECK constraints this is fine (PG promotes the boolean). In generated columns where the column is integer-typed, the rewrite needs a cast: `(x IS NULL)::int`. The bool-context rewrite already handles the int side; this rule just adds the function-call recognition. Note that PG also has a non-standard `ISNULL` with the boolean semantic (`x ISNULL` is an alias for `x IS NULL`), but that's the operator form, not a function call. |
| Source | sqlglot MySQL parser `isnull_to_is_null` |
| Importance | Medium — less common than `IFNULL` but still appears |

#### 18. `NULLIF(a, b)` — passthrough

| Field | Value |
| --- | --- |
| MySQL | `NULLIF(a, b)` |
| PG | `NULLIF(a, b)` |
| Notes | NOP. Both engines have this function with identical semantics. No rewrite needed. **Listed only to document the no-op.** |
| Source | SQL standard |
| Importance | Medium — appears in division-by-zero guards |

#### 19. `BIN(x)` / `OCT(x)` / `HEX(x)` → `to_char` / `to_hex`

| Field | Value |
| --- | --- |
| MySQL | `HEX(x)`, `BIN(x)`, `OCT(x)` |
| PG | `to_hex(x)` (integer → hex string), `to_char(x, 'FMS999…')` (no direct BIN equivalent), `to_char(x, '999…')` for OCT |
| Notes | `HEX(int) → to_hex(int)` is mechanical. `HEX(string)` (returning hex of bytes) → `encode(x::bytea, 'hex')`. `BIN(int)` has no clean PG one-call equivalent. **Lands narrow:** only `HEX(int_or_bigint) → to_hex(…)`. |
| Source | MySQL function list; PG `to_hex` |
| Importance | Medium — appears in display-format generated columns |

#### 20. `JSON_OBJECT(k1, v1, k2, v2, …)` / `JSON_ARRAY(a, b, …)` — passthrough

| Field | Value |
| --- | --- |
| MySQL | `JSON_OBJECT('k', v)`, `JSON_ARRAY(a, b)` |
| PG | `JSON_OBJECT('k', v)` (PG 16+), `JSON_ARRAY(a, b)` (PG 16+); pre-16: `JSON_BUILD_OBJECT(…)` / `JSON_BUILD_ARRAY(…)` |
| Notes | PG 16 added the SQL-standard `JSON_OBJECT` / `JSON_ARRAY` functions; earlier PG versions need the `JSON_BUILD_*` variants. sluice's PG target version isn't always known at write time. If sluice can detect the server version (which it already does for capability declaration), gate the rewrite: `<16` rewrites to `JSON_BUILD_*`, `>=16` passes through. |
| Source | sqlglot; PG release notes |
| Importance | Medium — JSON-typed generated columns are a growing pattern |

### Low priority — rare in DDL or with semantic gotchas

#### 21. `FIND_IN_SET(needle, csv_string)` → `(needle = ANY(string_to_array(csv_string, ',')))`

| Field | Value |
| --- | --- |
| MySQL | `FIND_IN_SET('x', 'a,b,x,y')` returns position 1-based (3 here), 0 if not found |
| PG | `(SELECT i FROM unnest(string_to_array('a,b,x,y', ',')) WITH ORDINALITY AS t(v, i) WHERE v = 'x')` for full semantics, or `('x' = ANY(string_to_array('a,b,x,y', ',')))` if only membership matters |
| Notes | The full position-returning semantic doesn't translate cleanly to a CHECK / GENERATED expression — the LATERAL-subquery form isn't valid in those contexts. Membership-only semantic translates cleanly. Probably **not worth a built-in rule** — too narrow. |
| Source | MySQL function list |
| Importance | Low |

#### 22. `FIELD(x, a, b, c, …)` — no clean PG equivalent

| Field | Value |
| --- | --- |
| MySQL | `FIELD('b', 'a', 'b', 'c')` returns 2 |
| PG | `array_position(ARRAY['a','b','c'], 'b')` (PG 9.5+) |
| Notes | Mechanical rename + array-construction wrap. Niche use case (returns the position of a value in a list, often used for custom ORDER BY). Rare in DDL. |
| Source | MySQL function list |
| Importance | Low |

#### 23. `CONVERT_TZ(ts, from_tz, to_tz)` → `(ts AT TIME ZONE from_tz AT TIME ZONE to_tz)`

| Field | Value |
| --- | --- |
| MySQL | `CONVERT_TZ(ts, 'UTC', 'America/Los_Angeles')` |
| PG | `(ts AT TIME ZONE 'UTC' AT TIME ZONE 'America/Los_Angeles')` |
| Notes | MySQL's `CONVERT_TZ` requires the timezone tables to be loaded (`mysql.time_zone_*`); PG ships with full tzdata. The rewrite is mechanical but the `AT TIME ZONE` operator has subtle semantics around `timestamp` vs `timestamptz`. Worth flagging for `--expr-override` rather than auto-rewriting. |
| Source | sqlglot `exp.ConvertTimezone` |
| Importance | Low — uncommon in DDL bodies, more often a query-level concern |

#### 24. `LAST_DAY(d)` → `(DATE_TRUNC('month', d) + INTERVAL '1 month' - INTERVAL '1 day')::date`

| Field | Value |
| --- | --- |
| MySQL | `LAST_DAY(d)` returns the last day of the month containing d |
| PG | No built-in. Composition: `(DATE_TRUNC('month', d) + INTERVAL '1 month - 1 day')::date` |
| Notes | Doable but verbose; the rewrite expands one call to a five-token expression. Probably one for `--expr-override` rather than the table. |
| Source | sqlglot `no_last_day_sql` |
| Importance | Low |

#### 25. `DAYNAME(d)` / `MONTHNAME(d)` → `TO_CHAR(d, 'Day')` / `TO_CHAR(d, 'Month')`

| Field | Value |
| --- | --- |
| MySQL | `DAYNAME(d)`, `MONTHNAME(d)` |
| PG | `TO_CHAR(d, 'Day')` (note PG pads to 9 chars), `TO_CHAR(d, 'Month')` |
| Notes | PG's `TO_CHAR` for `Day` / `Month` pads the result to fixed width with trailing spaces; use `'FMDay'` / `'FMMonth'` to suppress padding. Same `STABLE`-not-`IMMUTABLE` problem as `TO_CHAR` for `DATE_FORMAT`. |
| Source | sqlglot `MONTHNAME` parser |
| Importance | Low |

#### 26. `WEEK(d, mode)` / `WEEKOFYEAR(d)` → `EXTRACT(WEEK FROM d)`

| Field | Value |
| --- | --- |
| MySQL | `WEEK(d, 1)`, `WEEKOFYEAR(d)` |
| PG | `EXTRACT(WEEK FROM d)` |
| Notes | MySQL's `WEEK` takes a `mode` argument (0–7) controlling Sunday/Monday-start and locale; PG's `EXTRACT(WEEK …)` always uses ISO 8601. The semantic difference matters for derived columns. Don't auto-rewrite when mode != 1 (ISO). `WEEKOFYEAR` is equivalent to `WEEK(d, 3)`. |
| Source | sqlglot `WEEK` parser |
| Importance | Low |

#### 27. `YEARWEEK(d)` / `QUARTER(d)` → `EXTRACT(YEAR …) * 100 + EXTRACT(WEEK …)` / `EXTRACT(QUARTER FROM d)`

| Field | Value |
| --- | --- |
| MySQL | `QUARTER(d)`, `YEARWEEK(d)` |
| PG | `EXTRACT(QUARTER FROM d)`, `EXTRACT(YEAR FROM d) * 100 + EXTRACT(WEEK FROM d)` |
| Notes | `QUARTER` is mechanical. `YEARWEEK` requires multiplication and has the same week-numbering caveat as #26. |
| Source | sqlglot date-part transforms |
| Importance | Low |

#### 28. `DATEDIFF(a, b)` → `(a::date - b::date)`

| Field | Value |
| --- | --- |
| MySQL | `DATEDIFF(a, b)` returns days as integer |
| PG | `(a::date - b::date)` returns days as integer |
| Notes | Mechanical, but PG's date subtraction is a SQL operator, not a function call — the rewrite produces a parenthesised expression instead of a function call. |
| Source | sqlglot `_date_diff_sql` |
| Importance | Low — `DATEDIFF` rarely appears in DDL bodies |

#### 29. `INET_ATON(ip)` / `INET_NTOA(int)` → no clean PG equivalent without an extension

| Field | Value |
| --- | --- |
| MySQL | `INET_ATON('1.2.3.4')` returns 16909060 |
| PG | Requires custom function or extension; PG's `inet` type is the right-shaped abstraction but the conversion isn't built in |
| Notes | Out of scope — no portable rewrite. Catalog only. |
| Source | MySQL function list |
| Importance | Low |

#### 30. `UUID()` → `gen_random_uuid()`

| Field | Value |
| --- | --- |
| MySQL | `UUID()` |
| PG | `gen_random_uuid()` (PG 13+, no extension needed); pre-13 needs `uuid-ossp` and `uuid_generate_v4()` |
| Notes | Both are `VOLATILE` so they only appear in DEFAULT, not GENERATED. Mechanical rename. Gate on PG version 13+ (sluice already detects this for other capabilities). |
| Source | sqlglot `exp.Uuid → GEN_RANDOM_UUID()` in postgres generator |
| Importance | Low-Medium — UUID DEFAULTs are common in modern schemas; sluice's MySQL reader may already canonicalize the column type to UUID, in which case no rewrite is needed |

## Already covered

The rules sluice already implements (CONCAT, JSON extract idioms,
IFNULL, IF, CAST CHAR with charset/collate, the bool-context idioms,
the int-context coalesce direction) are documented in the
**Cumulative scope** table at the bottom of
[ADR-0016](../adr/adr-0016-layered-expression-translation.md). This
document deliberately doesn't restate them.

## Out of scope

These categories appeared during research but are deliberately
excluded from the candidate list:

- **Query-only constructs.** `LIMIT … OFFSET …`, `GROUP_CONCAT`,
  `LAG` / `LEAD` / window-function syntax, `ANY_VALUE`, aggregate
  functions, `MATCH … AGAINST` full-text search. sluice doesn't
  translate queries.
- **Spatial functions** (`ST_*`, `POINT`, `LINESTRING`, etc.). These
  go through PostGIS on the PG side and have a large surface; type
  mapping is handled by sluice's existing extension-aware type
  mapping, and expression-level rewrites would need to know whether
  PostGIS is present. Catalog only.
- **Functions that depend on session/server config.**
  - `LAST_INSERT_ID()` — PG has `lastval()` but the semantic depends
    on sequence usage; not a static rewrite.
  - `CURRENT_USER()` / `USER()` / `SESSION_USER()` — PG has these
    but with subtle semantic differences; appearing in DDL is rare
    enough that verbatim passthrough is fine.
  - `DATABASE()` / `SCHEMA()` — both have PG equivalents
    (`current_database()`, `current_schema()`) but again rare in DDL.
  - `CONVERT_TZ()` — depends on the source server having the timezone
    tables loaded, which sluice can't surface. Listed in #23 but as
    catalog-only.
- **Functions that cross extension boundaries.** `SHA2`, `INET_ATON`,
  `INET_NTOA`, full-text functions. Adding a rule that emits an
  extension-dependent expression violates the "contain Postgres
  complexity" tenet — the operator should opt in to the extension
  and use `--expr-override`.
- **Compound INTERVAL units.** `INTERVAL '5 1' HOUR_MINUTE` and
  similar — MySQL accepts dozens of unit combinations; PG only
  accepts the singular forms. Translating the table of compound
  forms would balloon the rule set; better to fall through and let
  the operator override.
- **The `0000-00-00` zero-date in DEFAULTs.** This is a value-level
  concern (sluice already maps the value to NULL via the engine
  reader's value layer) and the DDL-level rewrite (drop the default
  entirely) is the responsibility of the schema reader's normalizer,
  not the expression translator. Documented here to flag it for the
  reader.
- **`AUTO_INCREMENT`, `ON UPDATE CURRENT_TIMESTAMP`.** These are
  column-level attributes, not expressions. The reader / writer
  handle them as IR-level concepts, not via the expression translator.

## How to land a rule

The pattern is small and well-established. To add a new rule:

1. **Add the rewrite function** in
   `internal/engines/postgres/expr_translate.go`. Follow the
   `rewriteFunctionCalls("FNNAME", func(args []string) string { … })`
   shape used by `rewriteCONCAT`, `rewriteIFNULL`, etc. The walker
   already handles string literals and balanced parens; the callback
   only needs to validate arity and produce the rewritten text.
   Return `""` from the callback to fall through verbatim — this is
   the loud-failure escape hatch.
2. **Wire it into `translateExprForPG`.** Order matters when one
   rewrite depends on the canonical form produced by another (e.g.
   `IFNULL → COALESCE` runs before the bool-context COALESCE rewrite
   so the latter only needs to look at COALESCE).
3. **Add tests** in `expr_translate_test.go`. Cover at minimum:
   the basic-shape rewrite, a passthrough case for an unrelated
   construct, a string-literal-containing-function-name case, and any
   shape variants (lowercase function name, extra whitespace, nested
   call). The existing tests are templates.
4. **Update the cumulative-scope table** at the bottom of
   `docs/adr/adr-0016-layered-expression-translation.md` with a one-
   liner row for the new rule. The ADR is the canonical "what does
   sluice translate" reference; this catalog is "what could sluice
   translate."
5. **Add an integration repro** if the rule was motivated by a real
   schema. Cross-engine integration tests live in
   `internal/pipeline/migrate_cross_integration_test.go`. The repro
   doubles as a regression guard.
6. **Run the pre-commit hook.** `gofumpt`, `go vet`,
   `golangci-lint run`, `go test ./...` — same gate that CI runs.
7. **Leave the rule out** if any of the following apply, and instead
   document the construct as `--expr-override`-territory:
   - The PG equivalent depends on an extension sluice doesn't
     install (`pgcrypto`, `uuid-ossp` on PG <13, full-text-search
     dictionaries).
   - The semantic differs subtly between engines (NULL handling,
     timezone behavior, locale dependence, week-numbering modes).
     Silent rewrites that produce different output between source
     and target violate the "loud failure beats silent corruption"
     tenet.
   - The MySQL form can't be detected with the existing string-aware
     walker without a real parser. Building a parser is explicitly
     not the path (see ADR-0016 § Decision).
