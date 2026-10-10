# sluice v0.157.1

**Two loud broker-recovery fixes found by the v0.157.0 regression cycle, and a correction to the v0.157.0 notes. No data was at risk in either; both affect what an operator can do to recover a broker.** (1) Bug 299: `--reset-target-data` was refused over a broker position row that had been damaged so it no longer decoded, and the row was mislabelled as belonging to a non-broker writer. **The v0.157.0 notes said the flag "is honoured over any broker-owned row, decodable or not"; that was not true at v0.157.0.** It is true now. (2) Bug 298: the `BROKER-CLASSIC-RESUME` refusal, which a broker upgraded from v0.156.12 or older can hit, offered remedies that did not apply and never mentioned `--reset-target-data`. It now names the recoveries that work for its case, and says plainly when the shortcut would duplicate rows.

## Fixed

**`--reset-target-data` is honoured over a broker position row that does not decode (Bug 299; LOW, loud, no data loss).** sluice decided whether a `sluice_cdc_state` row belonged to a broker by decoding the whole position token. A broker row with a wrong-typed field, invalid JSON or a truncated token was therefore reported as `owned by a non-broker writer (position engine "postgres")`, naming the target applier's engine rather than the row's. No broker writes such a row: the damage comes from outside, such as a hand edit or a bad restore of the control table. Because a non-broker row is refused with or without the flag, `--reset-target-data` was refused over it too. Ownership is now judged from the token's `_engine` marker alone, for both the `backup-broker-v2` token and the classic `backup-broker` one, before anything else is decoded. Over such a row, `--reset-target-data` discards it and rebuilds the target. A plain run refuses with the new marker **`BROKER-POSITION-CORRUPT`**, naming `--reset-target-data` and the `--at-chain-id` alternative, with nothing applied. A row whose marker is missing, damaged or not a broker's is still refused with or without the flag, and that refusal now says what the token carries. The marker check is held to Go's own JSON decoding of `_engine` by the differential fuzz gate `FuzzTokenEngineMarker`. `TestBrokerToken_MarkerIsFirstAndSurvivesTruncation` checks that every broker token opens with the marker and stays broker-owned when cut at any later byte. The damage shapes are pinned on real Postgres and MySQL targets by `TestBroker_ResetOverAPosition_Postgres` and `TestBroker_CorruptBrokerRow_MySQLGTID`, and a foreign row stays refused by `TestBroker_ForeignRowRefusedWithOrWithoutReset`. **Affected releases:** the mislabelling since v0.20.1; the refused `--reset-target-data` matters from v0.157.0, the first release meant to honour the flag over a broker position.

**The `BROKER-CLASSIC-RESUME` refusal gives the remedy that fits it (Bug 298; LOW, loud, no data loss).** After an upgrade from v0.156.12 or older, the incremental the old broker may have been inside refuses with `SLUICE-E-BROKER-KEYLESS-TABLE` naming `BROKER-CLASSIC-RESUME`. The refusal is correct and unchanged. Its hint, though, was the general keyless one, which a chain this sluice wrote already satisfies, and it never mentioned `--reset-target-data`, the recovery the regression cycle measured converging to the source. The hint now:
- says re-running the same command refuses again, because the old position is replaced only by applying the incremental it refuses;
- always offers `--reset-target-data`;
- offers deleting the stream's `sluice_cdc_state` row and running with `--at-chain-id=<the last applied id>` only when the classic position is the sole reason that incremental is refused. If it also records no change identities, carries a change without one, or the target's apply marks cannot cover the table, that route would refuse again, so it is not offered.
- where that route is offered, says it is the operator's own assertion that no broker run started applying the incremental after the position was written. A later run's refusal of it is not evidence, since an earlier, older run may have been interrupted inside it and left the same position. If the assertion is wrong, the rows that run committed are duplicated silently.

Giving the target table a key is deliberately not offered: a table that is keyless on the source may hold duplicate rows, and a key would merge them. Pinned by `TestBroker_ClassicTokenResumeWithholdsTheKeylessLift` and `TestClassicResumeHint_ChoosesByReason`. **Affected releases:** v0.157.0.

## Compatibility

No format, flag or default changes. `BROKER-POSITION-CORRUPT` is a new grep-stable marker on an exit-1 refusal (uncoded), replacing a misleading "owned by a non-broker writer" message for broker rows that do not decode. The `SLUICE-E-BROKER-KEYLESS-TABLE` remedy text changed; the code and its exit status did not.

**Not run for this release:** the dispatch-only `psverify` (live PlanetScale), `d1verify` (live Cloudflare D1) and `nekiverify` (live Neki) suites, which need billable or live infrastructure and run only on the operator's call. This release changes only the broker's position-ownership judgment and a refusal's hint text, which those suites do not exercise beyond what the Postgres and MySQL integration tests above cover.

## Who needs this

- **Anyone recovering a `sync from-backup` broker whose `sluice_cdc_state` row was hand-edited or restored badly**, and who hit "owned by a non-broker writer" with `--reset-target-data`: upgrade and re-run with the flag.
- **Anyone who upgraded a broker from v0.156.12 or older and hit `BROKER-CLASSIC-RESUME`:** read the new hint before using the `--at-chain-id` route. If you cannot vouch that no run started the named incremental, use `--reset-target-data`.
- **Everyone else:** upgrade at leisure; nothing to check.

---

**Install:** `brew install sluicesync/tap/sluice` · `go install sluicesync.dev/sluice/cmd/sluice@v0.157.1` · **Container:** `ghcr.io/sluicesync/sluice:0.157.1`

**Full changelog:** https://github.com/sluicesync/sluice/blob/main/CHANGELOG.md
