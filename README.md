# Tabellarius
Change Data Capture Source

<br>

## Quick Start
1. Start MySQL and Inspect CDC State: `docker compose up mysql cdc-cli`
   - output when CDC metadata is not initialized: `[MISSING] cdc_log table`

2. Initialize metadata
   ```
   docker compose run --rm cdc-cli \
     cdc-cli \
     --mode=init \
     --apply \
     --config=/app/cdc-config.yaml \
     --dsn=root:root@tcp(mysql:3306)/mydb
    ```

3. Re-run Inspect (Safe to re-execute)

   ```
   docker compose run --rm cdc-cli \
     cdc-cli \
     --mode=inspect \
     --config=/app/cdc-config.yaml \
     --dsn=root:root@tcp(mysql:3306)/mydb
   ```

4. Start the Server: `docker compose up cdc-server`

## Capture allow-list

The `tables` entries in the CDC configuration are a strict row-level capture allow-list, not only metadata for primary-key lookup. Tabellarius emits CDC row events only for tables listed there.

Keep consumer-managed audit or sink tables (for example, `revision_history`) out of `tables` unless they are intentionally captured. This prevents writes performed by a CDC consumer from being captured and processed recursively.

Each captured table continues to use its configured `pk` value for primary-key extraction.

## Cursus publisher

Set `cdc_server.publisher_config` to a Cursus SDK publisher YAML file. The existing `publisher_addr` setting remains supported as a legacy fallback, using the default `tabellarius.cdc` topic with automatic creation enabled.

```yaml
cdc_server:
  offset_file: offset.txt
  publisher_config: /config.yaml
```

```yaml
# /config.yaml
broker_addrs:
  - cursus-broker:9000
topic: commerce.revision-history.v1
auto_create_topics: true
partitions: 1
acks: "1"
enable_idempotence: true
```

At startup Tabellarius explicitly creates and verifies the configured topic before it begins CDC publishing. When `auto_create_topics: false`, a missing topic is a startup error. A `[publish]` success log is emitted only after the broker acknowledges the event.
