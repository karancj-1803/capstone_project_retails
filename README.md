# Adding a New Source to the Pipeline

This platform is **metadata-driven** — every source (Orders, Products, Customers, Exchange Rates, and any future source) is processed by the same generic Bronze and Silver engines. You do not need to write any new PySpark code to onboard a new source. You only need to add one new entry to `config.json` and upload your raw files to the correct landing path.

This guide walks through exactly how to do that.

---

## 1. Where the config file lives

`config.json` is stored in ADLS at:

```
abfss://config@<storage_account>.dfs.core.windows.net/config_initial.json
```

It contains a single top-level `sources` array. Each object in that array describes one source. To add a new source, you add one new object to this array — you never need to touch the Bronze, Silver, or Gold notebooks.

---

## 2. The config entry — field by field

Copy this template and fill in every field for your new source:

```json
{
  "source_id": 5,
  "source_name": "your_source_name",
  "source_type": "csv",
  "landing_path": "abfss://landing@<storage_account>.dfs.core.windows.net/your_source_name/",
  "checkpoint_path": "abfss://bronze@<storage_account>.dfs.core.windows.net/checkpoints/your_source_name/",
  "schema_location": "abfss://bronze@<storage_account>.dfs.core.windows.net/schemas/your_source_name/",
  "bronze_table_path": "retails.bronze.your_source_name",
  "bronze_storage_path": "abfss://bronze@<storage_account>.dfs.core.windows.net/your_source_name/",
  "silver_table_path": "retails.silver.your_source_name",
  "silver_storage_path": "abfss://silver@<storage_account>.dfs.core.windows.net/your_source_name/",
  "load_pattern": "incremental",
  "primary_key": "YourPrimaryKeyColumn",
  "watermark_column": "SomeDateColumn",
  "is_active": true,
  "partition_column": null,
  "columns": [
    {
      "name": "YourPrimaryKeyColumn",
      "target_type": "int",
      "rule": "not_null"
    },
    {
      "name": "SomeAmountColumn",
      "target_type": "decimal(10,2)",
      "rule": "positive"
    },
    {
      "name": "SomeTextColumn",
      "target_type": "string",
      "rule": "none",
      "transformations": ["initcap", "normalize_space"]
    }
  ]
}
```

### Field reference

| Field | Required | Description |
|---|---|---|
| `source_id` | Yes | A unique integer. Use the next free number. |
| `source_name` | Yes | Lowercase, no spaces. Used in table names, logs, and folder paths. |
| `source_type` | Yes | `"csv"` or `"json"`. Determines how Auto Loader reads the file. |
| `landing_path` | Yes | Where raw files for this source land in ADLS. Auto Loader watches this folder. |
| `checkpoint_path` | Yes | Must be **unique to this source** — never reuse another source's checkpoint folder. |
| `schema_location` | Yes | Must also be **unique to this source**, for the same reason as `checkpoint_path`. |
| `bronze_table_path` / `silver_table_path` | Yes | Catalog table names. Use the pattern `retails.bronze.<source_name>` / `retails.silver.<source_name>`. |
| `bronze_storage_path` / `silver_storage_path` | Yes | The actual ADLS folder the table's files are written to. |
| `load_pattern` | Yes | One of `"full_load"`, `"incremental"`, or `"scd2"` — see Section 3 below. |
| `primary_key` | Yes | The column (or list of columns, for a composite key) that uniquely identifies a row. |
| `watermark_column` | Only for `incremental`/`scd2` | The date/timestamp column used to detect new or changed rows. Set to `null` for `full_load`. |
| `partition_column` | No | Only set this if the source has a genuinely useful partitioning column (e.g. a date column on a large, frequently-filtered table). Leave `null` otherwise — partitioning a small table adds overhead with no benefit. |
| `columns` | Yes | One entry per column you want validated/cast. See Section 4. |

---

## 3. Choosing the right `load_pattern`

| Pattern | Use when... | What happens |
|---|---|---|
| `full_load` | The source always sends its complete, current dataset every time (e.g. a small reference table) | Silver table is fully overwritten on every run |
| `incremental` | The source sends only new/changed rows, identifiable by a date/timestamp column | New rows are appended; nothing is overwritten |
| `scd2` | You need to preserve full history of changes to this entity (e.g. a customer or employee record where past values matter) | Old versions are marked inactive, new versions are inserted, full history is kept |

If you're not sure, `incremental` is the safest default for anything that grows over time. Only use `scd2` if a past version of a row needs to remain queryable after it changes.

---

## 4. Defining columns

Every column you want the Silver layer to validate needs an entry under `columns`. Columns not listed here will still pass through to Silver, but will not be cast to a specific type or validated.

```json
{
  "name": "ColumnName",
  "target_type": "int",
  "rule": "not_null",
  "transformations": ["upper"]
}
```

- **`target_type`** — any valid Spark type string: `"int"`, `"bigint"`, `"string"`, `"date"`, `"timestamp"`, `"decimal(10,2)"`, etc.
- **`rule`** — one of:
  - `"not_null"` — row is rejected if this column is null after casting
  - `"positive"` — row is rejected if null, zero, or negative
  - `"referential"` — row is rejected if the value doesn't exist in another table (requires `ref_table` and `ref_column`, see below)
  - `"none"` — no validation, casting only
- **`transformations`** (optional) — a list of any of: `"lower"`, `"upper"`, `"initcap"`, `"digits_only"`, `"remove_special_chars"`, `"normalize_space"`. Applied in order, before validation.

### If a column needs a referential check

```json
{
  "name": "CustomerID",
  "target_type": "int",
  "rule": "referential",
  "ref_table": "retails.silver.customers",
  "ref_column": "CustomerID"
}
```

This rejects any row whose `CustomerID` doesn't exist in `retails.silver.customers`. If the reference table tracks history (has an `IsCurrent` column), the check automatically only matches against current rows — you don't need to do anything extra for this.

---

## 5. Uploading your raw files

Once the config entry is added:

1. Upload your source's files to the `landing_path` you specified — for CSV, make sure the first row is a header row matching your `columns` list.
2. Do **not** manually create the Bronze or Silver tables — they are created automatically on first write.
3. Do **not** reuse another source's `checkpoint_path` or `schema_location` folder — each source must have its own.

---

## 6. Running the pipeline

Once your config entry is saved and your files are uploaded:

- If running manually in Databricks: re-run `bronze_layer()` then `silver_layer()` — your new source will be picked up automatically as long as `"is_active": true`.
- If running via Azure Data Factory: no pipeline changes are needed. The next scheduled run will read the updated `config.json` automatically.

---

## 7. Disabling a source without deleting it

If you ever need to temporarily stop processing a source, set:

```json
"is_active": false
```

The pipeline will skip it entirely on every run, without needing to remove its configuration.

---

## Checklist before you commit

- [ ] `source_id` is unique
- [ ] `checkpoint_path` and `schema_location` are unique to this source
- [ ] `load_pattern` matches how this source actually sends data
- [ ] `primary_key` correctly identifies a unique row
- [ ] `watermark_column` is set if `load_pattern` is `incremental` or `scd2`
- [ ] Every column you need validated has an entry under `columns`
- [ ] Raw files are uploaded to `landing_path` with a matching header row
