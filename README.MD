# 🦆 DuckLake Delta Exporter

A Python utility to **bridge the gap between DuckLake and Delta Lake** by generating Delta-compatible transaction logs directly from DuckLake metadata.

This isn’t your typical general-purpose library. It’s mostly battle-tested with **OneLake mounted storage**, and while it *should* work with local filesystems, there’s **no support for S3, GCS, or ABFSS** — you're on your own there.

It doesn’t use the `deltalake` Python package either. The metadata is handcrafted from scratch — because why not reinvent the wheel for fun and learning?

**Goal?**  
Mostly to annoy DuckDB developers into finally shipping a proper Delta Lake metadata exporter (ideally using [`delta_kernel_rs`](https://github.com/delta-io/delta-kernel)). You're welcome 😎

🔗 [Source code on GitHub](https://github.com/djouallah/ducklake_delta_exporter)

---

## ✨ Features

- **DuckLake → Delta Sync**  
  Generates Delta Lake `_delta_log/*.json` transaction files and Parquet checkpoints from the latest DuckLake state.

- **Schema Mapping**  
  Converts DuckDB types to their Spark SQL equivalents so Delta can understand them without throwing a tantrum.

- **Change Detection**  
  Detects file-level additions/removals since the last export — keeps things incremental and tidy.

- **Checkpointing**  
  Automatically writes Delta checkpoints every N versions (configurable), so readers don’t have to replay the entire log from scratch.

---

## ⚙️ Installation & Usage

Install via pip:

```bash
pip install ducklake-delta-exporter
```

```
from ducklake_delta_exporter import generate_latest_delta_log

generate_latest_delta_log('/lakehouse/default/Files/meta.db')
```
