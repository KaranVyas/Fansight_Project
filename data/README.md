# Data Directory

This folder is intentionally empty. Use it to store club-specific datasets:

- `raw/` — unmodified exports from ticketing, CRM, marketing, sponsorship, and external sources (CSV/Parquet/etc.).
- `processed/` — cleaned feature tables produced by `fansight.data.etl.build_and_save_dataset`.

To generate small synthetic files for experimentation, run:

```bash
python -m fansight.scripts.generate_sample_data
```

The pipeline then reads from `data/processed/`. Remove the generated CSVs before committing real projects.
