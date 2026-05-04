import polars as pl
import os

columns_to_rename = snakemake.config['columns_to_rename']
concatenated_metadata = snakemake.input.concatenated_metadata
spellchecked_headers = snakemake.output.spellchecked_headers

df = pl.read_csv(concatenated_metadata, infer_schema=False)
for old_name in columns_to_rename.keys():
    if old_name.upper() in df.columns:
        df = df.rename({old_name.upper(): columns_to_rename[old_name].upper()})

spellcheck_series = pl.DataFrame({"spellcheck": [col for col in df.columns]}).sort('spellcheck')
with pl.Config(tbl_rows=-1):
    print(spellcheck_series)
df = df.select(sorted(df.columns))
df.write_csv(spellchecked_headers)