import polars as pl
# import polars.selectors as cs
import os

columns_to_rename = snakemake.config['columns_to_rename']
concatenated_metadata = snakemake.input.concatenated_metadata
spellchecked_metadata = snakemake.output.spellchecked_metadata

df = pl.read_csv(concatenated_metadata)
for old_name in columns_to_rename.keys():
    if old_name.upper() in df.columns:
        df = df.rename({old_name.upper(): columns_to_rename[old_name].upper()})

spellcheck_series = pl.DataFrame({"spellcheck": [col for col in df.columns]})
with pl.Config(tbl_rows=-1):
    print(spellcheck_series)
df.write_csv(spellchecked_metadata)