import polars as pl
import os
os.system("clear")

spellchecked_metadata_df = pl.read_csv(snakemake.input.spellchecked_headers, infer_schema=False)
hierarchy = [x for x in snakemake.params.geographical_hierarchy if x is not None]
sample_column = snakemake.params.sample_column
latitude_column = snakemake.params.latitude_column
longitude_column = snakemake.params.longitude_column

id_columns = [col.upper() for col in [sample_column, latitude_column, longitude_column] if col.upper() in spellchecked_metadata_df.columns]
hierarchy_columns = [col.upper() for col in hierarchy if col.upper() in spellchecked_metadata_df.columns]
other_cols = [col for col in spellchecked_metadata_df.columns if col not in set(hierarchy_columns).union(id_columns)]
other_cols.sort()

max_len = max(len(hierarchy_columns), len(other_cols), 1)
pad = lambda lst: lst + [None] * (max_len - len(lst))

column_map = pl.DataFrame({
    "geographical_hierarchy": pad(hierarchy_columns),
    "sample, latitude, longitude": pad(id_columns),
    "other_columns": pad(other_cols),
})

user_message = """
The table below reflects the chosen geographical hierarchy in the left column, the sample id, latitude,
and longitude in the middle column, and all other "extra" columns on the right.  Edit the config file
until the first two columns look good.
"""
print(user_message)
with pl.Config(tbl_rows=-1):
    print(column_map.fill_null(""))

sorted_metadata = (
    spellchecked_metadata_df
    .select(hierarchy_columns + id_columns + other_cols)
    .sort(hierarchy_columns + id_columns)
)

if all(item in sorted_metadata.columns for item in [latitude_column.upper(), longitude_column.upper()]):
    sorted_metadata = sorted_metadata.with_columns(
        pl.col(latitude_column.upper(), longitude_column.upper()).cast(pl.Float64).round(3)
    )

sorted_metadata.write_csv(snakemake.output.sorted_metadata)