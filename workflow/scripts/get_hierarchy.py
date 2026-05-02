import polars as pl

spellchecked_metadata_df = pl.read_csv(snakemake.input.spellchecked_metadata)
geographical_hierarchy = [x.upper() for x in snakemake.params.geographical_hierarchy]
latitude_column = snakemake.params.latitude_column.upper()
longitude_column = snakemake.params.longitude_column.upper()
sample_column = snakemake.params.sample_column.upper()

geo_cols = [col for col in geographical_hierarchy if col in spellchecked_metadata_df.columns]
id_cols = [col for col in [sample_column, latitude_column, longitude_column] if col in spellchecked_metadata_df.columns]
other_cols = [col for col in spellchecked_metadata_df.columns if col not in set(geo_cols) and col not in set(id_cols)]

max_len = max(len(geo_cols), len(id_cols), len(other_cols), 1)
pad = lambda lst: lst + [None] * (max_len - len(lst))

column_map = pl.DataFrame({
    "geographical_columns": pad(geo_cols),
    "identifier_columns": pad(id_cols),
    "other_columns": pad(other_cols),
})

with pl.Config(tbl_rows=-1):
    print(column_map.fill_null(""))

sorted_metadata = (
    spellchecked_metadata_df
    .select(geo_cols + id_cols + other_cols)
    .sort(geo_cols + id_cols)
)

sorted_metadata.write_csv(snakemake.output.sorted_metadata)