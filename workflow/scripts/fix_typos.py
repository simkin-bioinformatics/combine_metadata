import polars as pl

sorted_metadata = pl.read_csv(snakemake.input.sorted_metadata, infer_schema=False)
spellcheck_geography_dict = snakemake.params.spellcheck_geography_dict
geographical_hierarchy = [x.upper() for x in snakemake.params.geographical_hierarchy]

spellchecked_geography = sorted_metadata.cast(pl.String).with_columns(
    pl.col(pl.String).str.strip_chars()
)

for row_key, replacements in spellcheck_geography_dict.items():
    row_key_str = str(row_key)
    if '-' in row_key_str:
        start, end = row_key_str.split('-')
        start_row, end_row = int(start) - 2, int(end) - 2
    else:
        start_row = end_row = int(row_key_str) - 2

    for old_val, new_val in replacements.items():
        spellchecked_geography = spellchecked_geography.with_columns([
            pl.when(
                pl.int_range(pl.len()).is_between(start_row, end_row) & (pl.col(c) == old_val)
            )
            .then(pl.lit(new_val))
            .otherwise(pl.col(c))
            .alias(c)
            for c in spellchecked_geography.columns
        ])

if snakemake.params.columns_to_delete > 0: 
    del geographical_hierarchy[-snakemake.params.columns_to_delete:]
unique_geo = (
    spellchecked_geography
    .with_row_index(name="row_numbers", offset=2)
    .select(["row_numbers"] + geographical_hierarchy)
    .group_by(geographical_hierarchy)
    .agg(
        pl.col("row_numbers").min().alias("first_row"),
        pl.col("row_numbers").max().alias("last_row"),
    )
    .with_columns(
        (pl.col("first_row").cast(pl.String) + "-" + pl.col("last_row").cast(pl.String)).alias("row_numbers")
    )
    .drop("first_row", "last_row")
    .select(["row_numbers"] + geographical_hierarchy)
    .sort(geographical_hierarchy)
)
df_dupes = unique_geo.sort(pl.col(geographical_hierarchy[-1])).filter(
    pl.col(geographical_hierarchy[-1]).is_duplicated()
)
with pl.Config(tbl_rows=-1):
    print(df_dupes.fill_null(""))

spellchecked_geography.write_csv(snakemake.output.spellchecked_geo)
