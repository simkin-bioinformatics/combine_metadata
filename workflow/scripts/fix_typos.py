import polars as pl
import math
import os
os.system('clear')

sorted_metadata = pl.read_csv(snakemake.input.sorted_metadata, infer_schema=False)
spellcheck_geography_dict = snakemake.params.spellcheck_geography_dict
geographical_hierarchy = [x.upper() for x in snakemake.params.geographical_hierarchy]
coordinates = [snakemake.params.latitude_column.upper(), snakemake.params.longitude_column.upper()]
close_threshold = snakemake.params.close_threshold
sample_column = snakemake.params.sample_column.upper()

spellchecked_geography = sorted_metadata.with_columns(
    pl.col("*").str.strip_chars()
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
                pl.int_range(pl.len()).is_between(start_row, end_row) & (pl.col(c) == str(old_val))
            )
            .then(pl.lit(str(new_val)))
            .otherwise(pl.col(c))
            .alias(c)
            for c in spellchecked_geography.columns
        ])



grouping_cols = geographical_hierarchy + coordinates
unique_geo = (
    spellchecked_geography
    .with_row_index(name="row_numbers", offset=2)
    .select(["row_numbers"] + grouping_cols)
    .group_by(grouping_cols)
    .agg(
        pl.col("row_numbers").min().alias("first_row"),
        pl.col("row_numbers").max().alias("last_row"),
    )
    .with_columns(
        (pl.col("first_row").cast(pl.String) + "-" + pl.col("last_row").cast(pl.String)).alias("row_numbers")
    )
    .drop("first_row", "last_row")
    .select(["row_numbers"] + grouping_cols)
    .sort(grouping_cols)
)

_R_MILES = 3958.8
_lat, _lon = coordinates[0], coordinates[1]
_geo = (
    unique_geo
    .with_row_index("_idx")
    .select(["_idx", _lat, _lon])
    .with_columns([pl.col(_lat).cast(pl.Float64), pl.col(_lon).cast(pl.Float64)])
)
_close_idx = (
    _geo
    .join(_geo, how="cross", suffix="_r")
    .filter(pl.col("_idx") != pl.col("_idx_r"))
    .with_columns([
        (pl.col(_lat) * math.pi / 180).alias("_lat1"),
        (pl.col(_lon) * math.pi / 180).alias("_lon1"),
        (pl.col(_lat + "_r") * math.pi / 180).alias("_lat2"),
        (pl.col(_lon + "_r") * math.pi / 180).alias("_lon2"),
    ])
    .with_columns(
        (
            ((pl.col("_lat2") - pl.col("_lat1")) / 2).sin().pow(2)
            + pl.col("_lat1").cos() * pl.col("_lat2").cos()
            * ((pl.col("_lon2") - pl.col("_lon1")) / 2).sin().pow(2)
        ).alias("_a")
    )
    .filter(pl.col("_a").sqrt().arcsin() * 2 * _R_MILES < close_threshold)
    .select("_idx")
    .unique()
)
unique_geo_with_close = (
    unique_geo
    .with_row_index("_idx")
    .with_columns(
        pl.when(pl.col("_idx").is_in(_close_idx["_idx"].to_list()))
        .then(pl.lit("close"))
        .otherwise(pl.lit(""))
        .alias("close")
    )
    .drop("_idx")
)

def print_duplicates(columns_to_delete):
    if columns_to_delete == 0:
        grouping_cols = geographical_hierarchy.copy() + coordinates
        duplicate_check = geographical_hierarchy[-1]
        sorting_col = pl.col(geographical_hierarchy[-1])
    else:
        grouping_cols = geographical_hierarchy.copy()
        del grouping_cols[-(columns_to_delete):]
        duplicate_check = grouping_cols[-1]
        sorting_col = pl.col(grouping_cols[-1])

    df_dupes = (
        spellchecked_geography
        .with_row_index(name="row_numbers", offset=2)
        .select(["row_numbers"] + grouping_cols)
        .group_by(grouping_cols)
        .agg(
            pl.col("row_numbers").min().alias("first_row"),
            pl.col("row_numbers").max().alias("last_row"),
        )
        .with_columns(
            (pl.col("first_row").cast(pl.String) + "-" + pl.col("last_row").cast(pl.String)).alias("row_numbers")
        )
        .drop("first_row", "last_row")
        .select(["row_numbers"] + grouping_cols)
        .filter(pl.col(duplicate_check).is_duplicated())
        .sort(sorting_col)
    )

    with pl.Config(tbl_rows=-1, tbl_cols=-1):
        print(
            f"The table below shows {duplicate_check} duplicates with different hierarchichal parents\n"
            f"If the table is not empty it means that there is an error in which one {duplicate_check}\n"
            f"is listed under multiple coordinates or parent geographical categories.  Be sure to edit\n"
            f"the config file and keep re-running this step until the table is empty"
        )
        print(df_dupes.fill_null(""))

print(
    f"The table below shows {geographical_hierarchy[-1]} that are within {close_threshold} miles of another {geographical_hierarchy[-1]}\n"
    f"Check it to see if there are two different names for the same {geographical_hierarchy[-1]}.\n"
)
with pl.Config(tbl_rows=-1, tbl_cols=-1):
    print(unique_geo_with_close.filter(pl.col("close") == "close"))
for i in range(0, len(geographical_hierarchy) - 1):
    print_duplicates(i)

unique_samples = spellchecked_geography.filter(pl.col(sample_column).is_unique())
duplicated_samples = spellchecked_geography.filter(pl.col(sample_column).is_duplicated())
unique_samples.write_csv(snakemake.output.unique_samples)
duplicated_samples.write_csv(snakemake.output.duplicated_samples)
unique_geo.write_csv(snakemake.output.fixed_typos_summary)
