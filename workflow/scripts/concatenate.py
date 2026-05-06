import polars as pl
# import polars.selectors as cs
import os

input_files = snakemake.input.metadata_files
columns_to_rename = snakemake.config['columns_to_rename'] or {"old_name": "new_name"}
concatenated_metadata = snakemake.output.concatenated_metadata

data_frames = []
for file_number, input_file in enumerate(input_files):
    df = pl.read_csv(input_file, infer_schema=False)
    df = df.unique()
    df = df.rename(lambda col: col.strip().upper())
    for old_name in columns_to_rename.keys():
        if old_name.upper() in df.columns:
            df = df.rename({old_name.upper(): columns_to_rename[old_name].upper()})
    data_frames.append(df)


column_check = pl.DataFrame()
shared_columns = set(data_frames[0].columns)
for df in data_frames[1:]:
    shared_columns = shared_columns.intersection(df.columns)

for i, df in enumerate(data_frames):
    columns_in_df = set(df.columns)
    unique_columns = columns_in_df - shared_columns
    new_col = pl.Series(name=os.path.basename(input_files[i]), values=list(unique_columns)).sort().to_frame()
    column_check = pl.concat([column_check, new_col], how="horizontal")

os.system('clear')
user_message = """
Each column in the table below shows all of the categories contained in the metadata file 
that are not present in all three metadata files.  Check it over carefully to see if any
categories actually describe the same thing.  Rename them in the config file in the 
"columns_to_rename" section.  If the table below is empty it means all input metadata files have 
identical headers.
"""
print(user_message)
with pl.Config(tbl_rows=-1):
    print(column_check.fill_null(""))

concatenated_df = pl.concat(data_frames, how="diagonal")
concatenated_df.write_csv(concatenated_metadata)