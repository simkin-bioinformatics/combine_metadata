import polars as pl
import polars.selectors as cs
import os
# round the latitude and longitude columns to four decimal places


meta_21_file = "2021_metadata.csv"
meta_22_file = "2022_metadata.csv"
meta_23_file = "2023_metadata.csv"

input_files = [meta_21_file, meta_22_file, meta_23_file]

data_frames = []
for file_number, input_file in enumerate(input_files):
    df = pl.read_csv(input_file)
    for header in df.columns:
        new_header = header.replace(" ", "_").upper()
        df = df.rename({header: new_header})
    data_frames.append(df)

def rename_columns(data_frames):
    for i in range(1,len(data_frames)):
        rename = ""
        while rename != 'no':
            current_headers = data_frames[0].columns
            os.system('clear')            
            for j in range(i-1):
                current_headers.extend(data_frames[j].columns)
            rename_df = pl.DataFrame({'current_headers': current_headers})
            new_headers = [x for x in data_frames[i].columns if x not in current_headers]
            new_header_df = pl.DataFrame({'new_headers': new_headers})
            rename_df = pl.concat(
                [rename_df, new_header_df], how='horizontal'
            )
            with pl.Config(tbl_rows=-1):
                print(rename_df)
            rename = input(
                'Above is table of headers.\n'
                'It is possible that the headers in the metadata files do not \n'
                'match eachother perfectly.  This tool displays current headers\n'
                'on the left and any new headers it finds in subsequent files on\n'
                'the right.  You can rename any header you want, but it is especially\n'
                'important to look for headers on the left that are actually the same\n'
                'as those on the right but with a different name\n\n'
                'Would you like to rename any of these headers? (yes/no):\n'
            )
            if rename == 'yes':
                all_headers = set()
                for df in data_frames:
                    columns = set(df.columns)
                    all_headers.update(columns)
                original_header = input('Type the name of the header you want to rename: ').replace(" ", "_").upper()
                while original_header not in all_headers:
                    print('error, typed value not found')
                    original_header = input('Type the name of the header you want to rename: ').replace(" ", "_").upper()
                new_header = input('Type in the new name you want: ').replace(" ", "_").upper()
                for df_number, df in enumerate(data_frames):
                    if original_header in df.columns:
                        data_frames[df_number] = df.rename({original_header: new_header})
    big_df = pl.DataFrame()
    for df in data_frames:
        big_df = pl.concat([big_df, df], how='diagonal')
    big_df.write_csv('out1_renamed_columns.csv')

def filter_out_duplicate_samples(big_df_csv):
    big_df = pl.read_csv(big_df_csv)
    os.system('clear')
    headers = big_df.columns
    header_df = pl.DataFrame({'headers':headers})
    with pl.Config(tbl_rows=-1):
        print(header_df.sort('headers'))
    sample_names = input(
        'please type the name of the header that contains sample names: \n'
    ).replace(" ", "_").upper()    
    while sample_names not in headers:
        with pl.Config(tbl_rows=-1):
            print(header_df)
        print('error, entered value not in headers')
        sample_names = input(
            'please type the name of the header that contains sample names: \n'
        ).replace(" ", "_").upper()
    duplicated_df = big_df.filter((big_df.select(sample_names).is_duplicated())).sort(sample_names)
    removed_samples = duplicated_df.select(pl.col(sample_names)).group_by(pl.all()).len()
    print('removed samples')
    print(removed_samples)
    big_df = big_df.filter((big_df.select(sample_names).is_duplicated()).not_()).sort(sample_names)
    big_df.write_csv('out2_unique_samples.csv')
    duplicated_df.write_csv('out3_duplicated_sample_df.csv')
    print('created one output file with duplicated sample and one output file without')
    input('press any key to move on')

def look_for_coordinate_mismatches(big_df_csv):
    reassign = ''
    big_df = pl.read_csv(big_df_csv)
    headers = big_df.columns
    header_df = pl.DataFrame({'headers':headers})
    os.system('clear')
    with pl.Config(tbl_rows=-1):
        print(header_df)
    print('to ensure that each health facility has correct gsp coordinates,\n'
          'please type in the following info')
    hf_name = input('Health Facility Name Header: ').replace(" ","_").upper()
    while hf_name not in headers:
        print('error, typed value not found')
        hf_name = input('Health Facility Name Header: ').replace(" ","_").upper()
    lat = input('Latitude Header: ').replace(" ","_").upper()
    while lat not in headers:
        print('error, typed value not found')
        lat = input('Latitude Header: ').replace(" ","_").upper()
    lon = input('Longitude Header: ').replace(" ","_").upper()
    while lon not in headers:
        print('error, typed value not found')
        lon = input('Longitude Header: ').replace(" ","_").upper()
    while reassign != 'no':
        os.system('clear')
        filter_df = (big_df
            .select(hf_name, pl.col([lat, lon]).round(4))
            .cast({pl.Float64: pl.Decimal(scale=4)})
            .group_by(pl.all()).len()
            .sort(hf_name)
        )
        filter_df = filter_df.filter(filter_df.select(hf_name).is_duplicated())
        with pl.Config(tbl_rows=-1):
            print(filter_df)
        print('Above is a list of health facilities that have multiple coordinates associated with them\n')
        reassign = input('would you like to edit values? (yes/no)\n')
        while reassign not in ['yes', 'no']:
            print('please enter yes or no')
        if reassign == 'yes':
            health_facilities = big_df[hf_name]
            health_facility = input('Health Facility to Edit: ')
            while health_facility not in health_facilities:
                print('error, typed health facility not found')
                health_facility = input('Health Facility to Edit: ')
            new_lat = float(input('Correct Latitude: '))
            new_lon = float(input('Correct Longitude: '))
            big_df = big_df.with_columns(
                pl.when(pl.col(hf_name) == health_facility).then(new_lat)
                  .otherwise(lat).round(4).alias(lat),
                pl.when(pl.col(hf_name) == health_facility).then(new_lon)
                  .otherwise(lon).round(4).alias(lon),
            )
    reassign = ''
    while reassign != 'no':
        os.system('clear')
        filter_df = (big_df
            .select(hf_name, pl.col([lat, lon]).round(4))
            .cast({pl.Float64: pl.Decimal(scale=4)})
            .group_by(pl.all()).len()
            .sort(hf_name)
        )
        with pl.Config(tbl_rows=-1):
            print(filter_df)
        print('Above is a list of all health facilities and their coordinates\n')
        reassign = input('would you like to edit values? (yes/no)\n')
        if reassign == 'yes':
            health_facilities = big_df[hf_name]
            health_facility = input('Health Facility to Edit: ')
            while health_facility not in health_facilities:
                print('error, typed health facility not found')
                health_facility = input('Health Facility to Edit: ')
            new_lat = float(input('Correct Latitude: '))
            new_lon = float(input('Correct Longitude: '))
            big_df = big_df.with_columns(
                pl.when(pl.col(hf_name) == health_facility).then(new_lat)
                  .otherwise(lat).round(4).alias(lat),
                pl.when(pl.col(hf_name) == health_facility).then(new_lon)
                  .otherwise(lon).round(4).alias(lon),
            )
    big_df = big_df.with_columns(
        pl.col(lat).round(4).cast(pl.Decimal(scale=4)).alias(lat),
        pl.col(lon).round(4).cast(pl.Decimal(scale=4)).alias(lon),
    )
    big_df.write_csv('out4_fixed_coordinates.csv')

def look_for_different_versions(big_df_csv):
    big_df = pl.read_csv(big_df_csv).cast(pl.String)
    big_df = big_df.select(pl.all().str.replace(r'\s+','_'))
    big_df.write_csv('out5_inspected.csv')
    keep_going = ''
    def edit_df(big_df, filter_df):
        struct_df = (filter_df.select(pl.exclude('len')))
        struct_dict = struct_df.rows_by_key(key=['index'])
        row_index = input('row index to change: ')
        while int(row_index) not in struct_dict:
            row_index = input('please enter a valid index: ')
        checklist = struct_dict[int(row_index)]
        if type(checklist[0]) is tuple:
            checklist = checklist[0]
        change_header = input(f'Header to change: ({struct_df.select(pl.exclude('index')).columns}) ').replace(' ','_').upper()
        while change_header not in struct_df.columns:
            change_header = input('please enter a valid header: ').replace(' ','_').upper()
        new_value = input('change to: ')
        big_df = (
            big_df.with_columns(
                pl.when(pl.col(x).is_in(checklist) for x in chosen_headers)
                  .then(pl.lit(new_value))
                  .otherwise(change_header)
                  .alias(change_header)
            )
        )
        big_df.write_csv('out5_inspected.csv')
        return big_df

    while keep_going != 'no':
        os.system('clear')
        headers = big_df.columns
        header_df = pl.DataFrame({'headers':headers}).sort('headers')
        with pl.Config(tbl_rows=-1):
            print(header_df)
        keep_going = input('would you like to examine headers? (yes/no)\n')
        while keep_going not in (['yes', 'no']):
            print("please enter yes or no")
            keep_going = input('would you like to examine headers? (yes/no)\n')
        if keep_going == 'yes':
            chosen_headers = input(
                'choose headers to examine by typing in a comma separated list of headers.\n'
                'Choose the first header carefully as the values will be sorted on it\n'
            )
            chosen_headers = [x.strip().replace(' ', '_').upper() for x in chosen_headers.split(',')]
            for header_num, header in enumerate(chosen_headers):
                while header not in headers:
                    header = input(f'{header} not a valid header, please retype the header\n')
                    chosen_headers[header_num] = header
            change_index = 'show all'
            while change_index != 'no':
                if change_index == 'show duplicates':
                    filter_df = (big_df
                        .select(pl.col(chosen_headers))
                        .group_by(pl.all())
                        .len()
                        .sort(chosen_headers[0])
                        .with_row_index()
                    )
                    filter_df = filter_df.filter(filter_df.select(chosen_headers[0]).is_duplicated())
                    with pl.Config(tbl_rows=-1):
                        print(filter_df)
                    change_index = input('would you like to change any rows? (yes/no/show all)\n')
                    while change_index not in ['yes', 'no', 'show all']:
                        change_index = input('please type yes, no, or show all')
                    if change_index == 'yes':
                        big_df = edit_df(big_df, filter_df)
                        change_index = 'show duplicates'
                if change_index == 'show all':
                    filter_df = (big_df
                        .select(pl.col(chosen_headers))
                        .group_by(pl.all())
                        .len()
                        .sort(chosen_headers[0])
                        .with_row_index()
                    )
                    with pl.Config(tbl_rows=-1):
                        print(filter_df)
                    change_index = input('would you like to change any rows? (yes/no/show duplicates)\n')
                    while change_index not in ['yes', 'no', 'show duplicates']:
                        change_index = input('please type yes, no, or show duplicates')
                    if change_index == 'yes':
                        big_df = edit_df(big_df, filter_df)
                        change_index = 'show all'

    big_df.write_csv('out5_inspected.csv')

if not os.path.isfile('out1_renamed_columns.csv'):
    rename_columns(data_frames)
if not os.path.isfile('out2_unique_samples.csv'):
    filter_out_duplicate_samples('out1_renamed_columns.csv')
if not os.path.isfile('out4_fixed_coordinates.csv'):
    look_for_coordinate_mismatches('out2_unique_samples.csv')
look_for_different_versions('out5_inspected.csv')
