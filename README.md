## concatenate metadata files

1. input all metadata file paths
2. examine metadata files and rename columns to match across files
3. merge any rows that are exact duplicates of eachother
4. concatenate files 

## remove duplicate samples

1. ask for sample name column,
2. check for duplicated sample names in the concatenated file, output once csv per duplicated sample name
3. output csv of all of the unique sample names
4. give user opportunity to edit the duplicated samples and merge them back in

## check for hierarchichal discrepancies
1. show user a list of all columns and ask for a hierarchy of location based columns, including codes (country>region>region_code>district>district_code>health_facility>hf_code)
2. Check for typos by showing an alphabetical list of unique values for each category and asking user if they want to rename any
3. starting from the top, check to see if any children have multiple parents, if so show them to user and ask if they would like to rename the child.  

## check for coordinate discrepancies
1. ask user for the most granular location column (eg health_facility)
2. ask user for latitude and longitude column names
3. check for discrepancies where health facility has multiple coordinates listed and give user opportunity to correct them