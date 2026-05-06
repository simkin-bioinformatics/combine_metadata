Be sure you using a computer that has pixi installed

1. Clone this reopository.  Open a terminal and cd into the cloned directory.
2. Edit the config metadata_files paths to point to the metadata file or files that need to be concatenated and cleaned up.
3. Edit the output_folder path or leave it at the default "results"
4. Open a terminal, cd into the cloned directory and run "pixi run concatenate".  Examine the terminal output carefully and change the configfile as appropriate.
5. Repeat the last step until you are satisfied with the results
6. Run "pixi run get_hierarchy".  Examine the output, edit the config, and rerun until you are satisfied
7. Run "pixi run fix_typos".  Examine the output, edit the config, and rerun
8. Check the "fixed_typos_duplicated_samples.csv" output.  If this file is not empty it means that the sample names in the input files were not unique.  To fix this error you will need to edit the original input files
9. Check the "fixed_typos_summary.csv" output.  It shows the complete output for each collection site in your metadata after the typos have been fixed.  If any rows do not seem right you can edit them in the config file and rerun.

The final output that should be used as the cleaned up metadata is the file called "fixed_typos_unique_samples.csv".  It contains all of the samples from the original input files with their cleaned up metadata.