#!/usr/bin/env python3

import argparse
import pandas as pd
import numpy as np

# argpase used to take in command line arguments
def get_opts():
    p = argparse.ArgumentParser(description = 'This program reads in a tsv of sequence metadata and performs some reformatting and data sanitization then spits out a tsv to be uploaded to terra.bio', usage='[-h] metadata_cleanser.py <metadata_file.tsv> <outfile_name> <table_name> <optional_inputs>')
    p.add_argument('tsv_meta_file', help='tsv metadata file input')
    p.add_argument('out_file', help='Output file: required, must be a string.')
    args = p.parse_args()
    return args
arguments = get_opts()

# read in metadata tsv file
meta_tsv1 = arguments.tsv_meta_file
meta_df1 = pd.read_csv(meta_tsv1, delimiter='\t')

# replacing blank values with nan
meta_df1_nan_added = meta_df1.replace(r'^\s+$', np.nan, regex=True)
print(meta_df1_nan_added)


# sort on these 3 columns, in this order
# NaN cells are first so that larger %ref coverage values are last in the list
meta_df1_sorted = meta_df1_nan_added.sort_values(by=['specimen_accession_number','gisaid_accession','percent_reference_coverage'], ascending=True, na_position='first')
print(meta_df1_sorted)

# step to remove SOME of the duplicates
# duplicates where sequencing_lab and specimen_accession_number is the same - remove these
# do NOT remove duplicate specimen_accession_numbers if they come from different labs
# ignore the NaN values, do not treat NaNs as duplicates
meta_df1_sorted_dups_removed = meta_df1_sorted[(~meta_df1_sorted.duplicated(subset=[ 'sequencing_lab', 'specimen_accession_number'], keep='last')) | meta_df1_sorted['specimen_accession_number'].isna()]
print(meta_df1_sorted_dups_removed)

# Get outfile name
out_file_name = arguments.out_file

# Print to tsv file
meta_file_out = meta_df1_sorted_dups_removed.to_csv(out_file_name, sep="\t", index=False)
