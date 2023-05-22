#!/usr/bin/env python3

import argparse
import pandas as pd

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
meta_df1 = pd.read_csv(meta_tsv1, delimiter='\t', na_filter=False)

# input_headers = meta_df1.columns.values
output_headers = ['entity:county_specimen_updated_id', 'assembly_mean_coverage', 'gisaid_accession', 'percent_reference_coverage', 'sequencing_lab', 'specimen_accession_number']

# remove duplicate lines, keeping the first values
#meta_df1.drop_duplicates(subset=table_name, keep='first', inplace=True)

# get list of duplicate specimen_accession_numbers (subset across whole set). keep = false
#duplicated_specimens = meta_df1.duplicated(subset=['specimen_accession_number'], keep=False)

#duplicated_specimens.sort_values(by=['gisaid_accession','percent_reference_coverage'])

meta_df1_sorted = meta_df1.sort_values(by=['specimen_accession_number','gisaid_accession','percent_reference_coverage'], ascending=True)
print(meta_df1_sorted)

meta_df1_sorted_dups_removed = meta_df1_sorted.drop_duplicates(subset=['specimen_accession_number'],keep='last',ignore_index=True)
print(meta_df1_sorted_dups_removed)

# rename sample ID header
#meta_df1_sorted_dups_removed.rename(columns={'entity:county_specimen_updated_id': 'county_specimen_updated_id'}, inplace=True)


# get a list of specimen_accession_numbers that are duplicated

# for each of those numbers, do the following:
### look for gisaid_accession, if only one exists. take that accession & sample
### if 2 or more gisaid_accessions exist, what do????
### if NO gisaid_accessions exist, enter new conditional
###### keep/retain sample with highest percent_ref_coverage, delete/toss others with lower coverage





# Get outfile name
out_file_name = arguments.out_file

# Print to tsv file
meta_file_out = meta_df1_sorted_dups_removed.to_csv(out_file_name, sep="\t", index=False)

#print to stdout
#print(meta_df1)
