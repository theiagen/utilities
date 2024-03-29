#!/usr/bin/env python3

import argparse
import pandas as pd

#argpase used to take in command line arguments
def get_opts():
    p = argparse.ArgumentParser(description = 'This program reads in a tsv of sequence metadata and performs some reformatting and data sanitization then spits out a tsv to be uploaded to terra.bio', usage='[-h] metadata_cleanser.py <metadata_file.tsv> <outfile_name> <table_name> <optional_inputs>')
    p.add_argument('tsv_meta_file', help='tsv metadata file input')
    p.add_argument('out_file', help='Output file: required, must be a string.')
    p.add_argument('table_name', help='Terra table name: required, must be a string; do not include entity: or _id.')
    p.add_argument('puertorico', help='Perform Puerto Rico-specific actions')
    p.add_argument('helix', help='Perform Helix-specific actions')
    args = p.parse_args()
    return args
arguments = get_opts()

# read in metadata tsv file
meta_tsv1 = arguments.tsv_meta_file
meta_df1 = pd.read_csv(meta_tsv1, delimiter='\t', dtype={'strain': str, 'age': str})

table_name = "entity:" + arguments.table_name + "_id"

# input_headers = meta_df1.columns.values
output_headers = [table_name, 'age', 'authors', 'country', 'country_exposure', 'date_submitted', 'division', 'division_exposure', 'GISAID_clade', 'gisaid_epi_isl', 'host', 'location', 'originating_lab', 'pango_lineage', 'region', 'region_exposure', 'segment', 'sex', 'submitting_lab', 'url', 'virus', 'gisaid_accession', 'nextclade_clade', 'gisaid_clade', 'county', 'collection_date']

# rename headers
meta_df1.rename(columns={'strain': table_name, 'gisaid_epi_isl': 'gisaid_accession', 'Nextstrain_clade': 'nextclade_clade', 'vendor': 'sequencing_lab', 'location': 'county', 'GISAID_clade': 'gisaid_clade', 'pangolin_lineage': 'pango_lineage', 'date': 'collection_date'}, inplace=True)

# perform PR specific actions:
if arguments.puertorico == "true":
    # drop pangolin lineage column
    meta_df1.drop('pango_lineage', axis='columns', inplace=True)
    # remove any samples uploaded by PR
    meta_df1 = meta_df1[~meta_df1[table_name].str.contains("PR-CVL")]

# perform Helix specific actions:
if arguments.helix == "true":
    # rename virus names to start after the `hCoV-10/USA/CA-` prefix
    meta_df1[table_name] = meta_df1[table_name].str.replace('hCoV-19/USA/CA-', '')
    meta_df1[table_name] = meta_df1[table_name].str[:-5]

# drop extraneous cols
drop_list = []
for i in meta_df1.columns.values:
	if i not in output_headers:
		drop_list.append(i)
meta_df1.drop(drop_list, axis='columns', inplace=True)

# replace all NA values with the string 'unknown'
meta_df1.fillna(value='unknown', inplace=True)

# replace all newline characters with spaces
meta_df1.replace("\n", value=' ', regex=True, inplace=True)

# replace all forward slashes in first  with underscores
meta_df1[table_name].replace('/', value='_', regex=True, inplace=True)
meta_df1[table_name].replace('\|', value='_', regex=True, inplace=True) # prevent accidental piping

# replace all commas with spaces
meta_df1.replace(',', value=' ', regex=True, inplace=True)

# replace all 'Unknown' with 'unknown'
meta_df1.replace('Unknown', value='unknown', regex=True, inplace=True)

# replace all '_' with '-' in collection date cols
meta_df1['collection_date'].replace('_', value='-', regex=True, inplace=True)
meta_df1['date_submitted'].replace('_', value='-', regex=True, inplace=True)

# remove the word 'years' from the age column
meta_df1['age'].replace(' years', value='', regex=True, inplace=True)

# age column cleaning
# replace string inputs of age ranges with individual numerical age equivalent to the bottom of the bins
age_range_replace_dict = {'0-4': 4, '5-17': 5, '18-49': 18, '50-64': 50}
meta_df1['age'].replace(age_range_replace_dict, inplace=True)

# replace all NA values with numerical value 151
meta_df1['age'] = pd.to_numeric(meta_df1['age'], errors ='coerce').fillna(151).astype('int')

# set bin boundaries
bins1 = [0, 4, 17, 49, 64, 123, 1000000]

# give bins labels
labels1 = ['0-4', '5-17', '18-49', '50-64', '65<', 'unknown']

# perform binning
meta_df1['age_bins'] = pd.cut(x=meta_df1['age'], bins=bins1, labels=labels1, include_lowest=True)

# replace all values >151 with unknown
meta_df1['age'].replace(151, 'unknown', inplace=True)

# replace all NA values with unknown
meta_df1['age_bins'] = meta_df1['age_bins'].fillna('unknown')

# remove duplicate lines, keeping the first values
meta_df1.drop_duplicates(subset=table_name, keep='first', inplace=True)

# Get outfile name
out_file_name = arguments.out_file

# Print to tsv file
meta_file_out = meta_df1.to_csv(out_file_name, sep="\t", index=False)

#print to stdout
print(meta_df1)
