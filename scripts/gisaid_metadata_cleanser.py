#!/usr/bin/env python3
# import sys
# import csv
import argparse
import pandas as pd
#argpase used to take in command line arguments
# two positional arguments, argparse might be overkill, sys command included
def get_opts():
	p = argparse.ArgumentParser(description = 'This program reads in a tsv of sequence metadata and performs some reformatting and data sanitization then spits out a tsv to be uploaded to terra.bio', usage='[-h] metadata_cleanser.py -i <metadata_file.tsv> -o <outfile_name>')
	p.add_argument('-i',
				'--input_tsv',
				dest='tsv_meta_file',
				help='tsv metadata file input, downloaded from GISAID "input for Augur"')
	p.add_argument('-e',
				'--entity',
				dest='root_entity',
				help='name of the Terra data table to which the metadata will be added (no _id required)')
	p.add_argument('-o',
				'--output',
				default='gisaid_metadata_cleansed.tsv',
				dest='out_file',
				help='Output file: required, must be a string.')
	args = p.parse_args()
	return args
arguments = get_opts()

# read in the root entity for the terra data table
root_entity_name1 = arguments.root_entity

# read in metadata tsv file
meta_tsv1 = arguments.tsv_meta_file
meta_df1 = pd.read_csv(meta_tsv1, delimiter='\t', dtype={'strain': str, 'age': str})

# input_headers = meta_df1.columns.values
output_headers = ['entity:{}_id'.format(root_entity_name1), 'age', 'authors', 'country', 'country_exposure', 'date_submitted', 'division', 'division_exposure', 'GISAID_clade', 'host', 'location', 'originating_lab', 'region', 'region_exposure', 'segment', 'sex', 'submitting_lab', 'gisaid_accession', 'gisaid_clade', 'collection_date']

# rename headers
meta_df1.rename(columns={'strain': 'entity:{}_id'.format(root_entity_name1), 'gisaid_epi_isl': 'gisaid_accession', 'GISAID_clade': 'gisaid_clade', 'pangolin_lineage': 'pango_lineage', 'date': 'collection_date'}, inplace=True)

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
meta_df1['entity:{}_id'.format(root_entity_name1)].replace('/', value='_', regex=True, inplace=True)

# replace all commas with spaces
meta_df1.replace(',', value=' ', regex=True, inplace=True)

# replace all 'Unknown' with 'unknown'
meta_df1.replace('Unknown', value='unknown', regex=True, inplace=True)

# replace all '_' with '-' in collection date cols
meta_df1['collection_date'].replace('_', value='-', regex=True, inplace=True)
meta_df1['date_submitted'].replace('_', value='-', regex=True, inplace=True)

# remove the word 'years' from the age column
meta_df1['age'].replace(' years', value='', regex=True, inplace=True)


# Get outfile name
out_file_name = arguments.out_file

# Print to tsv file
meta_file_out = meta_df1.to_csv(out_file_name, sep="\t", index=False)

#print to stdout
print(meta_df1)
