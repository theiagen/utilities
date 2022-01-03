#!/usr/bin/env python3

# import sys
# import csv
import argparse
import pandas as pd
#argpase used to take in command line arguments
# three positional arguments, argparse might be overkill, sys command included
def get_opts():
	p = argparse.ArgumentParser(description = 'This program reads in a tsv of sequence metadata and performs some reformatting and data sanitization then spits out a tsv to be uploaded to terra.bio', usage='[-h] metadata_cleanser.py <metadata_file.csv> <ZipCode_County_Lookup_Table> <outfile_name>')
	p.add_argument('csv_meta_file',
				help='tsv metadata file input')
	p.add_argument('out_file',
				help='Output file: required, must be a string.')
	args = p.parse_args()
	return args
arguments = get_opts()

# read in metadata csv file
meta_csv1 = arguments.csv_meta_file
meta_df1 = pd.read_csv(meta_csv1, delimiter='\t', dtype={'strain': str})

# input_headers = meta_df1.columns.values
output_headers = ['entity:gisaid_louisiana_data_id', 'age', 'authors', 'country', 'country_exposure', 'date', 'date_submitted', 'division', 'division_exposure', 'GISAID_clade', 'gisaid_epi_isl', 'host', 'location', 'originating_lab', 'pangolin_lineage', 'region', 'region_exposure', 'segment', 'sex', 'submitting_lab', 'url', 'virus', 'gisaid_accession', 'nextclade_clade', 'gisaid_clade']

# rename headers
meta_df1.rename(columns={'strain': 'entity:gisaid_louisiana_data_id', 'GISAID_accession': 'gisaid_accession', 'Nextstrain_clade': 'nextclade_clade', 'vendor': 'sequencing_lab', 'zip': 'county', 'GISAID_clade': 'gisaid_clade'}, inplace=True)

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
meta_df1['entity:gisaid_louisiana_data_id'].replace('/', value='_', regex=True, inplace=True)

# replace all commas with spaces
meta_df1.replace(',', value=' ', regex=True, inplace=True)

# replace all 'Unknown' with 'unknown'
meta_df1.replace('Unknown', value='unknown', regex=True, inplace=True)

# replace all '_' with '-' in collection date cols
meta_df1['date'].replace('_', value='-', regex=True, inplace=True)
meta_df1['date_submitted'].replace('_', value='-', regex=True, inplace=True)


# remove the word 'years' from the age column
meta_df1['age'].replace(' years', value='', regex=True, inplace=True)


# age column cleaning
# replace string inputs of age ranges with individual numerical age equivalent to the bottom of the bins
age_range_replace_dict = {'0-4': 4, '5-17': 5, '18-49': 18, '50-64': 50}
meta_df1['age'].replace(age_range_replace_dict, inplace=True)

# replace all NA values with numerical value 151
meta_df1['age'] =pd.to_numeric(meta_df1['age'], errors ='coerce').fillna(151).astype('int')

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
meta_df1.drop_duplicates(subset='entity:gisaid_louisiana_data_id', keep='first', inplace=True)

# Get outfile name
out_file_name = arguments.out_file

# Print to tsv file
meta_file_out = meta_df1.to_csv(out_file_name, sep="\t", index=False)

#print to stdout
print(meta_df1)
