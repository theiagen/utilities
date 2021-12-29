#!/Users/frank/opt/anaconda3/bin/python

# import sys
# import csv
import argparse
import pandas as pd
import re
#argpase used to take in command line arguments
# three positional arguments, argparse might be overkill, sys command included
def get_opts():
	p = argparse.ArgumentParser(description = 'This program reads in a csv of sequence metadata and performs some reformatting and data sanitization then spits out a tsv to be uploaded to terra.bio', usage='[-h] metadata_cleanser.py <metadata_file.csv> <ZipCode_County_Lookup_Table> <outfile_name>')
	p.add_argument('csv_meta_file',
				help='csv metadata file input')
	p.add_argument('county_zipcodes_file',
				help='csv file containing columns mapping zipcodes to county')
	p.add_argument('out_file',
				help='Output file: required, must be a string.')
	args = p.parse_args()
	return args
arguments = get_opts()

# read in metadata csv file
meta_csv1 = arguments.csv_meta_file
meta_df1 = pd.read_csv(meta_csv1, dtype={'zip': int}, error_bad_lines=False, index_col=False)

# set all zipcode types to int
# replace all 'Unknown' entries with 0
meta_df1['zip'].replace('Unknown', value=0, regex=True, inplace=True)
# split all entries with '-' and seperate into two new cols
meta_df1[['zip1','zip2']] = meta_df1.zip.str.split('-', expand=True)
# replace all NA values with 0
meta_df1['zip1'].fillna(value=0, inplace=True)
meta_df1.astype({'zip1': 'int'}).dtypes

# read in zipcode county lookup table
zip_csv1 = arguments.county_zipcodes_file
zip_df1 = pd.read_csv(zip_csv1, dtype={'ZipCode': int})

# make a dictionary for fast zip/county lookups
zip_county_lookup_dict = dict(zip(zip_df1.ZipCode, zip_df1.County))

# add new column 'county' mapped from zip1
meta_df1['county'] = meta_df1['zip1'].map(zip_county_lookup_dict)

# input_headers = meta_df1.columns.values
output_headers = ['entity:cdc_specimen_id', 'collection_date', 'county', 'gisaid_accession', 'nextclade_clade', 'pango_lineage', 'sequencing_lab', 'state', 'zip', 'zip1', 'zip2']

# rename headers
meta_df1.rename(columns={'vendor_accession': 'entity:cdc_specimen_id', 'GISAID_accession': 'gisaid_accession', 'clade_Nextclade_clade': 'nextclade_clade', 'lineage_PANGO_lineage': 'pango_lineage', 'vendor': 'sequencing_lab'}, inplace=True)

# replace all


# for i in meta_df1['county']:
# 	if re.match('*-*', i):
# 		i = i.split('-')[0]
#
# meta_df1 = pd.to_numeric(meta_df1['county'])




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
meta_df1.replace('/', value='_', regex=True, inplace=True)

# replace all commas with underscores
meta_df1.replace(',', value='_', regex=True, inplace=True)

# replace all 'Unknown' with 'unknown'
meta_df1.replace('Unknown', value='unknown', regex=True, inplace=True)

# replace all '_' with '-' in collection date cols
meta_df1['collection_date'].replace('_', value='-', regex=True, inplace=True)

# Get outfile name
out_file_name = arguments.out_file

# Print to tsv file
meta_file_out = meta_df1.to_csv(out_file_name, sep="\t", index=False)

#print to stdout
# print(zip_county_lookup_dict)
