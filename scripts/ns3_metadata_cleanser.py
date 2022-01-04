#!/usr/bin/env python3

# import sys
# import csv
import argparse
import pandas as pd
import re
#argpase used to take in command line arguments
# three positional arguments, argparse might be overkill, sys command included
def get_opts():
	p = argparse.ArgumentParser(description = 'This program reads in a csv of sequence metadata and performs some reformatting and data sanitization then spits out a tsv to be uploaded to terra.bio', usage='[-h] metadata_cleanser.py <metadata_file.csv> <target_terra_data_table_name> <ZipCode_County_Lookup_Table> <outfile_name>')
	p.add_argument('csv_meta_file',
				help='csv metadata file input')
	p.add_argument('root_entity',
				help='name of the Terra data table to which the metadata will be added (no _id required)')
	p.add_argument('county_zipcodes_file',
				help='csv file containing columns mapping zipcodes to county')
	p.add_argument('out_file',
				help='Output file: required, must be a string.')
	args = p.parse_args()
	return args
arguments = get_opts()

# read in metadata csv file AND CONVERT ALL ZIPCODES TO STRINGS
meta_csv1 = arguments.csv_meta_file
meta_df1 = pd.read_csv(meta_csv1, dtype={'zip': str})

# read in ZIPCODE csv file AND CONVERT ALL ZIPCODES TO STRINGS
zip_csv1 = arguments.county_zipcodes_file
zip_df1 = pd.read_csv(zip_csv1, dtype={'ZipCode': str})

meta_df1[['zip1','zip2']] = meta_df1['zip'].str.split('-',expand=True)



# make a dictionary for fast zip/county lookups
zip_county_lookup_dict = dict(zip(zip_df1.ZipCode, zip_df1.County))

# add new column 'county' mapped from zip
meta_df1['county'] = meta_df1['zip1'].map(zip_county_lookup_dict)


# read in the root entity for the terra data table
root_entity_name1 = arguments.root_entity

# list of headers to be included in the final output file
output_headers = ['entity:{}_id'.format(root_entity_name1), 'collection_date', 'county', 'gisaid_accession', 'nextclade_clade', 'pango_lineage', 'sequencing_lab', 'state', 'zip']

# rename headers
meta_df1.rename(columns={'vendor_accession': 'entity:cdc_specimen_id', 'GISAID_accession': 'gisaid_accession', 'clade_Nextclade_clade': 'nextclade_clade', 'lineage_PANGO_lineage': 'pango_lineage', 'vendor': 'sequencing_lab'}, inplace=True)

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

# print to stdout
print(meta_df1)
