#!/usr/bin/env python3

import csv
import json
import collections
import os
import argparse

#argpase used to take in command line arguments
# three positional arguments, argparse might be overkill, sys command included
def get_opts():
	p = argparse.ArgumentParser(description = 'This program reads in a csv of sequence metadata and performs some reformatting and data sanitization then spits out a tsv to be uploaded to terra.bio', usage='[-h] metadata_cleanser.py <metadata_file.csv> <ZipCode_County_Lookup_Table> <outfile_name>')
	p.add_argument('tsv_file',
				help='tsv file input')
	p.add_argument('output_name',
				help='Output file name required, must be a string.')
	args = p.parse_args()
	return args
arguments = get_opts()

# Set output file name
out_fname = arguments.output_name

# Writing the newline json file from tsv output above
with open(arguments.tsv_file, 'r') as infile:
    headers = infile.readline()
    headers_array = headers.strip().split('\t')
    headers_array[0] = "specimen_id"
    with open(out_fname+'.json', 'w') as outfile:
      for line in infile:
        outfile.write('{')
        line_array=line.strip().split('\t')
        for x,y in zip(headers_array, line_array):
          if x == "nextclade_aa_dels" or x == "nextclade_aa_subs":
            y = y.replace("|", ",")
          if y == "NA":
            y = ""
          if y == "N/A":
            y = ""
          if y == "Unknown":
            y = ""
          if y == "unknown":
            y = ""
          if y == "UNKNOWN":
            y = ""
          if y == "required_for_submission":
            y = ""
          if "Uneven pairs:" in y:
            y = ""
          if x == "County":
            pass
          else:
            outfile.write('"'+x+'"'+':'+'"'+y+'"'+',')
        outfile.write('"notes":""}'+'\n')
