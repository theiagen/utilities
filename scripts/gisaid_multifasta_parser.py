#!/usr/bin/env python3

import argparse
import pyfaidx
import time
import os
import sys

# two positional inputs
def get_opts():
	p = argparse.ArgumentParser(description = 'This program will parse the multifasta file provided in the gisaid tarball download of augur input files', usage='[-h] gisaid_multifasta_parser.py <gisaid_multifasta> <output_dir>')
	p.add_argument('gisaid_multifasta_file',
				help='multifasta input file: Enter a multifasta file containing DNA sequence.')
	p.add_argument('output_dir',
				help='Location of output directory.')
	args = p.parse_args()
	return args
arguments = get_opts()

fasta1 = arguments.gisaid_multifasta_file
output_dir_loc = arguments.output_dir

# use pyfaidx to read in the fasta file to create a dictionary-like object and in the event of a duplicate sequence keey only take the first entry💪💪💪
seqs1 = pyfaidx.Fasta(fasta1, duplicate_action="first")

# pull original names and sequences into lists
original_seq_names_list = []
seqs_list = []
for i in seqs1.keys():
    original_seq_names_list.append(i)
    seqs_list.append(seqs1[i][:].seq)

# remove slashes and create new list of names
no_slashes_seq_names_list = []
for i in original_seq_names_list:
    j = i.replace('/','_')
    j = j.replace('|',"_") # to prevent accidental piping
    no_slashes_seq_names_list.append(j)

# zip sequences and new slashless names into dicitonary
seqs_dict = dict(zip(no_slashes_seq_names_list, seqs_list))

# create variable with timestamp
timestr = time.strftime("%Y-%m-%d")

# make the output directory using the directory path input
os.makedirs('{}/individual_gisaid_assemblies_{}/'.format(output_dir_loc,timestr), exist_ok=True)

# redirect the stdout to file, print to file, reset stdout
for i in seqs_dict:
    with open('{}/individual_gisaid_assemblies_{}/{}.fasta'.format(output_dir_loc,timestr,i), 'w') as f:
        original_stdout = sys.stdout
        sys.stdout = f
        j = seqs_dict[i]
        print('>{}\n{}'.format(i,j))
        sys.stdout = original_stdout
