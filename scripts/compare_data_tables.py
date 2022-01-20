#!/usr/bin/env python3

# import sys
# import csv
import argparse
import pandas as pd
import pdfkit as pdf
from collections import defaultdict
#argpase used to take in command line arguments
# three positional arguments, argparse might be overkill, sys command included
def get_opts():
    p = argparse.ArgumentParser(description = 'This file compares two tsv files and reports the significant differences', usage='[-h] compare_data_tables.py <table_1.tsv> <table_2.tsv> <outfile_name>')
    p.add_argument('tsv_file_1',
                help='first tsv file input')
    p.add_argument('tsv_file_2',
                help='second tsv file input')
    p.add_argument('--out_file',
                default='outfile',
                help='Output file: required, must be a string.')
    args = p.parse_args()
    return args
arguments = get_opts()
tsv1 = arguments.tsv_file_1
tsv2 = arguments.tsv_file_2

df1 = pd.read_csv(tsv1, sep='\t')
df2 = pd.read_csv(tsv2, sep='\t')

df_diff_vert = df1.compare(df2, align_axis = 0, keep_shape=True, keep_equal=True).transpose()
#df_diff_vert['equals'] = df_diff_vert.duplicated()

out_basename = arguments.outfile
out_html_name='{}.html'.format(out_basename)
out_pdf_name='{}.pdf'.format(out_basename)

df_diff_vert.to_html(out_html_name)
pdf.from_file(out_html_name, out_pdf_name)



print(f"{df_diff_vert}")
