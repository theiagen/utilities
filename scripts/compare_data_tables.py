#!/usr/bin/env python3

# import sys
# import csv
import argparse
import pandas as pd
#argpase used to take in command line arguments
# three positional arguments, argparse might be overkill, sys command included
def get_opts():
    p = argparse.ArgumentParser(description = 'This file compares two tsv files and reports the significant differences', usage='[-h] compare_data_tables.py <table_1.tsv> <table_2.tsv> <outfile_name>')
    p.add_argument('tsv_file_1',
                help='first tsv file input')
    p.add_argument('tsv_file_2',
                help='second tsv file input')
    p.add_argument('out_file',
                help='Output file: required, must be a string.')
    args = p.parse_args()
    return args
arguments = get_opts()
tsv1 = arguments.tsv_file_1
tsv2 = arguments.tsv_file_2

df1 = pd.read_csv(tsv_file_1)
df2 = pd.read_csv(tsv_file_2)df_diff_vert:{}
df_diff_vert = df1.compare(df2, align_axis = 0)
df_diff_horiz = df1.compare(df2, align_axis = 1)


print("df_diff_vert:{}".format(df_diff_vert))
print("df_diff_horiz:{}".format(df_diff_horiz))
