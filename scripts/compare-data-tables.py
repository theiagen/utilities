#!/usr/bin/env python3
"""
usage: compare-data-tables.py [-h] [--outdir STRING] [--prefix STRING] TSV TSV

Compare two TSV files and report the differences

positional arguments:
  TSV              The first TSV to compare.
  TSV              The second TSV file to compare

options:
  -h, --help       show this help message and exit
  --outdir STRING  The directory to output files to. (Default: ./)
  --prefix STRING  The prefix to use for output files (Default: comparison)
  --compcols TSV   The list of columns to be compared
"""
import pandas as pd

def read_tsv(tsv_file):
    """Read TSV and change first column to 'samples'"""
    df = pd.read_csv(tsv_file, sep='\t').fillna('')
    c1_name = df.columns.values[0]
    df.columns.values[0] = "samples"

    return [df, c1_name]

def write_html(data, html_out):
    """Data to write to HTML"""
    return None

if __name__ == '__main__':
    import argparse as ap
    import os
    import sys
    import pdfkit as pdf

    parser = ap.ArgumentParser(
        prog='compare-data-tables.py',
        conflict_handler='resolve',
        description=("Compare two TSV files and report the differences")
    )
    parser.add_argument('tsv1', metavar="TSV", type=str,
                        help='The first TSV to compare.')
    parser.add_argument('tsv2', metavar="TSV", type=str,
                        help='The second TSV file to compare')
    parser.add_argument('--outdir', metavar="STRING", type=str, default='./',
                        help='The directory to output files to. (Default: ./)')
    parser.add_argument('--prefix', metavar="STRING", type=str, default='comparison',
                        help='The prefix to use for output files (Default: comparison)')
    parser.add_argument('--compcols', metavar="TSV", type=list, default='comparison',
                        help='The list of columns to be compared')

    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(0)

    args = parser.parse_args()

    # Verify input exists
    has_error = False
    for tsv_file in [args.tsv1, args.tsv2]:
        if not os.path.isfile(tsv_file):
            print(f'Unable to locate TSV file: {tsv_file}', file=sys.stderr)
            has_error = True

    if has_error:
        sys.exit(1)

    # Read in TSVs
    df1, df1_c1_name = read_tsv(args.tsv1)
    df2, df2_c1_name = read_tsv(args.tsv2)

    # Read in list of cols
    comp_columns = read_tsv(args.compcols)

    print(df1.columns)
    # Drop columns that will almost always differ, keep only the columns that matter for validating the workflow
    keepers_list = comp_columns
    # Old list of comparison columns: ['assembly_length_unambiguous','assembly_mean_coverage','assembly_method','kraken_human','kraken_human_dehosted','kraken_sc2','kraken_sc2_dehosted','meanbaseq_trim','meanmapq_trim','nextclade_aa_dels','nextclade_aa_subs','nextclade_clade','number_Degenerate','number_N','number_Total','pango_lineage','pangolin_conflicts','pangolin_notes','percent_reference_coverage','primer_bed_name','seq_platform','vadr_num_alerts','validation_set','primer_trimmed_read_percent']
    drop_list1 = []
    drop_list2 = []

    for i in df1.columns:
    	if i not in keepers_list:
    		drop_list1.append(i)
    df1.drop(drop_list1, axis='columns', inplace=True)

    for j in df2.columns:
    	if j not in keepers_list:
    		drop_list2.append(j)
    df2.drop(drop_list2, axis='columns', inplace=True)

    if drop_list1 != drop_list2:
        print('Datatables have different sets of extraneous columns.')
    else:
        print('Datatables have the same set of extraneous columns.')

    # Perform comparison
    df_comp1 = df1.compare(df2, align_axis=1, keep_shape=True, keep_equal=False)
    # Count non-NA values in each columns
    val_cnts = df_comp1.count()
    df_val_cnts=val_cnts.to_frame()
    df_val_cnts.columns = ['Number of Diffs']
    print(df_val_cnts)
    # Replace NAs with "EXACT_MATCH"
    df_comp1.fillna(value='-', method=None, axis=None, inplace=True, limit=None, downcast=None)

    count_dict={}
    for i in df_comp1.columns:
        count_dict[i]=df_comp1[i].value_counts()
    counts_df=pd.DataFrame.from_dict(count_dict, orient='columns', dtype=None, columns=None)
    print(counts_df)


    out_xlsx_name=f'{args.outdir}/{args.prefix}.xlsx'
    out_html_name=f'{args.outdir}/{args.prefix}.html'
    out_pdf_name=f'{args.outdir}/{args.prefix}.pdf'

    pd.set_option('display.max_colwidth', 20)
    df_comp1.to_excel(out_xlsx_name)
    df_val_cnts.to_html(out_html_name)

    options = {
    'page-width': '10000mm',
    'title': 'Validation Report',
    'margin-top': '0.25in',
    'margin-right': '0.25in',
    'margin-bottom': '0.25in',
    'margin-left': '0.25in'}
    out_pdf_var=out_html_name
    pdf1 = pdf.from_file(out_html_name, out_pdf_name, options=options)


    print(df_comp1)
