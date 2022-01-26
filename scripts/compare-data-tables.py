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

    df_comp1 = df1.compare(df2, align_axis=1, keep_shape=False, keep_equal=False)
    df_comp1.fillna(value='EXACT_MATCH', method=None, axis=None, inplace=True, limit=None, downcast=None)
    # Get the side-by-side comparison of the TSVs
    # df_diff_vert = df1.compare(df2, align_axis = 0, keep_shape=True, keep_equal=True).transpose()
    # df_comp_bool = df1.where()
    #missing_samples = []

    # Compare samples
    #print(df1.samples.values)
    #print('column\tis_same\ttsv1\ttsv2')
    #print(f'filename\t{args.tsv1==args.tsv2}\t{args.tsv1}\t{args.tsv2}')
    #for i, data in df_diff_vert.iterrows():
    #    if data[0]['self'] != data[0]['other']:
    #        print(f"{data.name}\t{data[0]['self'] == data[0]['other']}\t{data[0]['self']}\t{data[0]['other']}")



    out_xlsx_name=f'{args.outdir}/{args.prefix}.xlsx'
    out_html_name=f'{args.outdir}/{args.prefix}.html'
    out_pdf_name=f'{args.outdir}/{args.prefix}.pdf'

    pd.set_option('display.max_colwidth', 20)
    df_comp1.to_excel(out_xlsx_name)
    df_comp1.to_html(out_html_name)

    options = {
    'page-width': '10000mm',
    'orientation': 'landscape',
    'margin-top': '0.25in',
    'margin-right': '0.25in',
    'margin-bottom': '0.25in',
    'margin-left': '0.25in'}
    out_pdf_var=out_html_name
    pdf1 = pdf.from_file(out_html_name, out_pdf_name, options=options)


    print(df_comp1)
