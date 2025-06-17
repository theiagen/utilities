#! /usr/bin/env python3

import re
import sys
import json
import logging
import argparse
import pandas as pd
from ete3 import Tree
from collections import defaultdict
from theiaphylo.lib.StdPath import Path
from theiaphylo.theiaphylo import *

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def id_clade_mrca(tree, metadf, clade_col, clade, noncomprehensive=False):
    """Identify the most recent common ancestor (MRCA) of a clade if it is monophyletic.
    Otherwise report a warning and return None."""
    # extract df for clade
    clade_df = metadf[metadf[clade_col] == clade]

    # get tips from clade
    clade_tips = sorted(set(clade_df.index))
    if len(clade_tips) == 1:
        # clade is a single tip, extract the node as the leaf name
        logger.info(f"{clade} mutations will be derived from one tip: {clade_tips[0]}")
        return clade_tips[0]
    # get MRCA
    mrca = tree.lowest_common_ancestor(clade_tips)
    mrca_tips = sorted(x.name for x in mrca.iter_tips())

    # extract clades associated with tips
    if noncomprehensive:
        mrca_df_tips = sorted(set(metadf.index).intersection(set(mrca_tips)))
        mrca_df = metadf.loc[mrca_df_tips]
    else:
        mrca_df = metadf.loc[mrca_tips]

    # identify clades descended from the MRCA
    mrca_clades = sorted(set(x for x in mrca_df[clade_col] if not pd.isna(x)))

    # clade is not monophyletic
    if len(mrca_clades) > 1:
        conflicts = sorted(set(mrca_clades) - {clade})
        logger.warning(
            f"{clade} is not monophyletic; conflicts with {conflicts} - Skipping."
        )
        return None
    # extract node from monophyletic clade
    else:
        # convert to ete3 object
        ete_tree = Tree(
            tree.get_newick(with_node_names=True, escape_name=False), format=8
        )
        # get the node name from the ete3 tree
        mrca_node = (
            ete_tree.get_common_ancestor(mrca_tips)
            .name.replace("'", "")
            .replace('"', "")
        )
    #        clade2node[clade] =
    return mrca_node


def write_clade_muts(clade2muts, out_file):
    """Extract and write the mutations for each clade to a TSV file."""
    with open(out_file, "w") as f:
        f.write("clade\tgene\tsite\talt\n")
        for clade, mut_dict in clade2muts.items():
            for mut in mut_dict["nt"]:
                mut_components = re.search(r"\D+(\d+)(\D+)", mut)
                if mut_components:
                    site = mut_components.group(1)
                    alt = mut_components.group(2)
                    f.write(f"{clade}\tnuc\t{site}\t{alt}\n")
            for prot, muts in mut_dict["aa"].items():
                for mut in muts:
                    mut_components = re.search(r"\D+(\d+)(\D+)", mut)
                    if mut_components:
                        site = mut_components.group(1)
                        alt = mut_components.group(2)
                        f.write(f"{clade}\t{prot}\t{site}\t{alt}\n")


def main(
    tree,
    metadf,
    clade_cols,
    nt_muts,
    aa_muts=None,
    excluded=set(),
    noncomprehensive=False,
):
    """Main function to extract mutations from clades."""
    # remove metadata entries that are not in the tree
    metadf = metadf[metadf.index.isin(tree.get_tip_names())]

    clade2muts = defaultdict(lambda: {"nt": [], "aa": {}})
    for clade_col in clade_cols:
        # get the clades
        clades = sorted(
            set([x for x in metadf[clade_col] if not pd.isna(x) and x not in excluded])
        )
        for clade in clades:
            mrca_node = id_clade_mrca(
                tree, metadf, clade_col, clade, noncomprehensive=noncomprehensive
            )
            if mrca_node:
                logger.info(f"{clade_col}: {clade}\tMRCA: {mrca_node}")
                clade2muts[clade]["nt"] = nt_muts["nodes"][mrca_node]["muts"]
                if aa_muts:
                    for prot, muts in aa_muts["nodes"][mrca_node]["aa_muts"].items():
                        clade2muts[clade]["aa"][prot] = muts
    return clade2muts


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Extract mutations from monophyletic clades for Nextclade genotyping"
    )
    parser.add_argument(
        "-t", "--tree", required=True, help="Path to the Augur-refined newick"
    )
    parser.add_argument("-m", "--metadata", required=True, help="Path to metadata TSV")
    parser.add_argument(
        "-cc", "--clade_cols", nargs="*", required=True, help="Clade columns to extract"
    )
    parser.add_argument(
        "-tc", "--tip_col", required=True, help="Column in metadata to use as tip label"
    )
    parser.add_argument("-nt", "--nt_muts", required=True, help="Path to nt_muts JSON")
    parser.add_argument("-aa", "--aa_muts", help="Path to aa_muts JSON")
    parser.add_argument(
        "-e", "---exclude", nargs="*", help="Clades to exclude from analysis"
    )
    parser.add_argument(
        "-r", "--root", nargs="*", help="Root tip(s) / node for rooting"
    )
    parser.add_argument(
        "-n",
        "--noncomprehensive",
        action="store_true",
        help="Accept missing metadata for tips",
    )
    parser.add_argument(
        "-o", "--output", help="Output file name. DEFAULT: 'clades.tsv'"
    )
    args = parser.parse_args()

    # load the tree
    tree = import_tree(Path(args.tree), outgroup=args.root)

    # load the metadata
    metadata_file = Path(args.metadata)
    if metadata_file.endswith(".csv"):
        metadf = pd.read_csv(Path(metadata_file), index_col=args.tip_col)
    else:
        metadf = pd.read_csv(Path(metadata_file), sep="\t", index_col=args.tip_col)

    # load the mutation files
    with open(Path(args.nt_muts), "r") as f:
        nt_muts = json.load(f)
    # load the aa_muts file if provided
    if args.aa_muts:
        with open(Path(args.aa_muts), "r") as f:
            aa_muts = json.load(f)
    else:
        aa_muts = None

    if args.exclude:
        exclusion_clades = set(args.exclude)
    else:
        exclusion_clades = set()

    clade2muts = main(
        tree,
        metadf,
        args.clade_cols,
        nt_muts,
        aa_muts,
        excluded=exclusion_clades,
        noncomprehensive=args.noncomprehensive,
    )

    # write the output
    if args.output:
        output_file = Path(args.output)
    else:
        output_file = Path("clades.tsv")
    write_clade_muts(clade2muts, output_file)
    sys.exit(0)
