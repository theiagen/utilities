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
from theiaphylo.phyloutils import *

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def compile_tip2mutations(tree, tip, nt_muts, aa_muts=None):
    """Compile mutations for a given tip from the nt_muts and aa_muts JSON."""
    tip_muts = {"nt": set(), "aa": defaultdict(set)}
    par_node = tree.get_node_matching_name(tip)
    # iteratively accumulate all mutations from the tip to the root
    while par_node is not None:
        node_name = par_node.name
        tip_muts["nt"].update(set(nt_muts["nodes"][node_name]["muts"]))
        par_node = par_node.parent

    # iteratively accumulate all amino acid mutations from the tip to the root
    if aa_muts:
        par_node = tree.get_node_matching_name(tip)
        while par_node is not None:
            node_name = par_node.name
            for prot, muts in aa_muts["nodes"][node_name]["aa_muts"].items():
                tip_muts["aa"][prot].update(set(muts))
            par_node = par_node.parent
    return tip_muts


def id_clade_mrca(
    tree, metadf, clade_col, clade, noncomprehensive=False, skip_singletons=True
):
    """Identify the most recent common ancestor (MRCA) of a clade if it is monophyletic.
    Otherwise report a warning and return None."""
    # extract df for clade
    clade_df = metadf[metadf[clade_col] == clade]

    # get tips from clade
    clade_tips = sorted(set(clade_df.index))
    if len(clade_tips) == 1:
        if skip_singletons:
            logger.warning(
                f"{clade} is a singleton clade with one tip: {clade_tips[0]} - Skipping."
            )
            return None, None
        else:
            # clade is a single tip, extract the node as the leaf name
            #           logger.info(
            #                f"{clade} mutations will be derived from one tip: {clade_tips[0]}"
            #          )
            return clade_tips[0], clade_tips[0]
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
        return None, None
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
    return mrca_node, mrca_tips


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


def check_nt_uniqueness(muts_set, tips2muts, mrca_tips, tree, mrca_node, nt_muts):
    """Check if the mutations are unique enough to call."""
    failing_tips = []
    for tip in set(tips2muts.keys()).difference(set(mrca_tips)):
        tip_muts = tips2muts[tip]["nt"]
        if not muts_set.difference(tip_muts):
            failing_tips.append(tip)

    # if there are failing tips, traverse the tree to get unique mutations
    if failing_tips:
        parent = tree.get_node_matching_name(mrca_node)
        while failing_tips:
            parent = parent.parent
            # traversed to the root and couldn't identify unique mutations
            if parent is None:
                return False, failing_tips
            node_name = parent.name
            node_muts = nt_muts["nodes"][node_name]["muts"]
            # compile unique mutations and remove if found
            todel_tips = []
            for tip in failing_tips:
                diff_muts = set(node_muts).difference(tips2muts[tip]["nt"])
                if diff_muts:
                    muts_set.update(diff_muts)
                    todel_tips.append(tip)
            for tip in todel_tips:
                failing_tips.remove(tip)

    return sorted(muts_set), failing_tips


def main(
    tree,
    metadf,
    clade_cols,
    nt_muts,
    aa_muts=None,
    excluded=set(),
    noncomprehensive=False,
    skip_singletons=True,
):
    """Main function to extract mutations from clades."""
    # remove metadata entries that are not in the tree
    metadf = metadf[metadf.index.isin(tree.get_tip_names())]

    # compile cumulative mutation data for each tip
    tips2muts = defaultdict(lambda: {"nt": [], "aa": {}})
    for tip in tree.get_tip_names():
        tips2muts[tip] = compile_tip2mutations(tree, tip, nt_muts, aa_muts=aa_muts)

    clade2muts = defaultdict(lambda: {"nt": [], "aa": {}})
    for clade_col in clade_cols:
        # get the clades
        clades = sorted(
            set([x for x in metadf[clade_col] if not pd.isna(x) and x not in excluded])
        )
        for clade in clades:
            mrca_node, mrca_tips = id_clade_mrca(
                tree,
                metadf,
                clade_col,
                clade,
                noncomprehensive=noncomprehensive,
                skip_singletons=skip_singletons,
            )
            if mrca_node:
                logger.info(f"{clade_col}: {clade}\tMRCA: {mrca_node}")
                clade_nt_muts_set = set(nt_muts["nodes"][mrca_node]["muts"])

                # check if the mutations are unique enough to call
                clade_nt_muts, conflict_tips = check_nt_uniqueness(
                    clade_nt_muts_set, tips2muts, mrca_tips, tree, mrca_node, nt_muts
                )
                if not clade_nt_muts:
                    logger.warning(f"{clade} mutations are not unique - Skipping.")
                    continue

                clade2muts[clade]["nt"] = clade_nt_muts
                if aa_muts:
                    for prot, muts in aa_muts["nodes"][mrca_node]["aa_muts"].items():
                        clade2muts[clade]["aa"][prot] = muts
    #               else:
    #                  logger.warning(
    #                     f"{clade} mutations are not unique"
    #                )
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
    parser.add_argument("---exclude", nargs="*", help="Clades to exclude from analysis")
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
        "-s",
        "--skip_singletons",
        action="store_true",
        help="Skip singletons (clades with one tip)",
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
        skip_singletons=args.skip_singletons,
    )

    # write the output
    if args.output:
        output_file = Path(args.output)
    else:
        output_file = Path("clades.tsv")
    write_clade_muts(clade2muts, output_file)
    sys.exit(0)
