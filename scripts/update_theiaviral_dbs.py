#! /usr/bin/env python3

import os
import re
import csv
import sys
import gzip
import json
import shutil
import logging
import tarfile
import zipfile
import argparse
import requests
import subprocess
from datetime import datetime

logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def expand_env_var(path):
    """Expands environment variables by regex substitution"""
    # identify all environment variables in the path
    envs = re.findall(r"\$[^/]+", path)
    # replace the environment variables with their values
    for env in envs:
        path = path.replace(env, os.environ[env.replace("$", "")])
    return path.replace("//", "/")


def format_path(path):
    """Convert all path types to absolute path with explicit directory ending"""
    if path:
        # expand username
        path = os.path.expanduser(path)
        # expand environment variables
        path = expand_env_var(path)
        # only save the directory ending if it is a directory
        if path.endswith("/"):
            if not os.path.isdir(path):
                path = path[:-1]
        # add the directory ending if it is a directory
        else:
            if os.path.isdir(path):
                path += "/"
        # make the path absolute
        if not path.startswith("/"):
            path = os.getcwd() + "/" + path
        # replace redundancies
        path = path.replace("/./", "/")
        # trace back to the root directory
        while "/../" in path:
            path = re.sub(r"[^/]+/\.\./(.*)", r"\1", path)
    return path


def mk_output_dir(
    base_dir, program, mkdir=True, suffix=datetime.now().strftime("%Y%m%d")
):
    """Create an output directory for outputs using the date as a suffix"""
    if not base_dir:
        base_dir = os.getcwd() + "/"
    if not os.path.isdir(format_path(base_dir)):
        raise FileNotFoundError(base_dir + " does not exist")
    out_dir = format_path(base_dir) + program + "_" + suffix

    if mkdir:
        if not os.path.isdir(out_dir):
            os.mkdir(out_dir)
    return out_dir + "/"


def download_file(url, out_path, max_attempts=3, force=False):
    """Download a file from a URL"""
    # skip if the file exists
    if os.path.isfile(out_path) and not force:
        logger.info(f"{out_path} already exists")
        return
    # attempt to download the file
    for attempt in range(max_attempts):
        try:
            # download the file
            response = requests.get(url)
            response.raise_for_status()
            with open(out_path + ".tmp", "wb") as out:
                out.write(response.content)
            os.rename(out_path + ".tmp", out_path)
            break
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to download {url} on attempt {attempt + 1}")
            logger.error(e)
    else:
        logger.error(f"Failed to download {url} after {max_attempts} attempts")


def decompress_tarchive(tar_path, out_dir):
    """Decompress a tar archive"""
    subprocess.call(["tar", "-xzf", tar_path, "-C", out_dir])
    return format_path(tar_path.replace(".tar.gz", ""))


def compress_tarchive(base_path, compression="tar", ext=".tar"):
    """Compress a directory into a tar archive"""
    if ext == ".tar.gz":
        file_open = "w:gz"
    elif ext == ".tar":
        file_open = "w"
    with tarfile.open(base_path + ext, file_open) as tar:
        tar.add(base_path, arcname=os.path.basename(base_path))
    return base_path + ext


def download_viral_genomes(viral_accs_path, out_dir):
    """Calls NCBI datasets to download viral genomes"""
    precd_dir = os.getcwd()
    os.chdir(out_dir)
    if not os.path.isfile("ncbi_dataset.zip") and not os.path.isdir("ncbi_dataset"):
        datasets_cmd = [
            "datasets",
            "download",
            "virus",
            "genome",
            "accession",
            "--complete-only",
            "--inputfile",
            viral_accs_path,
        ]
        datasets_exit = subprocess.call(datasets_cmd)
    else:
        logger.info("NCBI datasets already downloaded")
    os.chdir(precd_dir)
    return out_dir + "ncbi_dataset.zip"


def download_genomes(accs_path, out_dir):
    """Calls NCBI datasets to download genomes"""
    precd_dir = os.getcwd()
    os.chdir(out_dir)
    if not os.path.isfile("ncbi_dataset.zip") and not os.path.isdir("ncbi_dataset"):
        datasets_cmd = [
            "datasets",
            "download",
            "genome",
            "accession",
            "--assembly-level",
            "complete",
            "--inputfile",
            accs_path,
        ]
        datasets_exit = subprocess.call(datasets_cmd)
    else:
        logger.info("NCBI datasets already downloaded")
    os.chdir(precd_dir)
    return out_dir + "ncbi_dataset.zip"


def pull_datasets_genomes(out_dir, fa_dir):
    """Moves genomes in independent directoiries to a fasta directory"""
    prefix_dir = f"{out_dir}/ncbi_dataset/data/"
    acc_dirs = [
        prefix_dir + x for x in os.listdir(prefix_dir) if os.path.isdir(prefix_dir + x)
    ]
    for acc_dir in acc_dirs:
        acc = os.path.basename(acc_dir)
        acc_fa = prefix_dir + acc + "/" + os.listdir(acc_dir)[0]
        shutil.copy(acc_fa, f"{fa_dir}{acc}.fna")


def unzip_datasets(out_dir, datasets_zip):
    """Extracts genomes from datasets download"""
    if not os.path.isdir(out_dir + "ncbi_dataset/"):
        with zipfile.ZipFile(datasets_zip, "r") as zip_ref:
            zip_ref.extractall(out_dir)
    return f"{out_dir}ncbi_dataset/data/genomic.fna"


def dict2fa(fasta_dict, description=True):
    """Convert a dictionary to a FASTA string"""
    fasta_string = ""
    for gene in fasta_dict:
        fasta_string += ">" + gene.rstrip()
        # account for description field if it exists
        if "description" in fasta_dict[gene]:
            fasta_string += (" " + fasta_dict[gene]["description"]).rstrip() + "\n"
        else:
            fasta_string += "\n"
        fasta_string += fasta_dict[gene]["sequence"].rstrip() + "\n"
    return fasta_string


def output_fa(fa_dict, out_dir, seq_name, description=False):
    """Output a FASTA dictionary to a file"""
    out_path = f"{out_dir}{seq_name}.fna"
    if description:
        with open(out_path, "w") as out:
            out.write(
                dict2fa(
                    {
                        seq_name: {
                            "sequence": fa_dict["sequence"],
                            "description": fa_dict["description"],
                        }
                    }
                )
            )
    else:
        with open(out_path, "w") as out:
            out.write(
                dict2fa(
                    {seq_name: {"sequence": fa_dict["sequence"], "description": ""}}
                )
            )


def multifas2fas(fa_path, out_dir):
    """Convert a FASTA string to a dictionary"""
    fa_dict = {"sequence": "", "description": ""}
    if fa_path.endswith(".gz"):
        fasta_input = gzip.open(fa_path, "rt")
    else:
        fasta_input = open(fa_path, "r")
    for line in fasta_input:
        data = line.rstrip()
        if data.startswith(">"):
            # output previous entry
            if fa_dict["sequence"]:
                output_fa(fa_dict, out_dir, seq_name)
            # start a new entry
            header = data[1:].split(" ")
            seq_name = header[0]
            fa_dict = {"sequence": "", "description": " ".join(header[1:])}
        elif not data.startswith("#"):
            fa_dict["sequence"] += data
    # output the last entry
    if fa_dict["sequence"]:
        output_fa(fa_dict, out_dir, seq_name)
    fasta_input.close()
    return fa_dict


def parse_viral_metadata(viral_metadata_path, out_dir):
    """Parse the viral metadata and extract the complete accessions"""
    coronavirus_names = {
        "Severe acute respiratory syndrome-related coronavirus",
        "Betacoronavirus pandemicum",
    }
    segmented_families = {
        "orthomyxoviridae",
        "hantaviridae",
        "arenaviridae",
        "phenuiviridae",
    }
    viral_accs_path = f"{out_dir}viral_accessions.txt"
    count = 0
    segmented_accs = []
    with open(viral_accs_path, "w") as out:
        with gzip.open(viral_metadata_path, "rt") as raw:
            reader = csv.reader(raw)
            next(reader)
            for row in reader:
                # skip SARS-Cov-2
                if row[6] not in coronavirus_names:
                    if row[12] == "complete":
                        # we don't want redundancy and refseq segment reporting may be false
                        # we get segmented viruses from refseq later
                        if row[8].lower() in segmented_families:
                            segmented_accs.append(row[0])
                        elif row[11].lower() != "refseq":
                            # forego segments because they cannot be reliably linked
                            if not row[14]:
                                count += 1
                                out.write(row[0] + "\n")
                            else:
                                segmented_accs.append(row[0])
                        elif row[14]:
                            segmented_accs.append(row[0])

    logger.info(f"Found {count} compatible viral accessions")

    return viral_accs_path, segmented_accs


def compile_complete_segments(segmented_accs, fa_dir, out_dir):
    """Identify complete segment accessions"""
    if not os.path.isdir(f"{out_dir}segmented/"):
        os.mkdir(f"{out_dir}segmented/")
    segmented_accs_path = f"{out_dir}segmented/segmented_accessions.txt"
    seg_dir = f"{out_dir}segmented/"

    with open(segmented_accs_path, "w") as out:
        out.write("\n".join(segmented_accs) + "\n")
    if not os.path.isdir(f"{out_dir}segmented/ncbi_dataset/"):
        datasets_cmd = [
            "datasets",
            "summary",
            "gene",
            "accession",
            "--as-json-lines",
            "--report",
            "gene",
            "--inputfile",
            segmented_accs_path,
        ]
        datasets_exit = subprocess.run(datasets_cmd, stdout=subprocess.PIPE)
        datasets_raw = datasets_exit.stdout.decode("utf-8")
        datasets_json = [json.loads(line) for line in datasets_raw.split("\n") if line]
        gcfs = []
        for hit in datasets_json:
            if "annotations" in hit:
                if "assembly_accession" in hit["annotations"][0]:
                    gcfs.append(hit["annotations"][0]["assembly_accession"])
        full_gcfs = sorted(set(x for x in gcfs if x.startswith(("GCF_", "GCA_"))))
        complete_segments_path = f"{out_dir}segmented/complete_segments.txt"
        with open(complete_segments_path, "w") as out:
            out.write("\n".join(full_gcfs) + "\n")

        # download the datasets zip file
        logger.info("Downloading segmented genomes from NCBI datasets")
        datasets_zip = download_genomes(complete_segments_path, seg_dir)
        with zipfile.ZipFile(datasets_zip, "r") as zip_ref:
            zip_ref.extractall(f"{out_dir}segmented/")

    # copy the FASTA files to the output directory
    pull_datasets_genomes(seg_dir, fa_dir)


def output_list_fastas(fna_dir, out_path):
    """Output a list of FASTA files"""
    with open(out_path, "w") as out:
        for fna in os.listdir(fna_dir):
            if fna.endswith(".fna"):
                out.write(format_path(fna_dir + fna) + "\n")


def build_skani_db(fa_list, db_dir, threads=8):
    """Build the SKANI database"""
    # skani requires the output directory to not exist
    if os.path.isdir(db_dir):
        shutil.rmtree(db_dir)
    skani_cmd = [
        "skani",
        "sketch",
        "-l",
        fa_list,
        "-o",
        db_dir,
        "-t",
        str(threads),
        "-m",
        "50",
        "-c",
        "20",
    ]
    #    skani_file = out_dir + 'build_skani_db.sh'
    #   with open(skani_file, 'w') as out:
    #      out.write(skani_cmd)
    try:
        skani_exit = subprocess.call(skani_cmd)
    except FileNotFoundError:
        raise FileNotFoundError("SKANI may not be installed")
    return skani_exit


def push_to_gs_bucket(gs_bucket, file_path):
    """Push a file to a Google Storage bucket"""
    if os.path.isdir(file_path):
        gs_exit = subprocess.call(["gsutil", "-m", "cp", "-r", file_path, gs_bucket])
    else:
        gs_exit = subprocess.call(["gsutil", "-m", "cp", file_path, gs_bucket])
    return gs_exit


def rm_files(out_dir):
    """Clean-up all the downloaded files"""
    for path_ in os.listdir(out_dir):
        if os.path.isfile(path_):
            os.remove(path_)
        elif os.path.isdir(path_):
            shutil.rmtree(path_)


def main():
    # hard-coded URLs
    nucleotide_viral_url = "https://ftp.ncbi.nlm.nih.gov/genomes/Viruses/AllNuclMetadata/AllNuclMetadata.csv.gz"
    gsbucket_url = "gs://theiagen-public-resources-rp/reference_data/databases/"

    usage = "Download complete RefSeq viral genomes and build SKANI database\n"
    parser = argparse.ArgumentParser(description=usage)
    parser.add_argument("-o", "--output_dir", help="Output directory")
    parser.add_argument(
        "-s", "--skani_skip", help="Skip SKANI database", action="store_true"
    )
    parser.add_argument(
        "-c", "--checkv_skip", help="Skip CheckV database", action="store_true"
    )
    parser.add_argument(
        "-u",
        "--upload_skip",
        help="Skip uploading to Google Storage",
        action="store_true",
    )
    parser.add_argument(
        "-r",
        "--ram",
        type=int,
        default=16,
    )
    args = parser.parse_args()

    if args.output_dir:
        out_dir = format_path(args.output_dir)
        if not os.path.isdir(out_dir):
            os.mkdir(out_dir)
    else:
        # build an output directory
        out_dir = mk_output_dir(os.getcwd(), "update_theiaviral_dbs")

    # download latest viral metadata
    if not args.skani_skip:
        logger.info("Downloading latest viral nucleotide metadata")
        viral_metadata_path = out_dir + "AllNuclMetadata.csv.gz"
        download_file(nucleotide_viral_url, viral_metadata_path)

        # parse the metadata and extract the complete non-SARS viral accessions
        logger.info("Parsing viral metadata")
        viral_accs_path, segmented_accs = parse_viral_metadata(
            viral_metadata_path, out_dir
        )

        # downloading the viral genomes
        datasets_zip = download_viral_genomes(viral_accs_path, out_dir)
        viral_fna = unzip_datasets(out_dir, datasets_zip)
        fna_dir = out_dir + "fna/"
        if not os.path.isdir(fna_dir):
            os.mkdir(fna_dir)
        logger.info("Downloading RefSeq viral genomes")
        compile_complete_segments(segmented_accs, fna_dir, out_dir)

        logger.info("Extracting NCBI viral genomes from multifasta")
        multifas2fas(viral_fna, fna_dir)

        fa_list = f"{out_dir}fna_list.txt"
        output_list_fastas(fna_dir, fa_list)

        # build the SKANI database
        logger.info("Building SKANI database")
        skani_dir = mk_output_dir(out_dir, "skani_db", mkdir=False)
        # can't exist prior to building db
        if os.path.isdir(skani_dir):
            shutil.rmtree(skani_dir)
        build_skani_db(fa_list, skani_dir, threads=8)
        skani_base = os.path.basename(skani_dir[:-1])

        os.chdir(out_dir)
        logger.info("Compressing SkaniDB into tarchive")
        skani_tar = compress_tarchive(skani_base)
        # not worth compressing because skani is already compressing
        logger.info("Pushing SkaniDB to Google Storage")
        if not args.upload_skip:
            gs_exit = push_to_gs_bucket(gsbucket_url + skani_base + ".tar", skani_tar)
            if gs_exit:
                logger.error("Failed to push SKANI database to Google Storage")
                logger.error(
                    f"Push manually via: `gsutil -m cp -r {skani_dir} {gsbucket_url}skani/{skani_base}`"
                )
                raise Exception("Failed to push SKANI database to Google Storage")

    if not args.checkv_skip:
        # download the CheckV database
        checkv_dir = mk_output_dir(out_dir, "checkv_db")
        subprocess.call(["checkv", "download_database", checkv_dir])
        checkv_base = os.path.basename(checkv_dir)
        logger.info("Compressing CheckV DB into tarchive")
        checkv_tar = compress_tarchive(checkv_base, compression="gztar", ext=".tar.gz")
        logger.info("Pushing CheckV DB to Google Storage")
        if not args.upload_skip:
            gs_exit = push_to_gs_bucket(
                f"{gsbucket_url}checkv/{checkv_base}.tar.gz", checkv_tar
            )
            # check if the push was successful
            if gs_exit:
                logger.error("Failed to push CheckV database to Google Storage")
                logger.error(
                    f"Push manually via: `gsutil -m cp -r {checkv_tar} {gsbucket_url}{checkv_base}.tar.gz`"
                )
                raise Exception("Failed to push CheckV database to Google Storage")

    if not args.upload_skip:
        logger.info("Cleaning up")
        rm_files(out_dir)


if __name__ == "__main__":
    main()
    sys.exit(0)
