#! /usr/bin/env python3

import os
import re
import csv
import sys
import gzip
import shutil
import logging
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


def compress_tarchive(base_path, compression = 'tar', ext = '.tar'):
    """Compress a directory into a tar archive"""
    shutil.make_archive(base_path, compression)
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
    with open(fa_path, "r") as fasta_input:
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
    return fa_dict


def download_human_genome(
    out_dir,
    url="https://ftp.ncbi.nlm.nih.gov/genomes/all/GCF/009/914/755/GCF_009914755.1_T2T-CHM13v2.0/GCF_009914755.1_T2T-CHM13v2.0_genomic.fna.gz",
):
    """Download the human genome for Metabuli"""
    download_file(url, out_dir + "T2T-CHM13v2.0.fna.gz")
    with gzip.open(out_dir + "T2T-CHM13v2.0.fna.gz", "rt") as raw:
        with open(out_dir + "T2T-CHM13v2.0.fna", "w") as out:
            for line in raw:
                out.write(line)
    return out_dir + "T2T-CHM13v2.0.fna"


def parse_viral_metadata(viral_metadata_path, out_dir):
    """Parse the viral metadata and extract the complete accessions"""
    viral_accs_path = f"{out_dir}viral_accessions.txt"
    with open(viral_accs_path, "w") as out:
        with gzip.open(viral_metadata_path, "rt") as raw:
            reader = csv.reader(raw)
            next(reader)
            for row in reader:
                # skip SARS-Cov-2
                if row[6] != "Severe acute respiratory syndrome-related coronavirus":
                    if row[12] == "complete":
                        out.write(row[0] + "\n")
    return viral_accs_path


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


def build_metabuli_db(fa_dir, taxdump_path, human_fna, out_dir):
    """Build the Metabuli database"""
    fas = [format_path(f) for f in os.listdir(fa_dir) if f.endswith(".fna")]
    fas += [format_path(human_fna)]
    fas = sorted(set(fas))
    with open(fa_dir + "reference.txt", "w") as out:
        out.write("\n".join(fas))
    metabuli_exit = subprocess.call(
        [
            "metabuli",
            "build",
            out_dir,
            fa_dir + "reference.txt",
            taxdump_path + "taxid.map",
            "--gtdb",
            "1",
            "--taxonomy-path",
            taxdump_path,
        ]
    )
    #                                    shell = True)
    return metabuli_exit


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
    # COMMENTED code for de novo metabuli DB built, currently not functional due to accession to gtdb-taxdump taxid mapping challenges
    # Building the de novo metabuli DB requires tying RefSeq accessions to GCF accessions, then taxids through gtdb-taxdump
    #    taxdump_url = 'https://github.com/shenwei356/gtdb-taxdump/releases/download/v0.5.0/gtdb-taxdump.tar.gz'
    nucleotide_viral_url = "https://ftp.ncbi.nlm.nih.gov/genomes/Viruses/AllNuclMetadata/AllNuclMetadata.csv.gz"
    #   human_url = 'https://ftp.ncbi.nlm.nih.gov/genomes/all/GCF/009/914/755/GCF_009914755.1_T2T-CHM13v2.0/GCF_009914755.1_T2T-CHM13v2.0_genomic.fna.gz'
    gsbucket_url = "gs://theiagen-large-public-files-rp/terra/databases/"
    metabuli_db_url = "https://metabuli.steineggerlab.workers.dev/refseq_virus.tar.gz"

    usage = (
        "Download complete RefSeq viral genomes and build SKANI database\n"
    )
    parser = argparse.ArgumentParser(description=usage)
    parser.add_argument("-o", "--output_dir", help="Output directory")
    parser.add_argument("-s", "--skani_skip", help="Skip SKANI database", action="store_true")
    parser.add_argument("-m", "--metabuli_skip", help="Skip Metabuli database", action="store_true")
    parser.add_argument("-c", "--checkv_skip", help="Skip CheckV database", action="store_true")
    args = parser.parse_args()

    #'Uses gtdb-taxdump.tar.gz v0.5.0 needed for Metabuli'
    # extract the arguments and require at least 1 input
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
        viral_accs_path = parse_viral_metadata(viral_metadata_path, out_dir)

        # downloading the viral genomes
        datasets_zip = download_viral_genomes(viral_accs_path, out_dir)
        viral_fna = unzip_datasets(out_dir, datasets_zip)
        fna_dir = out_dir + "fna/"
        if not os.path.isdir(fna_dir):
            os.mkdir(fna_dir)
        logger.info("Extracting viral genomes from multifasta")
        multifas2fas(viral_fna, fna_dir)
        fa_list = f"{out_dir}fna_list.txt"
        output_list_fastas(fna_dir, fa_list)

    # build the SKANI and Metabuli databases
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
        gs_exit = push_to_gs_bucket(gsbucket_url + skani_base + '.tar', skani_tar)
        if gs_exit:
            logger.error("Failed to push SKANI database to Google Storage")
            logger.error(
                f"Push manually via: `gsutil -m cp -r {skani_dir} {gsbucket_url}{skani_base}`"
            )
            raise Exception("Failed to push SKANI database to Google Storage")

    if not args.skip_metabuli:
        # download prebuilt metabuli DB
        # REMOVE if updating to automated build
        metabuli_dir = mk_output_dir(out_dir, "metabuli_db")
        metabuli_tar = metabuli_dir + "refseq_virus.tar.gz"
        download_file(metabuli_db_url, metabuli_tar)
        #  logger.info('Downloading human genome')
        #   human_fna = download_human_genome(out_dir, human_url)
        # download the gtdb-taxdump (UPDATE FOR NEW RELEASES)
        #    logger.info('Downloading GTDB taxdump')
        #   taxdump_path = out_dir + 'gtdb-taxdump.tar.gz'
        #  download_file(taxdump_url, taxdump_path)
        # taxdump_dir = decompress_tarchive(taxdump_path, out_dir)
        #  logger.info('Building Metabuli database')
        # build_metabuli_db(fna_dir, taxdump_dir, human_fna, metabuli_dir)
        # metabuli_base = os.path.basename(metabuli_dir)

        # compress the databases and push to gs buckets
        logger.info("Pushing Metabuli DB to Google Storage")
        #    metabuli_tar = compress_tarchive(metabuli_base, metabuli_base + '.tar.gz')
        gs_exit = push_to_gs_bucket(
            gsbucket_url + os.path.basename(metabuli_tar), metabuli_tar
        )
        if gs_exit:
            logger.error("Failed to push Metabuli database to Google Storage")
            logger.error(
                f"Push manually via: `gsutil -m cp -r {metabuli_tar} {gsbucket_url}{os.path.basename(metabuli_tar)}`"
            )
            raise Exception("Failed to push Metabuli database to Google Storage")
        
    if not args.checkv_skip:
        # download the CheckV database
        checkv_dir = mk_output_dir(out_dir, "checkv_db")
        subprocess.call(['checkv', 'download_database', checkv_dir])
        checkv_base = os.path.basename(checkv_dir)
        logger.info("Compressing CheckV DB into tarchive")
        checkv_tar = compress_tarchive(checkv_base)
        logger.info("Pushing CheckV DB to Google Storage")
        gs_exit = push_to_gs_bucket(
            gsbucket_url + checkv_base + ".tar", checkv_tar
        )
        if gs_exit:
            logger.error("Failed to push CheckV database to Google Storage")
            logger.error(
                f"Push manually via: `gsutil -m cp -r {checkv_tar} {gsbucket_url}{checkv_base}.tar.gz`"
            )
            raise Exception("Failed to push CheckV database to Google Storage"

    logger.info("Cleaning up")
    rm_files(out_dir)


if __name__ == "__main__":
    main()
    sys.exit(0)
