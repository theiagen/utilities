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


def upload_mngr(path2upload, upload_path, gs_bucket, upload=False):
    """Manage pushing to GS bucket"""
    if upload:
        gs_exit = push_to_gs_bucket(
            f"{gs_bucket}{upload_path}", path2upload
        )
        if gs_exit:
            logger.error("Failed to push Kraken2 database to Google Storage")
            logger.error(
                f"Push manually via: `gsutil -m cp -r {path2upload} {gs_bucket}{upload_path}`"
            )
            raise Exception("Failed to push Kraken2 database to Google Storage")
    else:
        logger.info(f"Upload Kraken2 DB to Google Storage with: `gsutil -m cp -r {path2upload} {gs_bucket}{upload_path}`")


def download_viral_genomes(viral_accs_path, out_dir):
    """Calls NCBI datasets to download viral genomes"""
    precd_dir = os.getcwd()
    os.chdir(out_dir)
    if not os.path.isfile("ncbi_dataset.zip") and not os.path.isdir("ncbi_dataset"):
        datasets_exit = 1
        attempts = 0
        while datasets_exit and attempts < 3:
            attempts += 1
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
        if attempts == 3:
            logger.error(
                "Failed to download genomes from NCBI datasets after 3 attempts"
            )
            raise Exception("Failed to download genomes from NCBI datasets")
    else:
        logger.info("NCBI datasets already downloaded")
    os.chdir(precd_dir)
    return out_dir + "ncbi_dataset.zip"


def download_genomes(accs_path, out_dir):
    """Calls NCBI datasets to download genomes"""
    precd_dir = os.getcwd()
    os.chdir(out_dir)
    if not os.path.isfile("ncbi_dataset.zip") and not os.path.isdir("ncbi_dataset"):
        datasets_exit = 1
        attempts = 0
        while datasets_exit and attempts < 3:
            attempts += 1
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
        if attempts == 3:
            logger.error(
                "Failed to download genomes from NCBI datasets after 3 attempts"
            )
            raise Exception("Failed to download genomes from NCBI datasets")
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

    acc2taxon = {}
    data_json_path = f"{out_dir}/ncbi_dataset/data/assembly_data_report.jsonl"
    with open(data_json_path, "r") as json_in:
        for line in json_in:
            data = json.loads(line)
            acc2taxon[data["currentAccession"]] = data["organism"]["organismName"]

    return acc2taxon


def unzip_datasets(out_dir, datasets_zip, rm=True, virus=True):
    """Extracts genomes from datasets download"""
    if not os.path.isdir(out_dir + "ncbi_dataset/"):
        with zipfile.ZipFile(datasets_zip, "r") as zip_ref:
            zip_ref.extractall(out_dir)
        if rm:
            os.remove(datasets_zip)
    if virus:
        return f"{out_dir}ncbi_dataset/data/genomic.fna", f"{out_dir}ncbi_dataset/data/data_report.jsonl"
    else:
        return f"{out_dir}ncbi_dataset/data/", f"{out_dir}ncbi_dataset/data/assembly_data_report.jsonl"


def chunk_datasets(accs_path, out_dir, chunk_size=250000):
    """Chunk the datasets file into smaller files"""
    with open(accs_path, "r") as infile:
        accs = [x.strip() for x in infile if x.strip() and not x.startswith("#")]

    # acquire the chunks
    chunks = [accs[i : i + chunk_size] for i in range(0, len(accs), chunk_size)]
    # write the chunks
    chunked_files = []
    for i, chunk in enumerate(chunks):
        chunk_file = f"{accs_path}.chunk{i + 1}"
        with open(chunk_file, "w") as outfile:
            outfile.write("\n".join(chunk))
        chunked_files.append(chunk_file)

    # download the genomes for each chunk and append to a complete genome file
    full_genome = out_dir + "full_genome.fna"
    if os.path.isfile(full_genome):
        os.path.remove(full_genome)
    acc2taxon = {}
    for chunk in chunked_files:
        logger.info(f"Running chunk: {chunk}")
        datasets_zip = download_viral_genomes(chunk, out_dir)
        genome_file, data_json_path = unzip_datasets(out_dir, datasets_zip, virus = True)
        with open(genome_file, "r") as infile:
            with open(full_genome, "a") as outfile:
                for line in infile:
                    outfile.write(line)
        with open(data_json_path, "r") as json_in:
            for line in json_in:
                data = json.loads(line)
                acc2taxon[data["accession"]] = data["virus"]["organismName"]

        shutil.rmtree(out_dir + "ncbi_dataset/")

    return full_genome, acc2taxon


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
        "severe acute respiratory syndrome-related coronavirus",
        "betacoronavirus pandemicum",
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
                # skip SARS-CoV-2 in one db
                if row[6].lower() not in coronavirus_names:
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

    logger.info(f"Found {count} compatible non-SARS-CoV-2 viral accessions")

    return viral_accs_path, segmented_accs


def parse_pangolin_json(pangolin_json_path):
    """Parse the pangolin JSON file to extract SARS-CoV-2 accessions"""
    lineages = set()
    with open(pangolin_json_path, "r") as infile:
        pangolin_data = json.load(infile)
    for lineage in pangolin_data:
        # only want the main lineages
        lineages.add(lineage[: lineage.find(".")])

    return sorted(lineages)


def lineage2accs(lineages, out_dir, accessions=1):
    """Grab the top accessions for a lineage"""
    accs_path = f"{out_dir}accessions.txt"
    cmd_scaf = [
        "datasets",
        "summary",
        "virus",
        "genome",
        "taxon",
        "sars-cov-2",
        "--as-json-lines",
        "--complete-only",
        "--limit",
        str(accessions),
        "--lineage",
    ]
    with open(accs_path, "w") as out:
        for lineage in lineages:
            lineage_scaf = cmd_scaf + [lineage]
            datasets_exit = subprocess.run(
                lineage_scaf, stdout=subprocess.PIPE, stderr=subprocess.PIPE
            )
            datasets_raw = datasets_exit.stdout.decode("utf-8")
            datasets_json = [
                json.loads(line) for line in datasets_raw.split("\n") if line
            ]
            for hit in datasets_json:
                out.write(hit["accession"] + "\n")

    return accs_path


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
    acc2taxon = pull_datasets_genomes(seg_dir, fa_dir)
    return acc2taxon


def output_list_fastas(fna_dir, out_path):
    """Output a list of FASTA files"""
    with open(out_path, "w") as out:
        for fna in os.listdir(fna_dir):
            if fna.endswith(".fna"):
                out.write(fna + "\n")


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


def skani_db_mngr(accs_path, out_dir, db_base, segmented_accs=None):
    """Download the viral genomes and build the SKANI database"""
    fna_dir = out_dir + "fna/"
    if not os.path.isdir(fna_dir):
        os.mkdir(fna_dir)

    # downloading the viral genomes
    viral_fna, viral_acc2taxon = chunk_datasets(accs_path, out_dir, chunk_size=250000)
    if segmented_accs:
        logger.info("Downloading RefSeq viral genomes")
        refseq_acc2taxon = compile_complete_segments(segmented_accs, fna_dir, out_dir)
    
    acc2taxon = {**viral_acc2taxon, **refseq_acc2taxon} if segmented_accs else viral_acc2taxon

    logger.info("Extracting NCBI viral genomes from multifasta")
    multifas2fas(viral_fna, fna_dir)

    fa_list = f"{out_dir}fna_list.txt"
    output_list_fastas(fna_dir, fa_list)

    # build the SKANI database
    logger.info("Building SKANI database")
    skani_dir = mk_output_dir(out_dir, db_base, mkdir=False)
    # can't exist prior to building db
    if os.path.isdir(skani_dir):
        shutil.rmtree(skani_dir)
    cwd = os.getcwd()
    os.chdir(fna_dir)
    build_skani_db(fa_list, skani_dir, threads=8)
    os.chdir(cwd)
    skani_base = os.path.basename(skani_dir[:-1])

    os.chdir(out_dir)
    logger.info("Compressing SkaniDB into tarchive")
    skani_tar = compress_tarchive(skani_base)

    acc2taxon_path = f"{out_dir}accession2taxon.tsv"
    with open(acc2taxon_path, "w") as out:
        out.write("#accession\ttaxon\n")
        for acc in sorted(acc2taxon.keys()):
            out.write(f"{acc}\t{acc2taxon[acc]}\n")

    return skani_tar, skani_base, fna_dir, acc2taxon_path


def prep_human_genome(human_genome_url, human_out_dir):
    """Download the human genome and prepare it for Kraken2"""
    human_genome_path = human_out_dir + os.path.basename(human_genome_url)
    download_file(human_genome_url, human_genome_path)
    with gzip.open(human_genome_path, "rt") as human_in:
        human_out_path = re.sub(r".gz$", "", human_genome_path)
        with open(human_out_path, "w") as human_out:
            for line in human_in:
                human_out.write(line)
    os.remove(human_genome_path)
    return human_out_path



def prep_kraken2_library(db_dir, human_genome_path, threads=4):
    """Download the Kraken2 library for RefSeq viruses"""
    download_cmd = [
        "k2",
        "download-library",
        "--library",
        "viral",
        "--assembly-source",
        "refseq",
        "--threads",
        str(threads),
        "--db",
        db_dir
    ]
    download_code = subprocess.call(download_cmd)
    add_cmd = [
        "k2",
        "add-to-library",
        "--db",
        db_dir,
        "--file",
        human_genome_path,
        "--threads",
        str(threads)
    ]
    add_code = subprocess.call(add_cmd)
    taxonomy_cmd = [
        "k2",
        "download-taxonomy",
        "--db",
        db_dir
    ]
    taxonomy_code = subprocess.call(taxonomy_cmd)


def build_kraken2_db(db_dir, threads=8):
    """Build the Kraken2 database"""
    build_cmd = [
        "k2",
        "build",
        "--db",
        db_dir,
        "--threads",
        str(threads),
    ]
    build_code = subprocess.call(build_cmd)


def build_bracken_db(db_dir, kmer_lens = [50, 75, 100, 150, 200, 250, 300], threads=8):
    """Build the Bracken database from the Kraken2 database"""
    for kmer_len in kmer_lens:
        logger.info(f"Building Bracken database for kmer length: {kmer_len}")
        build_cmd = [
            "bracken-build",
            "-d",
            db_dir,
            "-l",
            str(kmer_len),
            "-t",
            str(threads),
        ]
        build_code = subprocess.call(build_cmd)


def clean_kraken2_dir(db_path, dirty_files):
    """Clean the Kraken2 directory"""
    clean_cmd = ["k2", "clean", "--db", db_path]
    clean_code = subprocess.call(clean_cmd)
    dirty_files.extend([x for x in os.listdir(db_path) if x.endswith('kraken')])
    for file_ in dirty_files:
        if os.path.isfile(file_):
            os.remove(file_)
        elif os.path.isdir(file_):
            shutil.rmtree(file_)

    
def main():
    max_threads = os.cpu_count() * 2
    if max_threads > 16:
        max_threads = 16
    usage = "Download complete RefSeq viral genomes and build SKANI database\n"
    parser = argparse.ArgumentParser(description=usage)
    parser.add_argument(
        "-s", "--skani_skip", help="Skip SKANI database build", action="store_true"
    )
    parser.add_argument(
        "-c", "--checkv_skip", help="Skip CheckV database download", action="store_true"
    )
    parser.add_argument(
        "-k", "--kraken_skip", help="Skip kraken2 database build", action="store_true"
    )
    parser.add_argument(
        "-a",
        "--sars_accs_per_lineage",
        type=int,
        default=1,
        help="Number of SARS-CoV-2 accessions per Pangolin lineage (DEFAULT: 1)",
    )
    parser.add_argument(
        "-u",
        "--upload",
        help="Upload to Google Storage",
        action="store_true",
    )
    parser.add_argument(
        "-t",
        "--threads",
        type=int,
        default=max_threads,
        help=f"Number of threads to use (DEFAULT: max_threads)",
    )
    parser.add_argument("-o", "--output_dir", help="Output directory")


    url_parser = parser.add_argument_group()
    url_parser.add_argument(
        "-b",
        "--gsbucket_url",
        default="gs://theiagen-public-resources-rp/reference_data/databases/",
        help="Google Storage bucket URL (DEFAULT: gs://theiagen-public-resources-rp/reference_data/databases/)",
        type=str
    )
    url_parser.add_argument(
        "-p",
        "--pangolin_json_url",
        default="https://github.com/cov-lineages/lineages-website/raw/refs/heads/master/_data/lineage_data.full.json",
        help="Pangolin JSON URL (DEFAULT: https://github.com/cov-lineages/lineages-website/raw/refs/heads/master/_data/lineage_data.full.json)",
        type=str
    )
    url_parser.add_argument(
        "-v",
        "--viral_metadata_url",
        default="https://ftp.ncbi.nlm.nih.gov/genomes/Viruses/AllNuclMetadata/AllNuclMetadata.csv.gz",
        help="Viral metadata URL (DEFAULT: https://ftp.ncbi.nlm.nih.gov/genomes/Viruses/AllNuclMetadata/AllNuclMetadata.csv.gz)",
        type=str
    )
    url_parser.add_argument(
        "-r",
        "--viral_refseq_url",
        default="https://ftp.ncbi.nlm.nih.gov/refseq/release/viral/viral.1.1.genomic.fna.gz",
        help="Viral RefSeq URL (DEFAULT: https://ftp.ncbi.nlm.nih.gov/refseq/release/viral/viral.1.1.genomic.fna.gz)",
        type=str
    )
    url_parser.add_argument(
        "-g",
        "--human_genome_url",
        default="https://ftp.ncbi.nlm.nih.gov/genomes/all/GCF/000/001/405/GCF_000001405.40_GRCh38.p14/GCF_000001405.40_GRCh38.p14_genomic.fna.gz",
        help="Human genome URL (DEFAULT: https://ftp.ncbi.nlm.nih.gov/genomes/all/GCF/000/001/405/GCF_000001405.40_GRCh38.p14/GCF_000001405.40_GRCh38.p14_genomic.fna.gz)",
        type=str
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
        download_file(args.viral_metadata_url, viral_metadata_path)

        # parse the metadata and extract the complete non-SARS viral accessions
        logger.info("Parsing viral metadata for non-SARS-CoV-2 accessions")
        viral_accs_path, segmented_accs = parse_viral_metadata(
            viral_metadata_path, out_dir
        )

        # create the sars dir and run
        logger.info("Acquiring SARS-CoV-2 accessions")
        sars_dir = out_dir + "sars-cov-2/"
        if not os.path.isdir(sars_dir):
            os.mkdir(sars_dir)

        pango_json_path = sars_dir + "pangolin_lineages.json"
        if not os.path.isfile(pango_json_path):
            download_file(args.pangolin_json_url, pango_json_path)
        pango_lineages = parse_pangolin_json(pango_json_path)
        logger.info(
            f"Finding up to {args.sars_accs_per_lineage * len(pango_lineages)} SARS-CoV-2 accessions"
        )
        sars_accs_path = lineage2accs(pango_lineages, sars_dir, args.sars_accs_per_lineage)

        all_accs_path = out_dir + "all_accessions.txt"
        with open(all_accs_path, "w") as out:
            with open(viral_accs_path, "r") as viral_in:
                out.write(viral_in.read().strip() + "\n")
            with open(sars_accs_path, "r") as sars_in:
                out.write(sars_in.read())

        skani_tar, skani_base, fna_dir, acc2taxon_path = skani_db_mngr(
            all_accs_path, out_dir, "skani_db", segmented_accs=segmented_accs
        )

        # not worth compressing because skani is already compressing
        logger.info("Pushing SkaniDB to Google Storage")
        upload_mngr(skani_tar, f"skani/{skani_base}.tar", args.gsbucket_url, upload=args.upload)
        # upload the genome database
        cur_date = datetime.now().strftime("%Y%m%d")
        upload_mngr(fna_dir, f"skani/viral_fna_{cur_date}/", args.gsbucket_url, upload=args.upload)
        upload_mngr(acc2taxon_path, f"skani/viral_fna_{cur_date}/viral_accession2taxon_{cur_date}.tsv", args.gsbucket_url, upload=args.upload)


    if not args.checkv_skip:
        # download the CheckV database
        checkv_dir = mk_output_dir(out_dir, "checkv_db")
        subprocess.call(["checkv", "download_database", checkv_dir])
        checkv_base = os.path.basename(checkv_dir)
        logger.info("Compressing CheckV DB into tarchive")
        checkv_tar = compress_tarchive(checkv_base, compression="gztar", ext=".tar.gz")
        logger.info("Pushing CheckV DB to Google Storage")
        upload_mngr(checkv_tar, f"checkv/{checkv_base}.tar.gz", args.gsbucket_url, upload=args.upload)


    if not args.kraken_skip:
        logger.info("Building Kraken2 database")
        kraken_dir = mk_output_dir(out_dir, "k2_viral_refseq_GRCh38")
        logger.debug("Downloading human genome for Kraken2 database")
        human_genome_path = prep_human_genome(args.human_genome_url, kraken_dir)
        if args.threads > 4:
            download_threads = 4
        else:
            download_threads = args.threads
        logger.info("Downloading Kraken2 viral library and adding human genome to library")
        prep_kraken2_library(kraken_dir, human_genome_path, threads=download_threads)
        logger.info("Building Kraken2 database")
        build_kraken2_db(kraken_dir, threads=args.threads)
        logger.info("Building Bracken k-mer libraries")
        build_bracken_db(kraken_dir, threads=args.threads)
        logger.info("Cleaning Kraken2 database directory")
        clean_kraken2_dir(kraken_dir, [human_genome_path, 'estimated_capacity'])
        k2db_tar = compress_tarchive(kraken_dir, compression="gztar", ext=".tar.gz")
        upload_mngr(k2db_tar, f"kraken2/k2_viral_refseq_GRCh38.tar.gz", args.gsbucket_url, upload=args.upload)


    if args.upload:
        logger.info("Cleaning up")
        rm_files(out_dir)


if __name__ == "__main__":
    main()
    sys.exit(0)