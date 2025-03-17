#! /usr/bin/env python3

import os
import re
import sys
import gzip
import shutil
import logging
import requests
import subprocess
from datetime import datetime

logging.basicConfig(level = logging.DEBUG,
                    format = '%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def expand_env_var(path):
    """Expands environment variables by regex substitution"""
    # identify all environment variables in the path
    envs = re.findall(r'\$[^/]+', path)
    # replace the environment variables with their values
    for env in envs:
        path = path.replace(env, os.environ[env.replace('$','')])
    return path.replace('//','/')

def format_path(path):
    """Convert all path types to absolute path with explicit directory ending"""
    if path:
        # expand username
        path = os.path.expanduser(path)
        # expand environment variables
        path = expand_env_var(path)
        # only save the directory ending if it is a directory
        if path.endswith('/'):
            if not os.path.isdir(path):
                path = path[:-1]
        # add the directory ending if it is a directory
        else:
            if os.path.isdir(path):
                path += '/'
        # make the path absolute
        if not path.startswith('/'):
            path = os.getcwd() + '/' + path
        # replace redundancies
        path = path.replace('/./', '/')
        # trace back to the root directory
        while '/../' in path:
            path = re.sub(r'[^/]+/\.\./(.*)', r'\1', path)
    return path

def mk_output_dir(base_dir, program, reuse = True, 
             suffix = datetime.now().strftime('%Y%m%d')):
    """Create an output directory for outputs using the date as a suffix"""
    if not base_dir:
        base_dir = os.getcwd() + '/'
    if not os.path.isdir(format_path(base_dir)):
        raise FileNotFoundError(base_dir + ' does not exist')
    out_dir = format_path(base_dir) + program + '_' + suffix

    if not os.path.isdir(out_dir):
        os.mkdir(out_dir)
    return out_dir + '/'

def download_file(url, out_path, max_attempts = 3):
    """Download a file from a URL"""
    # attempt to download the file
    for attempt in range(max_attempts):
        try:
            # download the file
            response = requests.get(url)
            response.raise_for_status()
            with open(out_path, 'wb') as out:
                out.write(response.content)
            break
        except requests.exceptions.RequestException as e:
            logger.error(f'Failed to download {url} on attempt {attempt + 1}')
            logger.error(e)
    else:
        logger.error(f'Failed to download {url} after {max_attempts} attempts')

def decompress_tarchive(tar_path, out_dir):
    """Decompress a tar archive"""
    subprocess.call(['tar', '-xzf', tar_path, '-C', out_dir])
    return format_path(tar_path.replace('.tar.gz', ''))

def compress_tarchive(dir_path, tar_path):
    """Compress a directory into a tar archive"""
    shutil.make_archive(tar_path, 'gztar', root_dir = dir_path, base_dir = dir_path)
    return tar_path

def download_human_genome(out_dir, url = 'https://ftp.ncbi.nlm.nih.gov/genomes/all/GCF/009/914/755/GCF_009914755.1_T2T-CHM13v2.0/GCF_009914755.1_T2T-CHM13v2.0_genomic.fna.gz'):
    """Download the human genome for Metabuli"""
    download_file(url, out_dir + 'T2T-CHM13v2.0.fna.gz')
    with gzip.open(out_dir + 'T2T-CHM13v2.0.fna.gz', 'rt') as raw:
        with open(out_dir + 'T2T-CHM13v2.0.fna', 'w') as out:
            for line in raw:
                out.write(line)
    return out_dir + 'T2T-CHM13v2.0.fna'

def download_refseq(out_dir, refseq_url = 'https://ftp.ncbi.nlm.nih.gov/refseq/release/viral/'):
    """Download the latest RefSeq viral release by parsing the FTP dir"""
    index_path = os.path.join(out_dir, 'index.html')
    download_file(refseq_url, index_path)
    # identify the fna from the FTP index
    with open(index_path, 'r') as raw:
        for line in raw:
            if 'genomic.fna.gz' in line:
                fna_url = re.search('href="(.+?)"', line).group(1)
                break
    fna_path = os.path.join(out_dir, os.path.basename(fna_url))
    # download the refseq viral fna
    download_file(refseq_url + fna_url, fna_path)
    return fna_path

def fa2dict_str(fasta_input):
    """Convert a FASTA string to a dictionary"""
    fasta_dict = {}
    for line in fasta_input.split('\n'):
        data = line.rstrip()
        if data.startswith('>'):
            header = data[1:].split(' ')
            seq_name = header[0]
            fasta_dict[seq_name] = {'sequence': '', 
                                     'description': ' '.join(header[1:])}
        elif not data.startswith('#'):
            fasta_dict[seq_name]['sequence'] += data
    return fasta_dict

def dict2fa(fasta_dict, description = True):
    """Convert a dictionary to a FASTA string"""
    fasta_string = ''
    for gene in fasta_dict:
        fasta_string += '>' + gene.rstrip()
        # account for description field if it exists
        if 'description' in fasta_dict[gene]:
            fasta_string += (' ' + fasta_dict[gene]['description']).rstrip() + '\n'
        else:
            fasta_string += '\n'
        fasta_string += fasta_dict[gene]['sequence'].rstrip() + '\n'
    return fasta_string

def parse_and_extract(fa_path, output_dir):
    """Import the FASTA and extract individual sequences"""
    # read the FASTA and decompress if necessary
    with gzip.open(fa_path, 'rt') as raw:
        fa_dict = fa2dict_str(raw.read())

    # extract the sequences
    for key, value in fa_dict.items():
        # write the sequence to a file
        out_path = os.path.join(output_dir, key + '.fna')
        with open(out_path, 'w') as out:
            out.write(dict2fa({key: value}))

def build_skani_db(fa_dir, out_dir, threads = 8):
    """Build the SKANI database"""
    skani_exit = subprocess.call(['skani', 'sketch', '*fna', '-o', out_dir, '-t', str(threads)])
#                                 shell = True)
    return skani_exit

def build_metabuli_db(fa_dir, taxdump_path, human_fna, out_dir):
    """Build the Metabuli database"""
    fas = [format_path(f) for f in os.listdir(fa_dir) if f.endswith('.fna')]
    fas += [format_path(human_fna)]
    fas = sorted(set(fas))
    with open(fa_dir + 'reference.txt', 'w') as out:
        out.write('\n'.join(fas))
    metabuli_exit = subprocess.call(['metabuli', 'build', out_dir, 
                                     fa_dir + 'reference.txt', 
                                     taxdump_path + 'taxid.map', 
                                     '--gtdb', '1', '--taxonomy-path', taxdump_path])
#                                    shell = True)
    return metabuli_exit

def push_to_gs_bucket(gs_bucket, file_path):
    """Push a file to a Google Storage bucket"""
    gs_exit = subprocess.call(['gsutil', 'cp', file_path, gs_bucket], shell = True)
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
    refseq_url = 'https://ftp.ncbi.nlm.nih.gov/refseq/release/viral/'
 #   human_url = 'https://ftp.ncbi.nlm.nih.gov/genomes/all/GCF/009/914/755/GCF_009914755.1_T2T-CHM13v2.0/GCF_009914755.1_T2T-CHM13v2.0_genomic.fna.gz'
    gsbucket_url = 'gs://theiagen-large-public-files-rp/terra/databases/'

    usage = 'Download complete RefSeq viral genomes and build SKANI database'
            #'Uses gtdb-taxdump.tar.gz v0.5.0 needed for Metabuli'
    # extract the arguments and require at least 1 input
    if {'-h', '--help', '-help'}.intersection(set(sys.argv)):
        print(usage)
        sys.exit(0)

    # build an output directory
    out_dir = mk_output_dir(os.getcwd(), 'update_theiaviral_dbs')

    # download latest refseq release
    logger.info('Downloading latest RefSeq release viral genomes')
    refseq_fna = download_refseq(out_dir, refseq_url)

    # parse and extract the RefSeq genomes
    logger.info('Extracting RefSeq viral genomes')
    fna_dir = out_dir + 'extracted/'
    if not os.path.isdir(fna_dir):
        os.mkdir(fna_dir)
    parse_and_extract(refseq_fna, fna_dir)

    # build the SKANI and Metabuli databases
    logger.info('Building SKANI database')
    skani_dir = mk_output_dir(out_dir, 'skani_db')
    build_skani_db(fna_dir, skani_dir, threads = 8)
    skani_base = os.path.basename(skani_dir)

    # download prebuilt metabuli DB
    # REMOVE if updating to automated build
    metabuli_dir = mk_output_dir(out_dir, 'metabuli_db')
    metabuli_tar = metabuli_dir + 'refseq_virus.tar.gz'
    download_file(metabuli_db_url, metabuli_db_path)
#  logger.info('Downloading human genome')
 #   human_fna = download_human_genome(out_dir, human_url)
    # download the gtdb-taxdump (UPDATE FOR NEW RELEASES)
#    logger.info('Downloading GTDB taxdump')
 #   taxdump_path = out_dir + 'gtdb-taxdump.tar.gz'
  #  download_file(taxdump_url, taxdump_path)
   # taxdump_dir = decompress_tarchive(taxdump_path, out_dir)
  #  logger.info('Building Metabuli database')
    #build_metabuli_db(fna_dir, taxdump_dir, human_fna, metabuli_dir)
    #metabuli_base = os.path.basename(metabuli_dir)

    # compress the databases and push to gs buckets
    logger.info('Compressing and pushing databases to Google Storage')
    os.chdir(out_dir)
    skani_tar = compress_tarchive(skani_base, skani_base + '.tar.gz')
    push_to_gs_bucket(gsbucket_url + os.path.basename(skani_tar), skani_tar)    
#    metabuli_tar = compress_tarchive(metabuli_base, metabuli_base + '.tar.gz')
    push_to_gs_bucket(gsbucket_url + os.path.basename(metabuli_tar), metabuli_tar)

    logger.info('Cleaning up')
    rm_files(out_dir)

if __name__ == '__main__':
    main()
    sys.exit(0)