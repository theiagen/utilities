#!/bin/bash
# by: Curtis Kapsak (curtis.kapsak@theiagen.com)
# originally written 2021-07-30; updated 2022-03-03; added to theiagen/utilities repo 2023-04-17
#
# This script is for concatenating fastq.gz files that from a Nanopore sequencing run,
# specifically with barcoded/multiplexed sequencing runs. It will not overwrite the original fastq files, but rather
# concatenate fastqs for each barcoded sample, and write them to a single output directory.
#
# REQUIREMENTS:
# This script requires that fastq.gz files are arranged in the normal output/directory
# structure from Guppy basecalling. It will not work as a general script for concatenating
# fastq files. Here is the general structure:
#
# my-sequencing-run/
#   ├── barcode01/
#   │   └── fastq_runid_fbc8eee46271cbe60ee8a49d0ca657f6e92e174e_0_0.fastq.gz (there will be many .fastq.gz files per barcode)
#   ├── barcode02/
#   │   └── fastq_runid_fbc8eee46271cbe60ee8a49d0ca657f6e92e174e_0_0.fastq.gz
#   ├── barcode03/
#   │   └── fastq_runid_fbc8eee46271cbe60ee8a49d0ca657f6e92e174e_0_0.fastq.gz
# etc. to barcode 96 and beyond!
#
#
#
#

echo "Today's date:" $(date)

# This function will check to make sure the directory doesn't already exist before trying to create it
make_directory() {
    if [ -e $1 ]; then
        echo "Directory "$1" already exists"
    else
        mkdir -v $1
    fi
}

# set input and output dirs
INPUT_DIR=$1
echo "INPUT_DIR is set to:" $INPUT_DIR

OUTPUT_DIR=$2
echo "OUTPUT_DIR is set to:" $OUTPUT_DIR

make_directory ${OUTPUT_DIR}

# check for first and second argument
if [[ -z "${1}" || -z "${2}" ]]; then
  echo "You forgot to supply an input and/or output directory."
  echo "USAGE: "
  echo "concatenate-barcoded-nanopore-reads.sh input-dir/ output-dir/"
  exit 1
fi

# loop through barcode directories, concatenating the fastq files into the output directory and naming them after the barcode number e.g. barcode01.all.fastq.gz
for barcodeDir in ${INPUT_DIR}/barcode*; do
  barcode=$(echo $barcodeDir | sed "s|.*barcode|barcode|")
  echo "barcode is set to: ${barcode}"
  echo "concatenating reads for:" $barcodeDir
  cat ${barcodeDir}/*.fastq.gz > ${OUTPUT_DIR}/${barcode}.all.fastq.gz
done

echo "DONE"