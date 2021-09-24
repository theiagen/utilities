#!/bin/bash
#
# This script separates single end and paired end reads by 
# moving SE reads to a different directory
#
#


# function to check if there are any files (one or more files) that match the given input
# https://stackoverflow.com/questions/24615535/bash-check-if-file-exists-with-double-bracket-test-and-wildcards
exists() { [[ -f $1 ]]; }

# This function will check to make sure the directory doesn't already exist before trying to create it
make_directory() {
    if [ -e $1 ]; then
        echo "Directory "$1" already exists"
    else
        mkdir -v $1
    fi
}

TARGET_DIR=$1
echo "TARGET_DIR is set to:" $TARGET_DIR

DEST_DIR=$2
echo "DEST_DIR is set to:" $DEST_DIR

# make destination dir if it doesn't exist
make_directory ${DEST_DIR}

# check for first and second argument; if blank then exit
if [[ -z "${1}" || -z "${2}" ]]; then
  echo "You forgot to supply a target and/or destination directory."
  echo "USAGE: "
  echo "move-SE-reads.sh target-dir/ destination-for-single-end-reads/"
  echo
  exit 1
fi

# count number of fastq.gz fles in target_dir
NUM_FASTQ_GZ_FILES=$(ls -f1 ${TARGET_DIR}/*.fastq.gz | wc -l)

# check to see if target_dir has .fastq.gz files
# and to see if dest_dir does NOT have .fastq.gz files
if [[ ${NUM_FASTQ_GZ_FILES} -eq 0 ]]; then
  echo "There are no .fastq.gz files in $TARGET_DIR"
  exit 1
elif [ -f ${DEST_DIR}/*.fastq.gz ]; then
  echo "There are .fastq.gz files in $DEST_DIR, please use another directory"
  exit 1
fi


# generate a list of IDs. ID is the part of filename before _L001_R1_001.fastq.gz
# example: 3000140409_S1881_L002_R1_001.fastq.gz; ID=3000140409_S1881
ID_list="$(find ${TARGET_DIR} -maxdepth 1 -type f -name "*.fastq.gz" | while read F; do basename $F | rev | cut -c 22- | rev; done | sort | uniq)"

# set counters to 0, increment in loop
num_IDs_with_more_than_two_files=0
num_IDs_with_one_file=0
num_IDs_with_two_files=0

# for ID in ID_list; look for files, count how many there are
# if there are more than two files for an ID, increment counter by 1
# if there are exactly two files for an ID, increment counter by 1
# if there is only one file, increment other counter by 1
for ID in ${ID_list}; do
  num_files=$(ls -f1 ${ID}* | wc -l)
  echo "number of files for ${ID}" ${num_files}
  if [[ ${num_files} -gt 2 ]]; then
    echo "There are more than two files for" $ID
    ((num_IDs_with_more_than_two_files++))
  elif [[ ${num_files} -eq 2 ]]; then
    echo "There are two files for" $ID
    ((num_IDs_with_two_files++))
  else
    echo "There is only one file for" ${ID}
    ((num_IDs_with_one_file++))
    echo "moving single end files into ${DEST_DIR}"
    mv -v ${ID}*.gz ${DEST_DIR}
    echo
  fi
done

# summary
echo "number of IDs with more than two files:" ${num_IDs_with_more_than_two_files}
echo
echo "number of IDs with two files:" ${num_IDs_with_two_files}
echo
echo "number of IDs with only one file:" ${num_IDs_with_one_file}

echo "END"
date
