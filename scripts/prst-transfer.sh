#!/bin/bash
#
# requirements (have to be available on $PATH for user):
# sed, concatenate-barcoded-nanopore-reads.sh, docker
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

# set variables we will re-use
# PRST Terra workspace google bucket
TERRA_DATA_UPLOAD_GSURI="fc-b4d25547-715f-4139-8276-a2aff0ae00c1"
# local GridION filesystem root directory
LOCAL_ROOT_DIR="/data/cronjob/"
# variable for Terra data table root entity (does not includ "_id" at end)
ROOT_ENTITY="ont_specimen"

# create date variable, if not passed in by user, set today's date in YYYY-MM-DD format
export TODAY_DATE=${1}
if [[ -z ${1} ]]; then
  export TODAY_DATE=$(date -I)
fi

# create dir for today's upload and use this dir as the working directory for the cronjob script
make_directory ${LOCAL_ROOT_DIR}/${TODAY_DATE}
echo "TODAY_DATE set to:" $TODAY_DATE

# scan the default output directory set in MinKNOW interface, usually is located at `/data` on GridION
# detect when a new run is completed: based on the presence of a new subdirectory named after the sequencing run ID AND the presence of a `sequencing_summary.txt` file produced by Guppy to indicate that basecalling has completed

# find command to list all directories within /data that are not hidden and those that were created after 2023-04-17
# tail command is to remove the first in list, which would be just a line that says "/data"
find /data -maxdepth 1  -not -path '*/.*' -type d -newermt '2023-04-17' | tail -n+2 >${LOCAL_ROOT_DIR}/${TODAY_DATE}/LIST-OF-RUNDIRS.TXT

# download list of run_id 's from Terra, create a text file
# download entire prst ont_specimen Terra table to determine which runs have not been uploaded yet
docker run -e TODAY_DATE -u $(id -u):$(id -g) --rm -v ${LOCAL_ROOT_DIR}/${TODAY_DATE}:/data -v "$HOME"/.config:/.config quay.io/theiagen/terra-tools:0.2.2 bash -c "cd /data; python3 /scripts/export_large_tsv/export_large_tsv.py --project theiagen-prscitrust --workspace PR-SCITRUST-COVID --entity_type ont_specimen --attribute_list run_id --tsv_filename /data/runIDs-in-terra-${TODAY_DATE}.tsv"

# get list of unique run ID's based on table downloaded from Terra
# sed is to remove line with "run_id" column header
# tail +2 is to remove blank line at top of list (blank line is caused by samples in data table without a run_id)
cut -f 2 ${LOCAL_ROOT_DIR}/${TODAY_DATE}/runIDs-in-terra-${TODAY_DATE}.tsv | sed 's|run_id||g' sort | uniq | tail +2 >${LOCAL_ROOT_DIR}/${TODAY_DATE}/runIDs-in-terra-${TODAY_DATE}.unique.txt 

# compare our list of rundirs that are newer than 2023-04-17
# elif the rundir is NOT PRESENT, then upload the data to Terra
cat ${LOCAL_ROOT_DIR}/${TODAY_DATE}/LIST-OF-RUNDIRS.TXT | while read RUNDIR; do
  echo "RUNDIR is set to:" ${RUNDIR}
  echo

  # set RUN_ID variable, strip off the /data/ in the beginning, use this variable going forward
  RUN_ID=$(echo $RUNDIR | sed 's|/data/||g')
  echo "RUN_ID is set to:" $RUN_ID
  
  # if the run id IS PRESENT in list of run_id's from Terra, then skip
  if [ $(grep ${RUN_ID} ${LOCAL_ROOT_DIR}/${TODAY_DATE}/runIDs-in-terra-${TODAY_DATE}.unique.txt | wc -l ) -gt 0 ] ; then
      echo "RUN_ID ${RUN_ID} was found in the list of RUN ID's present in Terra, skipping upload..."
      # continue allows the script to run the remaining commands in the while loop (now that we have confirmed the existence of the sequencing_summary.txt file
      continue
  # else if the rundir is NOT PRESENT in list of run_id's on Terra, then prepare data and upload
  elif [ $(grep ${RUN_ID} ${LOCAL_ROOT_DIR}/${TODAY_DATE}/runIDs-in-terra-${TODAY_DATE}.unique.txt | wc -l ) -eq 0 ] ; then
      echo "RUN_ID ${RUN_ID} was NOT found in the list of RUN ID's present in Terra."
      echo "checking for a sequencing_summary.txt file now...."

      # check to see if a sequencing_summary.txt file exists within the rundir.
      # if the sequencing_summary.txt file does not exist, then exit the loop. This means either basecalling has not finished OR this directory is not a sequencing run directory
      if [ $(find ${RUNDIR} -maxdepth 3 -type f -iname "sequencing_summary_*.txt" | wc -l) -eq 0 ]; then
         echo "sequencing_summary.txt file not found, continuing on to next iteration of the loop now..."
        # continue sends the script back to the beginning of the while loop
        continue
      # if the sequencing_summary.txt file exists, then continue
      elif [ $(find ${RUNDIR} -maxdepth 3 -type f -iname "sequencing_summary_*.txt" | wc -l) -gt 0 ] ; then 
        echo "sequencing_summary.txt file WAS found, preparing files for upload to Terra now..."
      
      # else - any other outcomes, continue to next iteration of the loop 
      else
        echo "Not sure if a sequencing_summary.txt file was found or not, continuing to next iteration of loop now..."
        continue
      fi

      # run the concatenate-barcoded-nanopore-reads.sh script on the directory of fastq_pass/ FASTQ files
      echo "running concatenate-barcoded-nanopore-reads.sh script now..."
      concatenate-barcoded-nanopore-reads.sh ${RUNDIR}/*/*/fastq_pass ${RUNDIR}/concatenated-fastqs-for-terra-upload

      # gsutil cp’s data to Terra workspace GCP bucket
      echo "using gsutil cp to copy FASTQ.GZ files to Terra workspace Google storage bucket..."
      gsutil -m cp ${RUNDIR}/concatenated-fastqs-for-terra-upload/*.fastq.gz gs://${TERRA_DATA_UPLOAD_GSURI}/00000-${RUN_ID}

      # create list of FASTQ GSURIs
      gsutil -m ls gs://${TERRA_DATA_UPLOAD_GSURI}/00000-${RUN_ID}/ >${LOCAL_ROOT_DIR}/${TODAY_DATE}/FASTQ_GS_URIS.TXT

      # Create Terra metadata TSV based on new GS URIs of FASTQ files & sequencing run ID
      # 1st step insert headers into TSV
      # Using 4 columns here: ont_specimen_id, reads, upload_date, and run_id
      echo -e "entity:${ROOT_ENTITY}_id\treads\tupload_date\trun_id" > ${LOCAL_ROOT_DIR}/${TODAY_DATE}/00_terra_table_${RUN_ID}_for_upload.tsv

      # 2nd step
      # add row to TSV for every FASTQ file
      # using a "while" loop to loop through all FASTQ files uploaded to Terra
      cat ${LOCAL_ROOT_DIR}/${TODAY_DATE}/FASTQ_GS_URIS.TXT | while read FASTQ_GSURI; do
      echo -e "CVL_sample_identifier\t${FASTQ_GSURI}\t$(date -I)\t${RUN_ID}" >>${LOCAL_ROOT_DIR}/${TODAY_DATE}/00_terra_table_${RUN_ID}_for_upload.tsv
      echo "finished adding rows to the Terra metadata TSV"
      done

      # copy the Terra metadata TSV to the Terra workspace google storage bucket (specifically in RUN_ID subdirectory)
      echo "copying Terra metadata table to Terra workspace GCP storage bucket...."
      gsutil cp ${LOCAL_ROOT_DIR}/${TODAY_DATE}/00_terra_table_${RUN_ID}_for_upload.tsv gs://${TERRA_DATA_UPLOAD_GSURI}/00000-${RUN_ID}/
  fi
done 
echo
echo "END"
