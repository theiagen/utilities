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
TERRA_DATA_UPLOAD_GSURI="fc-b4d25547-715f-4139-8276-a2aff0ae00c1/uploads"
LOCAL_ROOT_DIR="/data/cronjob/"
ROOT_ENTITY="ont_specimen"

# create date variable, if not passed in by user, set today's date in YYYY-MM-DD format
export TODAY_DATE=${1}
if [[ -z ${1} ]]; then
  export TODAY_DATE=$(date -I)
fi

# create dir for today's upload
make_directory ${LOCAL_ROOT_DIR}/${TODAY_DATE}
echo "TODAY_DATE set to: " $TODAY_DATE

# scan the default output directory set in MinKNOW interface, usually is located at `/data` or something similar on GridION filesystem
# detect when a new run is completed: perhaps based on the presence of a new subdirectory named after the sequencing run ID AND the presence of a `sequencing_summary.txt` file produced by Guppy to indicate that basecalling has completed

# find command to list all directories within /data that are not hidden and those that were created after 2023-04-17
# tail command is to remove the first in list, which would be just /data
# sed command is to remove "/data/" from beginning of output
find /data -maxdepth 1  -not -path '*/.*' -type d -newermt '2023-04-17' | tail -n+2 >${LOCAL_ROOT_DIR}/${TODAY_DATE}/LIST-OF-RUNDIRS.TXT

# download list of run_id 's from Terra, create a text file
# download entire prst ont_specimen Terra table to determine which runs have not been uploaded yet
docker run -e TODAY_DATE -u $(id -u):$(id -g) --rm -v ${LOCAL_ROOT_DIR}/${TODAY_DATE}:/data -v "$HOME"/.config:/.config quay.io/theiagen/terra-tools:0.2.2 bash -c "cd data; python3 /scripts/export_large_tsv/export_large_tsv.py --project theiagen-prscitrust --workspace PR-SCITRUST-COVID --entity_type ont_specimen --attribute_list run_id --tsv_filename /data/runIDs-in-terra-${TODAY_DATE}.tsv"

# get list of unique run ID's based on table downloaded from Terra
# tail +3 is to remove blank line and remove line with "run_id"
cut -f 2 ${LOCAL_ROOT_DIR}/${TODAY_DATE}/runIDs-in-terra-${TODAY_DATE}.tsv | sort | uniq | tail +3 >${LOCAL_ROOT_DIR}/${TODAY_DATE}/runIDs-in-terra-${TODAY_DATE}.unique.txt 

# compare our list of rundirs that are newer than 2023-04-17
# elif the rundir is NOT PRESENT, then upload the data to Terra
cat ${LOCAL_ROOT_DIR}/${TODAY_DATE}/LIST-OF-RUNDIRS.TXT | while read RUNDIR; do
  # set RUN_ID variable, strip off the /data/ in the beginning, use this variable going forward
  RUN_ID=$(echo $RUNDIR | sed 's|/data/||g'
  echo "RUN_ID is set to:" $RUN_ID
  
  # if the rundir IS PRESENT in list of run_id's from Terra, then skip
  
  
  
  # elif the rundir is NOT PRESENT, then upload the data to Terra
  
done 


# run the concatenate-barcoded-nanopore-reads.sh script on the directory of pass/ FASTQ files. If I recall correctly, it has subdirectories for each barcode
# TODO need to double check the output directory structure from MinKNOW/Guppy
concatenate-barcoded-nanopore-reads.sh ${RUNDIR} ${RUNDIR}/concatenated-fastqs

# gsutil cp’s data to Terra workspace GCP bucket
# TODO - set SEQ_RUN_ID variable somewhere before this. Perhaps just use RUNDIR if that matches the seq run ID
gsutil -m cp ${RUNDIR}/concatenated-fastqs/*.fastq.gz gs://${TERRA_DATA_UPLOAD_GSURI}/00000-${SEQ_RUN_ID}/

# create list of FASTQ GSURIs
gsutil -m ls gs://${TERRA_DATA_UPLOAD_GSURI}/00000-${SEQ_RUN_ID}/ >${RUNDIR}/concatenated-fastqs/FASTQ_GS_URIS.TXT

# Create Terra metadata TSV based on new GS URIs of FASTQ files & sequencing run ID
# WARNING - we may not know the sample IDs based on the FASTQ filenames or FASTQ headers within FASTQ files.

# 1st step insert headers into TSV
# Using 4 columns here: ont_specimen_id, reads, upload_date, and run_id
echo -e "entity:${ROOT_ENTITY}_id\treads\tupload_date\trun_id" > ${LOCAL_ROOT_DIR}/${TODAY_DATE}/terra_table_${RUNDIR}_for_upload.tsv

# 2nd step
# add row to TSV for every FASTQ file
cat ${RUNDIR}/concatenated-fastqs/FASTQ_GS_URIS.TXT | while read FASTQ_GSURI; do
  echo -e "${SAMPLE_ID}\t${FASTQ_GSURI}\t$(date -I)\t${RUNDIR}" >>${LOCAL_ROOT_DIR}/${TODAY_DATE}/terra_table_${RUNDIR}_for_upload.tsv

# 3rd step
# import TSV into Terra workspace
docker run -e RUNDIR -e TODAY_DATE -u $(id -u):$(id -g) --rm -v "$HOME/.config:/.config" -v $PWD:/data quay.io/theiagen/terra-tools:0.2.2 /bin/bash -c "cd data; python3 /terra-tools/scripts/import_large_tsv/import_large_tsv.py --project theiagen-prscitrust --workspace PR-SCITRUST-COVID --tsv /data/terra_table_${RUNDIR}_for_upload.tsv"



################# OLD CODE BELOW #################

# # download entire ucsd Terra table to determine which runs have not been uploaded yet
# docker run -u $(id -u):$(id -g) --rm -v "$HOME"/.config:/.config -v "${UCSD_ROOT_DIR}/${TODAY_DATE}":/data broadinstitute/terra-tools:tqdm bash -c "cd data; python3 /scripts/export_large_tsv/export_large_tsv.py --project cdph-terrabio-taborda-manual --workspace dataAnalysis_SARS-CoV-2_CDPH_Master_clone_2021-08-24 --entity_type ucsd_scrm_specimen --attribute_list run_id --tsv_filename /data/ucsd-master-runIDs-${TODAY_DATE}.tsv"

# cat entire Terra table; csvcut on only run_id column (-t for tab-delimited input, -x for suppressing empty lines)
# tail to show all lines except header; sort and uniq for obvious reasons
cat ${UCSD_ROOT_DIR}/${TODAY_DATE}/ucsd-ww-runIDs-${TODAY_DATE}.tsv | csvcut -x -t -c run_id | tail -n +2 | sort | uniq >${UCSD_ROOT_DIR}/${TODAY_DATE}/runIDs-in-ucsd-ww-workspace.txt

# cat entire Terra table; csvcut on only run_id column (-t for tab-delimited input, -x for suppressing empty lines)
# tail to show all lines except header; sort and uniq for obvious reasons
cat ${UCSD_ROOT_DIR}/${TODAY_DATE}/ucsd-master-runIDs-${TODAY_DATE}.tsv | csvcut -x -t -c run_id | tail -n +2 | sort | uniq >${UCSD_ROOT_DIR}/${TODAY_DATE}/runIDs-in-ucsd-workspace.txt

# create txt file with list of all runIDs on taborda bucket
rclone lsf buckets:${UCSD_TABORDA_BUCKET_GSURI}/ > ${UCSD_ROOT_DIR}/${TODAY_DATE}/ID_LIST_ALL_RUNIDs-${TODAY_DATE}.txt

# remove / from end of directory paths because it causes the grep to break
sed -i.bak 's/\///g' ${UCSD_ROOT_DIR}/${TODAY_DATE}/ID_LIST_ALL_RUNIDs-${TODAY_DATE}.txt

# grep for un-uploaded runs, if found (e.g., grep results in 0 lines), then upload the runs
cat ${UCSD_ROOT_DIR}/${TODAY_DATE}/ID_LIST_ALL_RUNIDs-${TODAY_DATE}.txt | while read runID; do
  # if a runID is NOT in the ucsd workspace (e.g., grep results in 0 lines), it is not uploaded
  if [ $(grep ${runID} ${UCSD_ROOT_DIR}/${TODAY_DATE}/runIDs-in-ucsd-workspace.txt | wc -l ) -eq 0 ] ; then
    # remove any runIDs on the do-not-upload-these.txt list
    if [ $(grep ${runID} ${UCSD_ROOT_DIR}/do-not-upload-these.txt | wc -l ) -eq 0 ] ; then
      # upload the runs that remain
      echo ${runID} >> ${UCSD_ROOT_DIR}/${TODAY_DATE}/RUNIDS_TO_UPLOAD.txt
    fi
  fi
done

# grep for un-uploaded runs in WW workspace, if found (e.g., grep results in 0 lines), then upload the runs
cat ${UCSD_ROOT_DIR}/${TODAY_DATE}/ID_LIST_ALL_RUNIDs-${TODAY_DATE}.txt | while read runID; do
  # if a runID is NOT in the ucsd workspace (e.g., grep results in 0 lines), it is not uploaded
  if [ $(grep ${runID} ${UCSD_ROOT_DIR}/${TODAY_DATE}/runIDs-in-ucsd-ww-workspace.txt | wc -l ) -eq 0 ] ; then
    # remove any runIDs on the do-not-upload-these.txt list
    if [ $(grep ${runID} ${UCSD_ROOT_DIR}/do-not-upload-these-ww.txt | wc -l ) -eq 0 ] ; then
      # upload the runs that remain
      echo ${runID} >> ${UCSD_ROOT_DIR}/${TODAY_DATE}/WW_RUNIDS_TO_UPLOAD.txt
    fi
  fi
done


if exists ${UCSD_ROOT_DIR}/${TODAY_DATE}/RUNIDS_TO_UPLOAD.txt ; then
  echo "New run(s) detected, now beginning upload process.."

  # upload the files in the un-uploaded runs
  cat ${UCSD_ROOT_DIR}/${TODAY_DATE}/RUNIDS_TO_UPLOAD.txt | while read RUNDIR; do
    export RUNDIR
    echo "TRANSFERRING $RUNDIR"
    rclone copy --log-file=${UCSD_ROOT_DIR}/${TODAY_DATE}/rclone-taborda-to-terra.log --log-level INFO --transfers 8 --include "*.fastq.gz" buckets:${UCSD_TABORDA_BUCKET_GSURI}/${RUNDIR}/ buckets:${UCSD_DATA_UPLOAD_GSURI}/${RUNDIR}/
    echo "CREATING FOFNs and PAUIs for $RUNDIR"
    rclone lsf --include "*R1*.gz" buckets:${UCSD_DATA_UPLOAD_GSURI}/${RUNDIR}/ > ${UCSD_ROOT_DIR}/${TODAY_DATE}/0_${RUNDIR}-R1-FOFN.txt
    rclone lsf --include "*R2*.gz" buckets:${UCSD_DATA_UPLOAD_GSURI}/${RUNDIR}/ > ${UCSD_ROOT_DIR}/${TODAY_DATE}/0_${RUNDIR}-R2-FOFN.txt
    cat ${UCSD_ROOT_DIR}/${TODAY_DATE}/0_${RUNDIR}-R1-FOFN.txt | cut -d '/' -f 6 | cut -d '_' -f 1 > ${UCSD_ROOT_DIR}/${TODAY_DATE}/0_${RUNDIR}-PAUIs.txt
    echo "TRANSFERRING FOFNs and PAUIs for $RUNDIR"
    rclone copy --log-file=${UCSD_ROOT_DIR}/${TODAY_DATE}/rclone-fofns-and-pauis-to-terra.log --log-level INFO --transfers 8 --include "*{FOFN,PAUIs}.txt" ${UCSD_ROOT_DIR}/${TODAY_DATE}/ buckets:${UCSD_DATA_UPLOAD_GSURI}/uploads/${RUNDIR}/
    echo "CREATING TERRA TABLE FOR $RUNDIR"  
  
    # make Terra metadata spreadsheet
    # create Terra table with gcp pointers
    # first insert headers into TSV
    
    echo -e "entity:${ROOT_ENTITY}_id\tpaui\tread1\tread2\tsequencing_lab\tupload_date\trun_id" > ${UCSD_ROOT_DIR}/${TODAY_DATE}/terra_table_${RUNDIR}_for_upload.tsv

    # loop through list of PAUIs to create one row per PAUI
    cat ${UCSD_ROOT_DIR}/${TODAY_DATE}/0_${RUNDIR}-PAUIs.txt | while read PAUI; do
      R1="gs://${UCSD_DATA_UPLOAD_GSURI}/${RUNDIR}/$(grep -E "${PAUI}.*R1.*" ${UCSD_ROOT_DIR}/${TODAY_DATE}/0_${RUNDIR}-R1-FOFN.txt)"
      R2="gs://${UCSD_DATA_UPLOAD_GSURI}/${RUNDIR}/$(grep -E "${PAUI}.*R2.*" ${UCSD_ROOT_DIR}/${TODAY_DATE}/0_${RUNDIR}-R2-FOFN.txt)"

      # check PAUIs for WW runs and do not upload them
      if [[ ${PAUI} = *"WW"* ]]; then
	echo "Skipping wastewater samples..."
      elif [[ ${PAUI} = *"Undetermined"* ]]; then
	echo "Skipping undetermined samples..."
      else      
        echo -e "${PAUI}\t${PAUI}\t${R1}\t${R2}\tUCSD-SCRM\t$(date -I)\t${RUNDIR}" >> ${UCSD_ROOT_DIR}/${TODAY_DATE}/terra_table_${RUNDIR}_for_upload.tsv
      fi
    done
    echo "Terra table has been created"
  
    echo "copying Terra table to Terra bucket"
    rclone copy -P --transfers 8 ${UCSD_ROOT_DIR}/${TODAY_DATE}/terra_table_${RUNDIR}_for_upload.tsv buckets:${UCSD_DATA_UPLOAD_GSURI}/uploads/${RUNDIR}/
   
    echo "importing terra data"
    # import to master workspace
    docker run -e RUNDIR -e TODAY_DATE -u $(id -u):$(id -g) --rm -v "$HOME/.config:/.config" -v /home/cronmaster/ucsd-cronjob/${TODAY_DATE}:/data broadinstitute/terra-tools:tqdm /bin/bash -c \
    "cd data; python3 /scripts/import_large_tsv/import_large_tsv.py --project cdph-terrabio-taborda-manual --workspace dataAnalysis_SARS-CoV-2_CDPH_Master_clone_2021-08-24 --tsv /data/terra_table_${RUNDIR}_for_upload.tsv"
    # import into dataUpload_UCSD workspace
    docker run -e RUNDIR -e TODAY_DATE -u $(id -u):$(id -g) --rm -v "$HOME/.config:/.config" -v /home/cronmaster/ucsd-cronjob/${TODAY_DATE}:/data broadinstitute/terra-tools:tqdm /bin/bash -c \
    "cd data; python3 /scripts/import_large_tsv/import_large_tsv.py --project cdph-terrabio-taborda-manual --workspace dataUpload_UCSD_SCRM --tsv /data/terra_table_${RUNDIR}_for_upload.tsv" 
  
    if [ $(rclone lsf --include "5*.fastq.gz" buckets:${UCSD_DATA_UPLOAD_GSURI}/${RUNDIR}/ | wc -l ) -gt 0 ] ; then

      rclone copy --log-file=${UCSD_ROOT_DIR}/${TODAY_DATE}/rclone-stacia-transfer.log --log-level INFO --transfers 8 --include "5*.fastq.gz" buckets:${UCSD_DATA_UPLOAD_GSURI}/${RUNDIR}/ buckets:kaiser-cdph-covidnet-data/ucsd-lab-sequences/fastqs/${RUNDIR}/
      echo "" >> ${UCSD_ROOT_DIR}/${TODAY_DATE}/stacia-notification.txt
      echo "Data has been transferred to Stacia's bucket!" >> ${UCSD_ROOT_DIR}/${TODAY_DATE}/stacia-notification.txt
      echo $(rclone lsf --include "5*.fastq.gz" buckets:${UCSD_DATA_UPLOAD_GSURI}/${RUNDIR}/ | wc -l) "files transferred" >> ${UCSD_ROOT_DIR}/${TODAY_DATE}/stacia-notification.txt
      echo "Path of files: gs://kaiser-cdph-covidnet-data/ucsd-lab-sequences/fastqs/${RUNDIR}" >> ${UCSD_ROOT_DIR}/${TODAY_DATE}/stacia-notification.txt

    fi 
  
  done

else
  echo "No new runs detected, ending process."
fi

echo "END"
