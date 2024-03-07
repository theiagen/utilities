#!/bin/bash

DATE_TAG=$(date +"%Y-%m-%d-%Hh-%Mm-%Ss")
TERRA_PROJECT=$1
WORKSPACE_NAME=$2
ORIGIN_TABLE_NAME=$3
# the DEST_TABLE_ID  field is used to fill out the "source_terra_table" field in output JSON instead of using the ORIGIN_TABLE_NAME
# this allows to differentiate between identically-named data tables coming from different counties or labs. eg. "sample" data table for all ClearLabs users
DEST_TABLE_ID=$4
DEST_BUCKET_URI=$5
OUTPUT_FILENAME_PREFIX=$6

# create variable for log filename
LOG_FILE=${DEST_TABLE_ID}.${OUTPUT_FILENAME_PREFIX}.${DATE_TAG}.log

# initialize log
echo "DEBUG: Sending ${ORIGIN_TABLE_NAME} to ${DEST_BUCKET_URI}" | tee logs/${LOG_FILE}

# if user specifies 'date' as OUTPUT_FILENAME_PREFIX, reset variable to default table filename ${ORIGIN_TABLE_NAME}_${DATE_TAG}.json
if [ "${OUTPUT_FILENAME_PREFIX}" == "date" ]; then
  echo 'DEBUG: User specified "date"' "for OUTPUT_FILENAME_PREFIX, final output JSON will be named: ${DEST_BUCKET_URI}${ORIGIN_TABLE_NAME}_${DATE_TAG}.json" | tee logs/${LOG_FILE}
  OUTPUT_FILENAME_PREFIX="${DEST_TABLE_ID}_${DATE_TAG}"
else
  echo "DEBUG: User did not specify 'date' for OUTPUT_FILENAME_PREFIX, final output JSON will be named: ${DEST_BUCKET_URI}${ORIGIN_TABLE_NAME}.json" | tee logs/${LOG_FILE}
  OUTPUT_FILENAME_PREFIX="${ORIGIN_TABLE_NAME}"
fi

# download Terra table TSV using export_large_tsv.py from Broad
echo "DEBUG: downloading Terra table (${ORIGIN_TABLE_NAME}) from ${WORKSPACE_NAME}" | tee logs/${LOG_FILE}
python3 /scripts/export_large_tsv/export_large_tsv.py \
  --project "${TERRA_PROJECT}" \
  --workspace "${WORKSPACE_NAME}" \
  --entity_type "${ORIGIN_TABLE_NAME}" \
  --page_size 5000 \
  --tsv_filename "${DEST_TABLE_ID}_${DATE_TAG}.tsv"

# add new column
echo "DEBUG: adding new column" | tee logs/${LOG_FILE}
sed -i "s/$/\t${DEST_TABLE_ID}/" "${DEST_TABLE_ID}_${DATE_TAG}.tsv"

# rename header to 'source_terra_table'
echo "DEBUG: renaming header" | tee logs/${LOG_FILE}
sed -i "1{s/${DEST_TABLE_ID}$/source_terra_table/}" "${DEST_TABLE_ID}_${DATE_TAG}.tsv"

# convert TSV to newline-delimited JSON
echo "DEBUG: converting TSV to newline-delimited JSON" >> logs/${LOG_FILE}
python3 /utilities-0.2/scripts/tsv_to_newline_json.py "${DEST_TABLE_ID}_${DATE_TAG}.tsv" "${OUTPUT_FILENAME_PREFIX}"

# copy to GCS bucket
echo "DEBUG: copying to GCS bucket" | tee logs/${LOG_FILE}
gsutil -m cp "${OUTPUT_FILENAME_PREFIX}.json" "${DEST_BUCKET_URI}${OUTPUT_FILENAME_PREFIX}.json"

echo "DEBUG: removing TSV and JSON files" | tee logs/${LOG_FILE}
rm -v "${DEST_TABLE_ID}_${DATE_TAG}.tsv"
rm -v "${OUTPUT_FILENAME_PREFIX}.json"

echo "DEBUG: loading to big query" | tee logs/${LOG_FILE}
bq load --ignore_unknown_values=true --project_id=general-theiagen --replace=true --source_format=NEWLINE_DELIMITED_JSON pr_sci_trust_dashboard_processing.${OUTPUT_FILENAME_PREFIX} ${DEST_BUCKET_URI}${OUTPUT_FILENAME_PREFIX}.json schema_v3.json

echo "DEBUG: done" | tee logs/${LOG_FILE}