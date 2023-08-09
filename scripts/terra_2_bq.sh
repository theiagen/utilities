date_tag=$(date +"%Y-%m-%d-%Hh-%Mm-%Ss")
terra_project=$1
workspace_name=$2
table_name=$3
# re-enabling table_id to use to fill out the "source_terra_table" field in output JSON instead of using the table_name
# allows to differentiate between identically-named data tables coming from different counties or labs. eg. "sample" data table for all ClearLabs users
table_id=$4
gcs_uri=$5
output_filename_prefix=$6

# if user specifies 'date' as output_filename_prefix, reset variable to default table filename ${table_name}_${date_tag}.json
if [ "${output_filename_prefix}" == "date" ]; then
  echo 'DEBUG: User specified "date"' "for output_filename_prefix, final output JSON will be named: ${gcs_uri}${table_name}_${date_tag}.json"
  output_filename_prefix="${table_id}_${date_tag}"
else
  echo "DEBUG: User did not specify 'date' for output_filename_prefix, final output JSON will be named: ${gcs_uri}${table_name}.json"
  output_filename_prefix="${table_name}"
fi

# download Terra table TSV using export_large_tsv.py from Broad
echo "DEBUG: downloading Terra table"
python3 /scripts/export_large_tsv/export_large_tsv.py \
  --project "${terra_project}" \
  --workspace "${workspace_name}" \
  --entity_type "${table_name}" \
  --page_size 5000 \
  --tsv_filename "${table_id}_${date_tag}.tsv"

# add new column
echo "DEBUG: adding new column"
sed -i "s/$/\t${table_id}/" "${table_id}_${date_tag}.tsv"

# rename header
echo "DEBUG: renaming header"
sed -i "1{s/${table_id}$/source_terra_table/}" "${table_id}_${date_tag}.tsv"

# convert TSV to newline-delimited JSON
echo "DEBUG: converting TSV to newline-delimited JSON"
python3 /utilities-0.2/scripts/tsv_to_newline_json.py "${table_id}_${date_tag}.tsv" "${output_filename_prefix}"

# copy to GCS bucket
echo "DEBUG: copying to GCS bucket"
gsutil -m cp "${output_filename_prefix}.json" "${gcs_uri}${output_filename_prefix}.json"

echo "DEBUG: removing TSV and JSON files"
rm -v "${table_id}_${date_tag}.tsv"
rm -v "${output_filename_prefix}.json"
gsutil rm -v gs://terra_2_bq_cdph/"${output_filename_prefix}.json"
gsutil rm -v gs://terra_2_bq_cdph/"${table_id}_${date_tag}.tsv"


echo "DEBUG: done"