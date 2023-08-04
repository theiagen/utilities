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
  echo 'User specified "date"' "for output_filename_prefix, final output JSON will be named: ${gcs_uri}${table_name}_${date_tag}.json"
  output_filename_prefix="${table_name}_${date_tag}"
fi

echo "***Exporting Terra table ${table_name} from workspace ${workspace_name} in Terra project ${terra_project}***"

# download Terra table TSV using export_large_tsv.py from Broad
python3 /scripts/export_large_tsv/export_large_tsv.py \
  --project "${terra_project}" \
  --workspace "${workspace_name}" \
  --entity_type "${table_name}" \
  --page_size 5000 \
  --tsv_filename "${table_name}_${date_tag}.tsv"

echo -e "\n::Procesing ${table_name} for export (${date_tag})::"
echo
echo "entering python block of code...."

# add new column
sed -i 's/$/\t${table_id}/' "${table_name}_${date_tag}.tsv"

# rename header
sed -i '1{s/${table_id}$/source_terra_table/}' ${table_name}_${date_tag}.tsv

# additionally take cleaned-TSV and create nlJSON

python tsv_to_newline_json.py <arguments>

gsutil -m cp "${table_name}.json" "${gcs_uri}${output_filename_prefix}.json"
