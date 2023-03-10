#!/bin/bash
set -e

# filename: standard_dashboard.sh
# authors: Sage Wright, Kevin Libuit, Frank Ambrosio

VERSION="Google Dashboarding v0.1"

showHelp() {
cat << EOF
Google Dashboarding v0.1 
This script is configured to work within a Google Batch job managed by a Google Workflow and Trigger. 
The following variables need to be passed in as input parameters. 
CAUTION: The entire command length must be under 400 characters; using the short version of arguments is recommended

Usage: ./standard_dashboard.sh 
	[ -v | --version ] display version and quit 
	[ -h | --help ] display this help message and quit
	[ -d | --dashboard-gcp-uri ] the gcp bucket where dashboard processing will occur ("gs://louisiana-dashboarding-processing")
	[ -j | --dashboard-newline-json ] the name of the dashboard newline json file to uploaded to Big Query ("gisaid_louisiana_data.json")
	[ -s | --dashboard-schema ] the path in the mounted directory of where you can find the dashboard schema ("/data/bq_schema/schema_LA_v6.json")
	[ -b | --gisaid-backup-dir ] the path in the mounted directory of where the gisaid data will be copied ("/data/input_gisaid")
	[ -o | --output-dir ] the path in the mounted direcotry where the output files will be written ("/data")
	[ -t | --trigger-bucket ] the gcp bucket where the trigger will watch ("gs://louisiana-gisaid-data")
	[ -g | --terra-gcp-uri ] the dashboard terra bucket (gs://fc-6c0c9352-49f4-4673-a41c-71baddb16f42")
	[ -r | --terra-table-root-entity ] the terra table you want the data stored ("gisaid_louisiana_data")
	[ -p | --terra-project ] the project hosting the terra workspace ("cdc-terra-la-phl")
	[ -w | --terra-workspace ] the terra workspace ("CDC-COVID-LA-Dashboard-Test")
	[ -q | --big-query-table-name ] the name of the big query table to upload to ("sars_cov_2_dashboard.workflow_la_state_gisaid_specimens_test")
	[ -m | --metadata-parameters ] (optional) any additional metadata cleanser parameter (enclose in quotes). available options: "--puertorico"
	[ -i | --input-tar-file ] the tar file given to the script by the Google Trigger

Happy dashboarding!
EOF
}

# use getopt to parse the input arguments
PARSED_ARGUMENTS=$(getopt -n "standard-dashboard" -o "hvd:j:s:b:o:t:g:r:p:w:q:m::i:" -l "version,help,dashboard-gcp-uri:,dashboard-newline-json:,dashboard-schema:,gisaid-backup-dir:,output-dir:,trigger-bucket:,terra-gcp-uri:,terra-table-root-entity:,terra-project:,terra-workspace:,big-query-table-name:,metadata-parameters::,input-tar-file:" -a -- "$@")

eval set -- "$PARSED_ARGUMENTS"

while true; do
  case "$1" in
	-v|--version)
      echo $VERSION; exit 0;;
    -h|--help)
      showHelp; exit 0;;
    -d|--dashboard-gcp-uri)
      dashboard_gcp_uri=$2; shift 2;;
    -j|--dashboard-newline-json)
      dashboard_newline_json=$2; shift 2;;
    -s|--dashboard_schema)
      dashboard_schema=$2; shift 2;;
    -b|--gisaid-backup-dir)
      gisaid_backup_dir=$2; shift 2;;
    -o|--output-dir)
      output_dir=$2; shift 2;;
    -t|--trigger-bucket)
      trigger_bucket=$2; shift 2;;
    -g|--terra-gcp-uri)
      terra_gcp_uri=$2; shift 2;;
    -r|--terra-table-root-entity)
      terra_table_root_entity=$2; shift 2;;
    -p|--terra-project)
      terra_project=$2; shift 2;;
    -w|--terra-workspace)
      terra_workspace=$2; shift 2;;
    -q|--big-query-table-name)
      big_query_table_name=$2; shift 2;;
    -m|--metadata-parameters)
      case "$2" in
        "") metadata_cleanser_parameters=''; shift 2;;
        *) metadata_cleanser_parameters=$2; shift 2;;
      esac ;;
	-i|--input-tar-file)
	  input_tar_file=$2; shift 2;;
    --) shift; break ;;
    *) echo "Unexpected option: $1 -- this should not happen."; exit 1;;
  esac
done

### SET RE-USED FUNCTIONS

# this function will make a direcotry if it does not already exist
make_directory() {
  if [ -e $1 ]; then
	echo "Directory "$1" already exists"
  else
	mkdir -v $1
  fi
}

### BEGIN DASHBOARD FUNCTION

# Set date tag
date_tag=$(date +"%Y-%m-%d-%Hh-%Mm-%Ss")

# Create output subdirectories if they do not yet exist:
make_directory ${output_dir}/automation_logs
make_directory ${output_dir}/gisaid_processing 
make_directory ${output_dir}/backup_jsons

# echo the variables that were provided
echo -e "Dashboarding Automated System initiated at ${date_tag}\n" | tee ${output_dir}/automation_logs/dashboard-${date_tag}.log
echo -e "Input variables:\ndashboard_gcp_uri: ${dashboard_gcp_uri},\ndashboard_newline_json: ${dashboard_newline_json},\ndashboard_bq_load_schema: ${dashboard_schema},\ngisaid_backup_dir: ${gisaid_backup_dir},\nmounted_output_dir: ${output_dir},\ntrigger_bucket_gcp_uri: ${trigger_bucket},\nterra_gcp_uri: ${terra_gcp_uri},\nterra_table_root_entity: ${terra_table_root_entity},\nterra_project: ${terra_project},\nterra_workspace: ${terra_workspace},\nbig_query_table_name: ${big_query_table_name}\n" >> ${output_dir}/automation_logs/dashboard-${date_tag}.log

# take in file as input from trigger
file=${trigger_bucket}/${input_tar_file}
filename=${input_tar_file}

# indicate that a file has been successfully passed to the script
echo "The file '$filename' appeared in directory '$trigger_bucket'" >> ${output_dir}/automation_logs/dashboard-${date_tag}.log

# copy the file to the gisaid_backup directory
gsutil cp ${file} ${gisaid_backup_dir}/

# if the created file is a gisaid_auspice input file, integrate into Terra and BQ
if [[ "$file" == *"gisaid_auspice_input"*"tar" ]]; then
  # indicate the new file is a gisaid file
  echo -e "New gisaid file identified: $filename \n" >> ${output_dir}/automation_logs/dashboard-${date_tag}.log

  # set up gisaid processing directory using the current date
  gisaid_dir="${output_dir}/gisaid_processing/${date_tag}"

  # run the following compilation of scripts:
  SCRIPTS="
  # decompress gisaid input tar ball into specific date processing directory
  \n
  mkdir ${gisaid_dir}
  \n
  tar -xf ${gisaid_backup_dir}/$filename -C ${gisaid_dir}
  \n
  \n
  # Create individual fasta files from GISAID multifasta
  \n
  python3 /data/utilities/scripts/gisaid_multifasta_parser.py ${gisaid_dir}/*.sequences.fasta ${gisaid_dir}
  \n
  \n
  # Deposit individual fasta files into Terra GCP bucket
  \n
  gsutil -m cp ${gisaid_dir}/individual_gisaid_assemblies_$(date -I)/*.fasta ${terra_gcp_uri}/uploads/gisaid_individual_assemblies_$(date -I)/
  \n
  \n
  # Create and import Terra Data table containing GCP pointers to deposited assemblies
  \n
  /data/utilities/scripts/terra_table_from_gcp_assemblies.sh ${terra_gcp_uri}/uploads/gisaid_individual_assemblies_$(date -I) ${terra_project} ${terra_workspace} ${terra_table_root_entity} ${gisaid_dir} \".fasta\" $(date -I)
  \n
  \n
  # Capture, reformat, and prune GISAID metadata
  \n
  python3 /data/utilities/scripts/gisaid_metadata_cleanser.py ${gisaid_dir}/*.metadata.tsv ${gisaid_dir}/gisaid_metadata_${date_tag}.tsv ${terra_table_root_entity} ${metadata_cleanser_parameters}
  \n
  \n
  # Import formatted data table into Terra
  \n
  python3 /scripts/import_large_tsv/import_large_tsv.py --project ${terra_project} --workspace ${terra_workspace} --tsv ${gisaid_dir}/gisaid_metadata_${date_tag}.tsv
  \n
  \n
  # Capture the entire Terra data table as a tsv
  \n
  python3 /scripts/export_large_tsv/export_large_tsv.py --project ${terra_project} --workspace ${terra_workspace} --entity_type ${terra_table_root_entity} --tsv_filename ${gisaid_dir}/full_${terra_table_root_entity}_terra_table_${date_tag}.tsv
  \n
  \n
  # Convert the local Terra table tsv into a newline json
  \n
  python3 /data/utilities/scripts/tsv_to_newline_json.py ${gisaid_dir}/full_${terra_table_root_entity}_terra_table_${date_tag}.tsv ${gisaid_dir}/${terra_table_root_entity}_${date_tag}
  \n
  \n
  # Push newline json to the dashboard GCP bucket and backup folder
  \n
  gsutil cp ${gisaid_dir}/${terra_table_root_entity}_${date_tag}.json ${dashboard_gcp_uri}/${terra_table_root_entity}.json
  \n
  gsutil cp ${gisaid_dir}/${terra_table_root_entity}_${date_tag}.json ${output_dir}/backup_jsons/
  \n
  \n
  # Load newline json to Big Query 
  \n
  bq load --ignore_unknown_values=true --replace=true --source_format=NEWLINE_DELIMITED_JSON ${big_query_table_name} ${dashboard_gcp_uri}/${terra_table_root_entity}.json ${dashboard_schema}
  \n
  \n
  "
  # write the commands that will be run to the automation log
  echo -e "#### Capturing GISAID data into Dashboard (${date_tag}) ####\n" >> ${output_dir}/automation_logs/dashboard-${date_tag}.log
  echo -e $SCRIPTS >> ${output_dir}/automation_logs/dashboard-${date_tag}.log

  # run the scripts
  echo -e $SCRIPTS | bash -x

else
  # display error message if the file is not a GISAID file
  echo "The file was not recognized as a GISAID auspice tar file."
fi
