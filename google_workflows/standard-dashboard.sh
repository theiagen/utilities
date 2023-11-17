#!/bin/bash
set -e

# filename: standard_dashboard.sh
# authors: Sage Wright, Kevin Libuit, Frank Ambrosio

VERSION="Google Dashboarding v0.3"

showHelp() {
cat << EOF
Google Dashboarding v0.3 
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
	[ -m | --puerto-rico ] apply Puerto Rico-specific changes. available options: true or false
	[ -i | --input-tar-file ] the tar file given to the script by the Google Trigger
  [ -k | --skip-bq-load ] skips the bq load step. available options: true or false
  [ -x | --helix ] apply Helix-specific changes. available options: true or false
Happy dashboarding!
EOF
}

# use getopt to parse the input arguments
PARSED_ARGUMENTS=$(getopt -n "standard-dashboard" -o "hvd:s:b:o:t:g:r:p:w:q:m:i:k:x:" -l "version,help,dashboard-gcp-uri:,dashboard-schema:,gisaid-backup-dir:,output-dir:,trigger-bucket:,terra-gcp-uri:,terra-table-root-entity:,terra-project:,terra-workspace:,big-query-table-name:,puerto-rico:,input-tar-file:,skip-bq-load:,helix:" -a -- "$@")
eval set -- "$PARSED_ARGUMENTS"

while true; do
  case "$1" in
    -v|--version)
      echo $VERSION; exit 0;;
    -h|--help)
      showHelp; exit 0;;
    -d|--dashboard-gcp-uri)
      dashboard_gcp_uri=$2; shift 2;;
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
    -m|--puerto-rico)
      puerto_rico=$2; shift 2;;
    -i|--input-tar-file)
      input_tar_file=$2; shift 2;;
    -k|--skip-bq-load)
      skip_bq_load=$2; shift 2;;
    -x|--helix)
      helix=$2; shift 2;;
    --) shift; break ;;
      *) echo "Unexpected option: $1 -- this should not happen."; exit 1;;
  esac
done

### SET RE-USED FUNCTIONS

# this function will make a directory if it does not already exist
make_directory() {
  if [ -e $1 ]; then
	echo "Directory "$1" already exists"
  else
	mkdir -v $1
  fi
}

### BEGIN DASHBOARD FUNCTION

# Set date tag
date_tag=$(date +"%Y-%m-%d-%Hh-%Mm-%Ss")_${RANDOM}

# Create output subdirectories if they do not yet exist:
make_directory ${gisaid_backup_dir}/
make_directory ${output_dir}/automation_logs
make_directory ${output_dir}/gisaid_processing 
make_directory ${output_dir}/backup_jsons

# echo the variables that were provided
echo -e "Dashboarding Automated System initiated at ${date_tag}\n" | tee ${output_dir}/automation_logs/dashboard-${date_tag}.log
echo "Input variables:" | tee -a ${output_dir}/automation_logs/dashboard-${date_tag}.log
echo "dashboard_gcp_uri: ${dashboard_gcp_uri}," | tee -a ${output_dir}/automation_logs/dashboard-${date_tag}.log
echo "dashboard_newline_json: ${dashboard_newline_json}," | tee -a ${output_dir}/automation_logs/dashboard-${date_tag}.log
echo "dashboard_bq_load_schema: ${dashboard_schema}" | tee -a ${output_dir}/automation_logs/dashboard-${date_tag}.log
echo "gisaid_backup_dir: ${gisaid_backup_dir}" | tee -a ${output_dir}/automation_logs/dashboard-${date_tag}.log
echo "output_dir: ${output_dir}" | tee -a ${output_dir}/automation_logs/dashboard-${date_tag}.log
echo "trigger_bucket: ${trigger_bucket}" | tee -a ${output_dir}/automation_logs/dashboard-${date_tag}.log
echo "terra_gcp_uri: ${terra_gcp_uri}" | tee -a ${output_dir}/automation_logs/dashboard-${date_tag}.log
echo "terra_table_root_entity: ${terra_table_root_entity}" | tee -a ${output_dir}/automation_logs/dashboard-${date_tag}.log
echo "terra_project: ${terra_project}" | tee -a ${output_dir}/automation_logs/dashboard-${date_tag}.log
echo "terra_workspace: ${terra_workspace}" | tee -a ${output_dir}/automation_logs/dashboard-${date_tag}.log
echo "big_query_table_name: ${big_query_table_name}" | tee -a ${output_dir}/automation_logs/dashboard-${date_tag}.log
echo "puerto_rico: ${puerto_rico}" | tee -a ${output_dir}/automation_logs/dashboard-${date_tag}.log
echo "input_tar_file: ${input_tar_file}" | tee -a ${output_dir}/automation_logs/dashboard-${date_tag}.log
echo "skip_bq_load: ${skip_bq_load}" | tee -a ${output_dir}/automation_logs/dashboard-${date_tag}.log
echo -e "helix: ${helix}\n" | tee -a ${output_dir}/automation_logs/dashboard-${date_tag}.log

# take in file as input from trigger
file=${trigger_bucket}/${input_tar_file}
filename=${input_tar_file}

# indicate that a file has been successfully passed to the script
echo "The file '$filename' appeared in directory '$trigger_bucket'" | tee -a ${output_dir}/automation_logs/dashboard-${date_tag}.log

# copy the file to the gisaid_backup directory
gsutil cp ${file} ${gisaid_backup_dir}/

# if the created file is a gisaid_auspice input file, integrate into Terra and BQ
if [[ "$file" == *"gisaid_auspice_input"*"tar" ]]; then
  # indicate the new file is a gisaid file
  echo -e "New gisaid file identified: $filename \n" >> ${output_dir}/automation_logs/dashboard-${date_tag}.log

  # set up gisaid processing directory using the current date
  gisaid_dir="${output_dir}/gisaid_processing/${date_tag}"

  # decompress gisaid input tar ball into specific date processing directory
  echo "Decompressing ${filename} into ${gisaid_dir}" >> ${output_dir}/automation_logs/dashboard-${date_tag}.log
  mkdir ${gisaid_dir}
  tar -xf ${gisaid_backup_dir}/${filename} -C ${gisaid_dir}
  
  # Create individual fasta files from GISAID multifasta
  echo "Creating individual fasta files from GISAID multifasta" >> ${output_dir}/automation_logs/dashboard-${date_tag}.log
  python3 /scripts/gisaid_multifasta_parser.py ${gisaid_dir}/*.sequences.fasta ${gisaid_dir} ${puerto_rico} ${helix}
  
  # Deposit individual fasta files into Terra GCP bucket    
  echo "Depositing individual fasta files into Terra GCP bucket" >> ${output_dir}/automation_logs/dashboard-${date_tag}.log
  gsutil -m cp ${gisaid_dir}/individual_gisaid_assemblies_$(date -I)/*.fasta ${terra_gcp_uri}/uploads/gisaid_individual_assemblies_${date_tag}/

  # Create and import Terra Data table containing GCP pointers to deposited assemblies
  echo "Creating and importing Terra Data table containing GCP pointers to deposited assemblies" >> ${output_dir}/automation_logs/dashboard-${date_tag}.log
  /scripts/terra_table_from_gcp_assemblies.sh ${terra_gcp_uri}/uploads/gisaid_individual_assemblies_${date_tag} ${terra_project} ${terra_workspace} ${terra_table_root_entity} ${gisaid_dir} ".fasta" ${date_tag}

  # Capture, reformat, and prune GISAID metadata
  echo "Capturing, reformatting, and pruning GISAID metadata" >> ${output_dir}/automation_logs/dashboard-${date_tag}.log
  python3 /scripts/gisaid_metadata_cleanser.py ${gisaid_dir}/*.metadata.tsv ${gisaid_dir}/gisaid_metadata_${date_tag}.tsv ${terra_table_root_entity} ${puerto_rico} ${helix}

  # Add sequencing lab column to metadata table if Helix data
  if ${helix} ; then  
    echo "Adding the sequencing lab column and upload date to metadata table" >> ${output_dir}/automation_logs/dashboard-${date_tag}.log
    awk -i inplace 'BEGIN{OFS="\t"} {sub(/\r$/,""); print $0, (NR>1 ? "Helix" : "sequencing_lab")}' ${gisaid_dir}/gisaid_metadata_${date_tag}.tsv
    awk -i inplace -v date=$(date -I) 'BEGIN{OFS="\t"} {sub(/\r$/,""); print $0, (NR>1 ? date : "upload_date")}' ${gisaid_dir}/gisaid_metadata_${date_tag}.tsv
    echo "Renaming 'division' column to 'state'" >> ${output_dir}/automation_logs/dashboard-${date_tag}.log
    sed -i '1s/division/state/' ${gisaid_dir}/gisaid_metadata_${date_tag}.tsv
  fi

  # Import formatted data table into Terra
  echo "Importing formatted data table into Terra" >> ${output_dir}/automation_logs/dashboard-${date_tag}.log
  python3 /scripts/import_large_tsv/import_large_tsv.py --project ${terra_project} --workspace ${terra_workspace} --tsv ${gisaid_dir}/gisaid_metadata_${date_tag}.tsv
  
  if ${skip_bq_load} ; then

    # Make a set table
    echo "Making a set table" >> ${output_dir}/automation_logs/dashboard-${date_tag}.log
    /scripts/make_terra_set.sh ${terra_gcp_uri}/uploads/gisaid_individual_assemblies_${date_tag} ${terra_project} ${terra_workspace} ${terra_table_root_entity} ${gisaid_dir} ".fasta" ${date_tag}
    
    # establish google auth token
    echo "Establishing google auth token" >> ${output_dir}/automation_logs/dashboard-${date_tag}.log
    TOKEN=`gcloud auth print-access-token`

    # Run TheiaCoV_FASTA on the set
    echo "Running TheiaCoV_FASTA on the set" >> ${output_dir}/automation_logs/dashboard-${date_tag}.log
    curl -X 'POST' \
      "https://api.firecloud.org/api/workspaces/${terra_project}/${terra_workspace}/submissions" \
      -H 'accept: */*' \
      -H "Authorization: Bearer ${TOKEN}" \
      -H 'Content-Type: application/json' \
      -d "{
      \"methodConfigurationNamespace\": \"${terra_project}\",
      \"methodConfigurationName\": \"TheiaCoV_FASTA_PHB\",
      \"entityType\": \"${terra_table_root_entity}_set\",
      \"entityName\": \"${date_tag}-set\",
      \"expression\": \"this.${terra_table_root_entity}s\",
      \"useCallCache\": true,
      \"deleteIntermediateOutputFiles\": false,
      \"useReferenceDisks\": false,
      \"memoryRetryMultiplier\": 1,
      \"workflowFailureMode\": \"NoNewCalls\",
      \"userComment\": \"${date_tag}-set automatically launched\"
      }"
    
  else 

    # Capture the entire Terra data table as a tsv
    echo "Capturing the entire Terra data table as a tsv" >> ${output_dir}/automation_logs/dashboard-${date_tag}.log
    python3 /scripts/export_large_tsv/export_large_tsv.py --project ${terra_project} --workspace ${terra_workspace} --entity_type ${terra_table_root_entity} --tsv_filename ${gisaid_dir}/full_${terra_table_root_entity}_terra_table_${date_tag}.tsv

    # Convert the local Terra table tsv into a newline json
    echo "Converting the local Terra table tsv into a newline json" >> ${output_dir}/automation_logs/dashboard-${date_tag}.log
    python3 /scripts/tsv_to_newline_json.py ${gisaid_dir}/full_${terra_table_root_entity}_terra_table_${date_tag}.tsv ${gisaid_dir}/${terra_table_root_entity}_${date_tag}

    # Push newline json to the dashboard GCP bucket and backup folder
    echo "Pushing newline json to the dashboard GCP bucket and backup folder" >> ${output_dir}/automation_logs/dashboard-${date_tag}.log
    gsutil cp ${gisaid_dir}/${terra_table_root_entity}_${date_tag}.json ${dashboard_gcp_uri}/${terra_table_root_entity}.json
    gsutil cp ${gisaid_dir}/${terra_table_root_entity}_${date_tag}.json ${output_dir}/backup_jsons/

    # Load newline json to Big Query 
    echo "Loading newline json to Big Query" >> ${output_dir}/automation_logs/dashboard-${date_tag}.log
    bq load --ignore_unknown_values=true --replace=true --source_format=NEWLINE_DELIMITED_JSON ${big_query_table_name} ${dashboard_gcp_uri}/${terra_table_root_entity}.json ${dashboard_schema}

  fi

else
  # display error message if the file is not a GISAID file
  echo "The file was not recognized as a GISAID auspice tar file."
fi
