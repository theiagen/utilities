#!/bin/bash

# Set variables
dashboarding_gcp_uri="gs://la-state-dashboarding/"
dashboarding_newline_json="gisaid_louisiana_data.json"
dashboarding_schema="/home/kevin_libuit/la_state_dashboarding/schema_LA_v1.json"
terra_table_root_entity="gisaid_louisiana_data"
monitorring_dir="/home/kevin_libuit/la_state_dashboarding/terra_gisaid_collection"
output_dir="/home/kevin_libuit/la_state_dashboarding/"
gcp_uri="gs://fc-dc36d985-ee37-436c-8cc8-8314067696b2/uploads/GISAID/"
terra_project="cdc-terra-la-phl"
terra_workspace="CDC-COVID-LA-Dashboard"
bq_load_schema="/home/kevin_libuit/la_state_dashboarding/schema_LA_v1.json"

HELP="

  Usage: ...
"
 
if [[ -z $monitorring_dir || -z $output_dir || -z $gcp_uri || -z $terra_project || -z $terra_workspace || -z $bq_load_schema ]]; then 
  echo "One or more required inputs not defined. $HELP"
  exit 0
fi

echo "monitorring_dir: ${monitorring_dir}, output_dir: ${output_dir}, gcp_uri: ${gcp_uri}, terra_project: ${terra_project}, terra_workspace: ${terra_workspace}, bq_load_schema: ${bq_load_schema}"

# Create output sub directories
mkdir -p ${output_dir}/{automation_logs,gisaid_files}
date_tag=$(date +"%Y-%m-%d-%Hh-%Mm-%Ss")
echo "LA State Dashboarding Automated System initiated at ${date_tag}" | tee ${output_dir}/automation_logs/inotifywait.log

# Start monitorring specified directory for the creation of new assembly_files
inotifywait -m ${monitorring_dir} -e create -e moved_to | while read dir action file; do
    echo "The file '$file' appeared in directory '$dir' via '$action'" >> ${output_dir}/automation_logs/inotifywait.log
    
    # if the created file is a gisaid_auspice input file, integrate into Terra and BQ
    if [[ "$file" == "gisaid_auspice_input"*"tar" ]]; then 
      echo "New gisaid file identified: $file" >> ${output_dir}/automation_logs/inotifywait.log
      date_tag=$(date +"%Y-%m-%d")
      gisaid_dir="${output_dir}/gisaid_files/${date_tag}/"
      file="${monitorring_dir}/${file}"
      
      SCRIPTS="
      # decompress tar ball 
      \n
      mkdir ${gisaid_dir}
      \n
      tar -xf $file -C ${output_dir}/gisaid_files/${date_tag}/
      \n
      \n
      # Create individual fasta files from GISAID multifasta
      \n
      gisaid_multifasta_parser.py ${gisaid_dir}/*.sequences.fasta  ${gisaid_dir}
      \n
      \n
      # Deposit individual fasta files into GCP bucket
      \n
      gsutil cp ${gisaid_dir}/individual_gisaid_assemblies_${date_tag}/*.fasta ${gcp_uri}uploads/gisaid_individual_assemblies_${date_tag}/
      \n
      \n
      # Create and Import Terra Data table containing GCP pointers to deposited assemblies
      \n
      terra_table_from_gcp_assemblies.sh ${gcp_uri}uploads/gisaid_individual_assemblies_${date_tag}/ ${terra_project} ${terra_workspace} ${terra_table_root_entity} ${gisaid_dir} \".fasta\"
      \n
      \n
      # Capture, reformat, and prune GISAID metadata 
      \n
      gisaid_metadata_cleanser.py ${gisaid_dir}/*.metadata.tsv ${gisaid_dir}/gisaid_metadata_${date_tag}.tsv
      \n
      \n
      # Import of formatted data table into Terra 
      \n
      docker run --rm -it -v \"$HOME\"/.config:/.config -v ${gisaid_dir}:/data broadinstitute/terra-tools:tqdm bash -c \"cd data; python3 /scripts/import_large_tsv/import_large_tsv.py --project ${terra_project} --workspace ${terra_workspace} --tsv /data/gisaid_metadata_${date_tag}.tsv\"
      \n
      \n
      # Capture full terra data table as tsv 
      \n
      docker run --rm -it -v \"$HOME\"/.config:/.config -v ${gisaid_dir}:/data broadinstitute/terra-tools:tqdm bash -c \cd data; python3 /scripts/export_large_tsv/export_large_tsv.py --project cdc-terra-la-phl --workspace CDC-COVID-LA-Dashboard --entity_type gisaid_louisiana_data --tsv_filename full_louisiana_terra_table_${date}.tsv\"
      # Convert local tsv to newline json
      \n
      tsv_to_newline_json.py ${gisaid_dir}/full_louisiana_terra_table_${date}.tsv ${gisaid_dir}/gisaid_louisiana_data_${date_tag}
      \n
      \n 
      # Push to GCP bucket
      \n
      gsutil cp  ${gisaid_dir}/gisaid_louisiana_data_${date_tag}.json ${dashboarding_gcp_uri}gisaid_louisiana_data.json
      \n
      gsutil cp  ${gisaid_dir}/gisaid_metadata_${date_tag}.json ${dashboarding_gcp_uri}backup/
      \n  
      \n
      # Load newline-json to BQ
      \n
      bq load --ignore_unknown_values=true --replace=true --source_format=NEWLINE_DELIMITED_JSON sars_cov_2_dashboard.la_state_gisaid_specimens ${dashboarding_gcp_uri}gisaid_louisiana_data.json ${dashboarding_schema}
      " 
      echo -e $SCRIPTS >> ${output_dir}/automation_logs/automation_executables.txt
    fi

done
     
     
     
     
     
     
     
     
     
     
     
     
     

