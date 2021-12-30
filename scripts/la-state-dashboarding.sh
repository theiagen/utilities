#!/bin/bash


# Set variables
dashboarding_gcp_uri="gs://la-state-dashboarding/"
dashboarding_newline_json="gisaid_louisiana_data.json"
dashboarding_schema="/home/kevin_libuit/la_state_dashboarding/schema_LA_v1.json"

# Set user-defined parameters
monitorring_dir=''
outout_dir=''
gcp_uri=''
terra_project=''
terra_workspace=''
bq_load_schema=''

HELP="
  Usage: ...
"
 
while getopts ':m:o:g:p:w:s:' flag; do
  case "${flag}" in
    m) monitorring_dir="${OPTARG}" ;;
    o) output_dir="${OPTARG}" ;;
    g) gcp_uri="${OPTARG}" ;;
    p) terra_project="${OPTARG}" ;;
    w) terra_workspace="${OPTARG}" ;;
    s) bq_load_schema="${OPTARG}" ;;
    :) echo "Missing option argument for -$OPTARG" ;;
    ?) echo "${HELP}" 
       exit 0;;
  esac
done

if [[ -z $monitorring_dir || -z $output_dir || -z $gcp_uri || -z $terra_project || -z $terra_workspace || -z $bq_load_schema ]]; then 
  echo "One or more required inputs not defined. $HELP"
  exit 0
fi

echo "did not exit"
echo "monitorring_dir: ${monitorring_dir}, output_dir: ${output_dir}, gcp_uri: ${gcp_uri}, terra_project: ${terra_project}, terra_workspace: ${terra_workspace}, bq_load_schema: ${bq_load_schema}"

# Create output sub directories
mkdir -p ${output_dir}/{automation_logs,gisaid_files}

# Start monitorring specified directory for the creation of new assembly_files
inotifywait -m ${monitorring_dir} -e create | while read dir action file; do
    echo "The file '$file' appeared in directory '$dir' via '$action'" >> ~{output_dir}/automation_logs/inotifywait.log
    
    # if the created file is a gisaid_auspice input file, integrate into Terra and BQ
    if [[ "$file" == *"gisaid_auspice_input"* ]]; then 
      date_tag=$(date +"%Y-%m-%d")
      gisaid_dir="~{output_dir}/gisaid_files/${date_tag}/"
      
      echo "
      # decompress tar ball
      mkdir ${gisaid_dir}
      tar -xf $file -C ~{output_dir}/gisaid_files/${date_tag}/
       
      # Create individual fasta files from GISAID multifasta
      gisaid_multifasta_parser.py ${gisaid_dir}/*.sequences.fasta  ${gisaid_dir}
       
      # Deposit individual fasta files into GCP bucket
      gsutil cp ${gisaid_dir}/individual_gisaid_assemblies_${date_tag}/*.fasta ${gcp_uri}uploads/gisaid_individual_assemblies_${date_tag}/
       
      # Create and Import Terra Data table containing GCP pointers to deposited assemblies
      terra_table_from_gcp_assemblies.sh ${gcp_uri}uploads/gisaid_individual_assemblies_${date_tag}/ ${terra_project} ${{terra_workspace} ${gisaid_dir} \".fasta\"
       
      # Capture, reformat, andgisaid prune GISAID metadata 
      gisaid_metadata_cleanser.py ${gisaid_dir}/*.metadata.tsv ${gisaid_dir}/gisaid_metadata_${date_tag}.tsv
       
      # Import of formatted data table into Terra 
      docker run --rm -it -v \"$HOME\"/.config:/.config -v $PWD:/data broadinstitute/terra-tools:tqdm bash -c \"cd data; python3 /scripts/import_large_tsv/import_large_tsv.py --project ${terra_project} --workspace ${terra_workspace} --tsv ${gisaid_dir}/gisaid_metadata_${date_tag}.tsv\"
        
       # Convert local tsv to newline json
       tsv_to_newline_json.py ${gisaid_dir}/gisaid_metadata_${date_tag}.tsv ${gisaid_dir}/gisaid_metadata_${date_tag}.json
        
       # Push to GCP bucket
       gsutil cp  ${gisaid_dir}/gisaid_metadata_${date_tag}.json ${dashboarding_gcp_uri}${dashboarding_newline_json}
       gsutil cp  ${gisaid_dir}/gisaid_metadata_${date_tag}.json ${dashboarding_gcp_uri}backup/
        
       # Load newline-json to BQ
       bq load --ignore_unknown_values=true --replace=true --source_format=NEWLINE_DELIMITED_JSON sars_cov_2_dashboard.la_state_gisaid_specimens ${dashboarding_gcp_uri}${dashboarding_newline_json} ${dashboarding_schema}
       " >> ${output_dir}/automation_logs/automation_executables.txt
    fi

done
     
     
     
     
     
     
     
     
     
     
     
     
     

