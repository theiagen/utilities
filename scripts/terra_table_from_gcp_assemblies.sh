#!/bin/bash

HELP="
Will create a terra data table with samplenames and gcp pointers to assemblies based on the assembly files present in a  gcp_uri and import this table to a Terra workspace. Assumes assembly files are in the {samplename}.fasta format within the specified gcp_uri.

For the Terra table to properly import into the user-defined workspace, gcloud authentication is required. 

Five positional arguments required:

terra_table_from_gcp_assemblies.sh {gcp_uri} {terra_project} {terra_workspace} {root_entity} {output_dir}

*NOTES on positional arguments: 
- gcp_uri must end in foward slash, e.g. \"gs://my_gcp_bucket/\"
- root_entity should not contain the \"entity:\" prefix nor the \"_id\" suffix
"

# If the user invokes the script with -h or any command line arguments, print some help.
if [ "$#" == 0 ] || [ "$1" == "-h" ] ; then
	echo "$HELP"
  exit 0
fi


# User-defined inputs
gcp_uri=$1
terra_project=$2
terra_workspace=$3
root_entity=$4
output_dir=$5

# Capture date to tag output file
date_tag=$(date +"%Y-%m-%d-%Hh-%Mm-%Ss")

# Capture samplenames from existing assembleis in given gcp_uri
assembly_files=$(gsutil ls ${gcp_uri}*.fasta | awk -F'/' '{ print $NF }')

# Create Terra table with gcp pointers
echo -e "entity:${root_entity}_id\tassembly_fasta" > ${output_dir}/assembly_terra_table_${date_tag}.tsv

for assembly in $assembly_files; do
  if [[ "*${assembly}*" =~ "_" ]]; then 
    samplename=$(echo ${assembly} | awk -F'_' '{ print $1 }')
  else
    samplename=$(echo ${assembly} | awk -F'.fasta' '{ print $1 }')
  fi
  echo -e "${samplename}\t${gcp_uri}${assembly}" >> ${output_dir}/assembly_terra_table_${date_tag}.tsv
done

# remove duplicates from tsv if samplename not unique
awk '!a[$1]++' ${output_dir}/assembly_terra_table_${date_tag}.tsv > temp.tsv && mv temp.tsv ${output_dir}/assembly_terra_table_${date_tag}.tsv

# Import Terra table to sepcified terra_workspace
docker run --rm -it -v "$HOME"/.config:/.config -v $PWD:/data broadinstitute/terra-tools:tqdm bash -c "cd data; python3 /scripts/import_large_tsv/import_large_tsv.py --project ${terra_project} --workspace ${terra_workspace} --tsv ${output_dir}/assembly_terra_table_${date_tag}.tsv"
