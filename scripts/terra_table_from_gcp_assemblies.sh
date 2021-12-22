#!/bin/bash

HELP="
Will create a terra data table with samplenames and gcp pointers to assemblies based on the assembly files present in a  gcp_uri and import this table to a Terra workspace. Assumes assembly files are in the {samplename}.fasta format within the specified gcp_uri.

For the Terra table to properly import into the user-defined workspace, gcloud authentication is required. 

Five positional arguments required:

$ terra_table_from_gcp_assemblies.sh {gcp_uri} {terra_project} {terra_workspace} {root_entity} {output_dir}

*NOTES on positional arguments: 
- gcp_uri must end in foward slash, e.g. \"gs://my_gcp_bucket/\"
- root_entity should not contain the \"entity:\" prefix nor the \"_id\" suffix


# User-defined inputs
gcp_uri=$1
terra_project=$2
terra_workspace=$3
root_entity=$4
output_dir=$5

# Capture date to tag output file
date_tag=$(date +"%Y-%m-%d-%Hh-%Mm-%Ss")

# Capture samplenames from existing assembleis in given gcp_uri
samplenames=$(gsutil ls ${gcp_uri}*.fasta | awk -F'/' '{ print $NF }' | awk -F'.fasta' '{ print $1 }')

# Create Terra table with gcp pointers
echo -e "entity:${root_entity}_id\tconensus_fasta" > ${output_dir}/assembly_terra_table_${date_tag}.tsv

for sample in $samplenames; do
  echo -e "${sample}\t${gcp_uri}${sample}.fasta" >> ${output_dir}/assembly_terra_table_${date_tag}.tsv
done

# Import Terra table to sepcified terra_workspace
docker run --rm -it -v "$HOME"/.config:/.config -v $PWD:/data broadinstitute/terra-tools:tqdm bash -c "cd data; python3 /scripts/import_large_tsv/import_large_tsv.py --project ${terra_project} --workspace ${terra_workspace} --tsv ${output_dir}/assembly_terra_table_${date_tag}.tsv"
