#!/bin/bash

HELP="
Will create a terra data table with samplenames and gcp pointers to assemblies based on the assembly files present in a  gcp_uri and import this table to a Terra workspace. Assumes assembly files are in the {samplename}.fasta format within the specified gcp_uri.

For the Terra table to properly import into the user-defined workspace, gcloud authentication is required. 

Five positional arguments required:

terra_table_from_gcp_assemblies.sh {gcp_uri} {terra_project} {terra_workspace} {root_entity} {output_dir} {alt_delimiter}
- {gcp_uri}: gcp_uri for the bucket containing assembly files; gcp_uri must end in foward slash, e.g. \"gs://my_gcp_bucket/\"
- {terra_project}: terra project that will host the imported terra data table
- {terra_workspace}: terra workspace taht will host the imported terra data table
- {root_entity}: name of terra table root entity; root_entity should not contain the \"entity:\" prefix nor the \"_id\" suffix
- {output_dir}: path to local directory to save a copy of the terra data table 
- {alt_delimiter}: filename delimiter to pull sample name from file; if no alt_delimiter is provided, an underscore (\"_\") will be utilized

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
alt_delimiter=$6

if [ -z $alt_delimiter ]; then
	alt_delimiter="_"
fi

# Capture date to tag output file
date_tag=$(date +"%Y-%m-%d-%Hh-%Mm-%Ss")

# Capture samplenames from existing assembleis in given gcp_uri
assembly_files=$(gsutil ls ${gcp_uri}*.fasta | awk -F'/' '{ print $NF }')

# Create Terra table with gcp pointers
echo -e "entity:${root_entity}_id\tassembly_fasta" > ${output_dir}/assembly_terra_table_${date_tag}.tsv

for assembly in $assembly_files; do
  # capture samplename from assembly filename
  samplename=$(echo ${assembly} | awk -F"${alt_delimiter}|.fasta" '{ print $1 }')
  # write samplename and gcp pointer to terra data table
  echo -e "${samplename}\t${gcp_uri}${assembly}" >> ${output_dir}/assembly_terra_table_${date_tag}.tsv
done

# remove duplicates from tsv if samplename not unique
awk '!a[$1]++' ${output_dir}/assembly_terra_table_${date_tag}.tsv > temp.tsv && mv temp.tsv ${output_dir}/assembly_terra_table_${date_tag}.tsv

# Import Terra table to sepcified terra_workspace
docker run --rm -it -v "$HOME"/.config:/.config -v $PWD:/data broadinstitute/terra-tools:tqdm bash -c "cd data; python3 /scripts/import_large_tsv/import_large_tsv.py --project ${terra_project} --workspace ${terra_workspace} --tsv ${output_dir}/assembly_terra_table_${date_tag}.tsv"
