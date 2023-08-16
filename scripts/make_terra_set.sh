#!/bin/bash

gcp_uri=$1
terra_project=$2
terra_workspace=$3
root_entity=$4
output_dir=$5
alt_delimiter=$6

date_tag=$7
set_name=${date_tag}-set

# set default for $alt_delimiter in case user does not specify one
if [ -z $alt_delimiter ]; then
	alt_delimiter="_"
fi

assembly_files=$(gsutil ls ${gcp_uri}/*.fasta | awk -F'/' '{ print $NF }')

# make set table header
echo -e "membership:${root_entity}_set_id\t${root_entity}" > ${output_dir}/${set_name}.tsv

for assembly in $assembly_files; do
  # capture samplename from assembly filename
  samplename=$(echo ${assembly} | awk -F "${alt_delimiter}|.fasta" '{ print $1 }')
  # write samplename to the set
  echo -e "${set_name}\t${samplename}" >> ${output_dir}/${set_name}.tsv
done

# remove duplicates from tsv if samplename not unique
awk '!a[$1]++' ${output_dir}/${set_name}.tsv > temp.tsv && mv temp.tsv ${output_dir}/${set_name}.tsv

# Import Terra table to sepcified terra_workspace
python3 /scripts/import_large_tsv/import_large_tsv.py --project ${terra_project} --workspace ${terra_workspace} --tsv ${output_dir}/${set_name}.tsv
