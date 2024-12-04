import requests
import json
import re
import argparse
import csv

def fetch_wdl_from_github():
    url = "https://raw.githubusercontent.com/theiagen/public_health_bioinformatics/main/workflows/utilities/wf_organism_parameters.wdl"
    response = requests.get(url)
    response.raise_for_status()
    return response.text

def parse_wdl_variables(wdl_content):
    ds_name_pattern = r'String\s+(\w+_nextclade_ds_name)\s*=\s*"([^"]+)"'
    ds_tag_pattern = r'String\s+(\w+_nextclade_ds_tag)\s*=\s*"([^"]+)"'
    
    ds_names = dict(re.findall(ds_name_pattern, wdl_content))
    ds_tags = dict(re.findall(ds_tag_pattern, wdl_content))
    
    return ds_names, ds_tags

def get_latest_version(dataset_info):
    if not dataset_info.get('versions'):
        return None
    return sorted(dataset_info['versions'], key=lambda x: x['tag'], reverse=True)[0]['tag']

def main():
    parser = argparse.ArgumentParser(description='Compare Nextclade dataset versions between JSON and WDL')
    parser.add_argument('json_file', help='Path to Nextclade datasets JSON file')
    parser.add_argument('--output', help='Output CSV file', default='nextclade_versions.csv')
    args = parser.parse_args()

    with open(args.json_file, 'r') as f:
        json_data = json.load(f)

    try:
        wdl_content = fetch_wdl_from_github()
    except requests.RequestException as e:
        print(f"Error fetching WDL file: {e}")
        return

    dataset_lookup = {dataset['path']: dataset for dataset in json_data}
    ds_names, ds_tags = parse_wdl_variables(wdl_content)

    with open(args.output, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['Variable Name', 'Dataset Name', 'Latest Tag', 'Current Tag', 'Needs Update'])

        for var_name, ds_name in ds_names.items():
            if ds_name == "NA":
                continue
                
            current_tag = ds_tags.get(var_name.replace('_name', '_tag'))
            dataset_info = dataset_lookup.get(ds_name)
            
            if dataset_info:
                latest_tag = get_latest_version(dataset_info)
                needs_update = "Yes" if latest_tag != current_tag else "No"
                writer.writerow([var_name, ds_name, latest_tag, current_tag, needs_update])
            else:
                writer.writerow([var_name, ds_name, "Not found in JSON", current_tag, "Unknown"])

if __name__ == "__main__":
    main()