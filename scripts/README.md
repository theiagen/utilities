### move-se-reads.sh

A shell script for separating a directory that contains both single end and paired end fastq files.

#### requirements
 - bash
 - a single directory that contains BOTH single-end and paired-end ILMN fastq.gz files
 - fastqs must end in standard ILMN file endings: `_L001_R1_001.fastq.gz`, `_L001_R2_001.fastq.gz`, `_L002_R1_001.fastq.gz`, `_L002_R2_001.fastq.gz`

#### usage
```bash
# first argument is TARGET_DIR (input dir with .fastq.gz files)
# second argument is DESTINATION_DIR (output dir, will be created if it doesn't already exist)

# move SE reads from PWD into single-end-dir, store STDOUT/STDERR into log file
$ move-SE-reads.sh . single-end-dir/ >move-SE-reads.log 2>&1
```

### concatenate-across-lanes.sh

A shell script for concatenating samples sequenced on ILMN machines across multiple lanes (L001, L002, L003, L004), e.g. on a NextSeq, HiSeq, or NovaSeq. This script concatenates based on the filename prefix before the standard ILMN filename endings, e.g. `<ID>_L001_R1_001.fastq.gz`. It sets the prefix as a variable, `$ID`, and concatenates based on this `$ID`.

It also allow an option for a "dry run", where it simply `echo`'s the concatenation commands, but doesn't actually run them. To enable this option, add a second argument `dry` when calling the script.

#### requirements
  - bash
  - fastqs must end in standard ILMN file endings: `_L001_R1_001.fastq.gz`, `_L001_R2_001.fastq.gz`, `_L002_R1_001.fastq.gz`, `_L002_R2_001.fastq.gz`
  - target directory must ONLY contain paired-end fastqs, single-end files MUST be removed prior to running this script. See `move-SE-reads.sh` above
#### usage
```bash
# dry run, don't actually concatenate
$ concatenate-across-lanes.sh . dry 

# actually concatenate reads in PWD; save a log file with the STDOUT/STDERR
$ concatenate-across-lanes.sh . >concatenate-across-lanes.log 2>&1
```

### gisaid_metadata_cleanser.py

This python script will read in a tsv of sequence metadata, perform some reformatting and data sanitization, and then produce a tsv that can be uploaded to Terra.

#### requirements
Positional inputs required:
 - tsv_meta_file (assumes GISAID-generated tsv)
 - out_file (output file name)
 - table_name (the name of the terra table; do not include entity: or _id)
 
Optional input parameters:
 - `--puertorico` which performs Puerto Rico-specific actions, like removing pango_lineage from the metadata and all samples with PR-CVL in their name

#### usage
```bash
$ python3 gisaid_metadata_cleanser.py <tsv_meta_file> <out_file> <table_name> <optional_parameters>
```

### gisaid_multifasta_parser.py

This python script will parse the mutlifasta file provided in the gisaid auspice tarball 

#### requirements
Two positional inputs required:
 - gisaid_multifasta_file (the multifasta file from the auspice tarball)
 - output_dir (the location of the output directory

#### usage
```bash
$ python3 gisaid_multifasta_parser.py <gisaid_multifasta> <output_dir> 
```


### terra_table_from_gcp_assemblies.sh

This shell script will create a Terra data table with sample names and GCP pointers to assemblies, and then import it to a Terra workspace. 

#### requirements
Five positional arguments required:
 - gcp_uri : the gcp_uri for the bucket containing the assembly files
 - terra_project : the terra projet that will host incoming terra table
 - terra_workspace : the terra workspace that will host incoming terra table
 - root_entity : the name of the terra table root entity; do not include entity: or _id
 - output_dir : path to local directory where a copy of the terra table will be saved

Two optional arguments:
 - alt_delimiter : file delimiter to pull sample name from file, an underscore is the default
 - terra_upload_set : the name of the set applied to the data; by default the date is used

#### usage
```bash
$ ./terra_table_from_gcp_assemblies.sh <gcp_uri> <terra_project> <terra_workspace> <root_entity> <output_dir> <alt_delimiter> <terra_upload_set>
```

### tsv_to_newline_json.py

This python script converts a tsv file into a newline json. 

#### requirements
Two positional inputs required:
 - tsv_file : the input tsv file
 - output_name : the name of the ouptut file (do not include .json)

#### usage
```bash
$ python3 tsv_to_newline_json.py <tsv_file> <output_name>
```

### standard-dashboard.sh

This shell script performs all of the functions necessary to transform a given GISAID-generated auspice tarball into a Big Query upload.

#### usage
```bash 
# read the help message
$ ./standard-dashboard.sh -h
```

