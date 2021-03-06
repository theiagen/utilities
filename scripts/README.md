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

