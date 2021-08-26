### concatenate-across-lanes.sh

A shell script for concatenating samples sequenced on ILMN machines across multiple lanes (L001, L002, L003, L004), e.g. on a NextSeq or NovaSeq.

#### requirements
  - bash
  - fastqs must end in standard ILMN file endings: `_L001_R1_001.fastq.gz`, `_L001_R2_001.fastq.gz`, `_L002_R1_001.fastq.gz`, `_L002_R2_001.fastq.gz`
  - target directory must ONLY contain paired-end fastqs, single-end files MUST be removed prior to running this script. See `move-SE-reads.sh` below
#### usage
```bash

```

### move-se-reads.sh

A shell script for separating a directory that contains both single end and paired end fastq files.

#### requirements
 - bash
 - fastqs must end in standard ILMN file endings: `_L001_R1_001.fastq.gz`, `_L001_R2_001.fastq.gz`, `_L002_R1_001.fastq.gz`, `_L002_R2_001.fastq.gz`

#### usage
```bash

```
