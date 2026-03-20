# HPC VCF Annotation Pipeline (SpliceAI + Annovar)

## Overview

This pipeline is designed for High Performance Computing (HPC) environments using **Singularity containers**. It processes VCF files (≥ v4.1) through a multi-stage annotation workflow including normalization, merging, SpliceAI scoring, and Annovar annotation.

### Pipeline Features
- Works with VCF files (≥ v4.1)
- Normalizes input VCFs
- Merges multiple VCF files
- Runs chromosome-wise SpliceAI analysis
- Merges SpliceAI outputs
- Performs Annovar annotation
- Adds SpliceAI scores to Annovar results
- Designed for HPC batch processing with Singularity containers

---

## Requirements

- HPC environment with Singularity installed
- Pre-configured Singularity containers:
  - `annovar`
  - `bcftools`
  - `spliceAI`
  - `python`
- Input data: VCF files (version ≥ 4.1)

All container versions must match those defined in the pipeline configuration file.

---

## Pipeline Workflow

1. Stage batch VCF files into a project directory  
2. Configure project settings  
3. Preview job submission  
4. Submit pipeline to HPC scheduler  

---

## Setup Instructions

### 1. Create a New Project Directory

Stage your input VCF files into a new project workspace:

```bash
bash /path/to/pipeline/setup_project.sh \
    --project-dir /home/data/proj/wes/batch \
    --vcf-source  /home/data/incoming/batch_vcfs
```
### 2. Configure Pipeline

Edit config.sh:
```bash
PROJECT_DIR="/home/data/wes/batch"
SCRIPT_DIR="/path/to/pipeline"
```
Important:\n
PROJECT_DIR = working directory for the project\n
SCRIPT_DIR = pipeline installation path (do not change frequently)

### 3. Dry Run
```bash /path/to/pipeline/master_submit.sh --dry-run```

### 4. submit job
```bash /path/to/pipeline/master_submit.sh```

### 5. Outputs
1. Outputs are stored in the project directory:
2. Normalized VCF files
3. Merged cohort VCF
4. SpliceAI chromosome-level results
5. Combined SpliceAI dataset
6. Annovar annotations
7. Final annotated dataset with SpliceAI scores

# Troubleshooting
## Missing containers
check Singularity paths in configuration.
## VCF errors
Ensure proper VCF format (≥ v4.1).
## Job failures
Check HPC scheduler logs and permissions.
## SpliceAI issues
Ensure consistent chromosome naming (chr1 vs 1).

# Author
Deepak Bharti\n
Clinical Bioinformatician\n
RCSI, DUBLIN
