#!/usr/bin/env bash
# =============================================================================
# config.sh - Central configuration for the VCF Annotation Pipeline
#
# PER-PROJECT SETUP — only edit the two lines below:
#   PROJECT_DIR : root for this project (vcf/ inputs + results/ outputs live here)
#   SCRIPT_DIR  : directory containing this config.sh and all pipeline scripts
#
# VCF inputs are auto-discovered from ${PROJECT_DIR}/vcf/*.vcf.gz
# Run setup_project.sh once to move raw VCFs into place before submitting.
# =============================================================================

# =============================================================================
# !! EDIT THESE TWO LINES PER PROJECT !!
# =============================================================================
PROJECT_DIR="/home/data/BATCH/bt3/"
SCRIPT_DIR="/path/to/scripts"

# =============================================================================
# Derived directories — all flow from PROJECT_DIR; do NOT edit
# =============================================================================
VCF_DIR="${PROJECT_DIR}/vcf"
BASE_OUTDIR="${PROJECT_DIR}/results"

DIR_MERGED="${BASE_OUTDIR}/01_merged_normalized"
DIR_SPLICEAI_CHROMS="${BASE_OUTDIR}/02_spliceai_chroms"
DIR_SPLICEAI_MERGED="${BASE_OUTDIR}/03_spliceai_merged"
DIR_SAMPLES="${BASE_OUTDIR}/04_samples_clean"
DIR_ANNOVAR="${BASE_OUTDIR}/05_annovar"
DIR_FINAL="${BASE_OUTDIR}/06_final_annotated"
DIR_LOGS="${BASE_OUTDIR}/logs"
PIPELINE_TMPDIR="${BASE_OUTDIR}/tmp"

# =============================================================================
# Auto-discover VCFs from VCF_DIR
# Files must be bgzipped (.vcf.gz) and tabix-indexed (.vcf.gz.tbi).
# Run setup_project.sh first to stage and auto-index files from a source dir.
# =============================================================================
if [[ ! -d "${VCF_DIR}" ]]; then
    echo "ERROR: VCF_DIR does not exist: ${VCF_DIR}" >&2
    echo "       Run setup_project.sh to stage your VCFs first." >&2
    exit 1
fi

mapfile -t VCF_LIST < <(find "${VCF_DIR}" -maxdepth 1 -name "*.vcf.gz" | sort)

if [[ "${#VCF_LIST[@]}" -eq 0 ]]; then
    echo "ERROR: No *.vcf.gz files found in ${VCF_DIR}" >&2
    echo "       Run setup_project.sh to stage your VCFs first." >&2
    exit 1
fi

N_SAMPLES=${#VCF_LIST[@]}

# =============================================================================
# Reference & Annotation Databases — shared; do NOT change per project
# =============================================================================
GENOME_FASTA="/path/to/"
ANNOVAR_DB="/path/to/Annovardb/humandb"
ANNOVAR_BUILD="hg38"

# 13 protocols / 13 operations / 13 argument slots (must stay in sync)
ANNOVAR_PROTOCOL="refGene,cytoBand,genomicSuperDups,exac03,gnomad41_exome,gnomad41_genome,avsnp151,dbnsfp47a,dbscsnv11,mcap,clinvar_20250721,revel,gene4denovo201907"
ANNOVAR_OPERATION="g,r,r,f,f,f,f,f,f,f,f,f,f"
ANNOVAR_ARGUMENT="'-splicing 5',,,,,,,,,,,,"   # 12 trailing commas = 13 slots

SPLICEAI_GENOME="grch38"
SPLICEAI_DIST=500
OMIM_CSV="path/to/OMIM_Summary_File.csv"

# Chromosomes for SpliceAI array job (24 total)
declare -a CHROMOSOMES=(
    chr1 chr2 chr3 chr4 chr5 chr6 chr7 chr8 chr9 chr10
    chr11 chr12 chr13 chr14 chr15 chr16 chr17 chr18 chr19 chr20
    chr21 chr22 chrX chrY
)
N_CHROMS=${#CHROMOSOMES[@]}

# =============================================================================
# Singularity SIF Images — shared; do NOT change per project
# Note: bcftool_1.23.sif has no trailing 's' — as per directory listing
# =============================================================================
SIF_DIR="/path/to/Docker_Images"
SIF_BCFTOOLS="${SIF_DIR}/bcftool_1.23.sif"
SIF_SPLICEAI="${SIF_DIR}/spliceai.sif"
SIF_ANNOVAR="${SIF_DIR}/annovar_2020Jun08.sif"
SIF_PYTHON="${SIF_DIR}/python_3.14.2.sif"

# Path to table_annovar.pl INSIDE the annovar container
# Verify with: singularity exec ${SIF_ANNOVAR} find / -name table_annovar.pl 2>/dev/null
ANNOVAR_BIN_IN_CONTAINER="/home/TOOLS/tools/annovar/current/bin"

# =============================================================================
# Singularity bind mounts
# Every host path a container reads or writes must appear here.
# All paths are bound at the same location inside the container.
# =============================================================================
GENOME_DIR="$(dirname "${GENOME_FASTA}")"
# Covers: input VCFs, reference genome (+.fai/.dict), ANNOVAR DB,
#         all pipeline outputs, and scratch TMPDIR
BIND_MOUNTS="${VCF_DIR},${GENOME_DIR},${ANNOVAR_DB},${BASE_OUTDIR},${PIPELINE_TMPDIR}"

# =============================================================================
# Tool commands — used verbatim in ALL pipeline scripts
# --no-home  : prevents host ~/.bashrc / conda environments leaking in
# --cleanenv : clears host env vars (avoids Python/Perl version conflicts)
# =============================================================================
SING_BASE="singularity exec --no-home --cleanenv --bind ${BIND_MOUNTS} --env TMPDIR=${PIPELINE_TMPDIR}"

BCFTOOLS="${SING_BASE} ${SIF_BCFTOOLS} bcftools"
TABIX="${SING_BASE}    ${SIF_BCFTOOLS} tabix"
BGZIP="${SING_BASE}    ${SIF_BCFTOOLS} bgzip"

SPLICEAI_BIN="${SING_BASE} ${SIF_SPLICEAI} spliceai"

# ANNOVAR: do NOT add 'perl' prefix — already invoked via perl inside wrapper
TABLE_ANNOVAR="${SING_BASE} ${SIF_ANNOVAR} perl ${ANNOVAR_BIN_IN_CONTAINER}/table_annovar.pl"

# Python utility script dir is bound separately at runtime in 06_final_output.sh
PYTHON3="${SING_BASE} ${SIF_PYTHON} python3"

# =============================================================================
# SLURM Defaults — patched into step scripts by master_submit.sh
# =============================================================================
SLURM_PARTITION="bigJo"
SLURM_ACCOUNT="deepneo"
SLURM_EMAIL="deepakbharti@rcsi.com"
SLURM_EMAIL_TYPE="FAIL"
