#!/usr/bin/env bash
#SBATCH --job-name=split_annotate
#SBATCH --output=%x_%A_%a.log
#SBATCH --error=%x_%A_%a.err
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=16G
#SBATCH --time=02:00:00
#SBATCH --partition=PARTITION_PLACEHOLDER
#SBATCH --account=ACCOUNT_PLACEHOLDER
#SBATCH --mail-type=FAIL
#SBATCH --mail-user=EMAIL_PLACEHOLDER
# =============================================================================
# Step 04: Per-sample split + SpliceAI annotation — SLURM array job
#
# Input:   ${DIR_MERGED}/clean_merged.vcf.gz        (step 01)
#          ${DIR_SPLICEAI_MERGED}/spliceai_all_chroms.vcf.gz  (step 03)
#          ${DIR_MERGED}/sample_list.txt
# Output:  ${DIR_SAMPLES}/${SAMPLE_SAFE}/${SAMPLE_SAFE}_spliceai.vcf.gz  (+.tbi)
#
# Substeps:
#   4a. Extract per-sample VCF from clean_merged (variant sites only)
#   4b. Annotate with SpliceAI INFO field via bcftools annotate
#
# Submit:  sbatch --array=1-N_SAMPLES 04_split_and_annotate.sh
#          (master_submit.sh handles this automatically)
# =============================================================================
set -euo pipefail

# Default CPU count when running outside SLURM (e.g. interactive test)
SLURM_CPUS_PER_TASK="${SLURM_CPUS_PER_TASK:-4}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/config.sh"

SAMPLE_LIST="${DIR_MERGED}/sample_list.txt"
[[ -f "${SAMPLE_LIST}" ]] || { echo "ERROR: sample_list.txt not found: ${SAMPLE_LIST}"; exit 1; }

mapfile -t SAMPLES < "${SAMPLE_LIST}"
IDX=$(( SLURM_ARRAY_TASK_ID - 1 ))
SAMPLE="${SAMPLES[$IDX]}"
SAMPLE_SAFE="${SAMPLE//\//_}"

LOG_FILE="${DIR_LOGS}/04_split_${SAMPLE_SAFE}.log"
SAMPLE_DIR="${DIR_SAMPLES}/${SAMPLE_SAFE}"
mkdir -p "${SAMPLE_DIR}" "${DIR_LOGS}" "${PIPELINE_TMPDIR}"
export TMPDIR="${PIPELINE_TMPDIR}"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "${LOG_FILE}"; }
log "========== STEP 04: Split + SpliceAI Annotate | ${SAMPLE} =========="

CLEAN_MERGED="${DIR_MERGED}/clean_merged.vcf.gz"
SPLICEAI_MERGED="${DIR_SPLICEAI_MERGED}/spliceai_all_chroms.vcf.gz"

[[ -f "${CLEAN_MERGED}" ]]     || { log "ERROR: clean_merged not found: ${CLEAN_MERGED}"; exit 1; }
[[ -f "${SPLICEAI_MERGED}" ]]  || { log "ERROR: spliceai_merged not found: ${SPLICEAI_MERGED}"; exit 1; }

# ── 4a: Extract sample — keep only variant sites ─────────────────────────────
SAMPLE_RAW="${SAMPLE_DIR}/${SAMPLE_SAFE}_raw.vcf.gz"
log "Extracting sample ${SAMPLE} (variant sites only)..."
${BCFTOOLS} view \
    --samples "${SAMPLE}" \
    --min-ac 1 \
    --threads "${SLURM_CPUS_PER_TASK}" \
    -Oz -o "${SAMPLE_RAW}" \
    "${CLEAN_MERGED}"
${TABIX} -p vcf "${SAMPLE_RAW}"
NVAR=$(${BCFTOOLS} stats "${SAMPLE_RAW}" | grep "^SN" | grep "number of records" | cut -f4)
log "Variants in ${SAMPLE}: ${NVAR}"

# ── 4b: Annotate per-sample VCF with SpliceAI INFO ───────────────────────────
ANNOT_HDR="${SAMPLE_DIR}/spliceai_header.hdr"
printf '##INFO=<ID=SpliceAI,Number=.,Type=String,Description="SpliceAIv1.3: ALLELE|SYMBOL|DS_AG|DS_AL|DS_DG|DS_DL">\n' \
    > "${ANNOT_HDR}"

SAMPLE_ANNOTATED="${SAMPLE_DIR}/${SAMPLE_SAFE}_spliceai.vcf.gz"
log "Annotating with SpliceAI INFO field..."
${BCFTOOLS} annotate \
    --annotations "${SPLICEAI_MERGED}" \
    --columns "INFO/SpliceAI" \
    --header-lines "${ANNOT_HDR}" \
    --threads "${SLURM_CPUS_PER_TASK}" \
    -Oz -o "${SAMPLE_ANNOTATED}" \
    "${SAMPLE_RAW}"
${TABIX} -p vcf "${SAMPLE_ANNOTATED}"

SPLICEAI_COUNT=$(${BCFTOOLS} query -f "%INFO/SpliceAI\n" "${SAMPLE_ANNOTATED}" \
    | grep -vc "^\.$" || true)
log "Variants with SpliceAI annotation: ${SPLICEAI_COUNT} / ${NVAR}"

log "========== STEP 04 COMPLETE | ${SAMPLE} =========="
log "Output: ${SAMPLE_ANNOTATED}"
