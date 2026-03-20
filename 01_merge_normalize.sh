#!/usr/bin/env bash
#SBATCH --job-name=merge_normalize
#SBATCH --output=%x_%j.log
#SBATCH --error=%x_%j.err
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=32G
#SBATCH --time=04:00:00
#SBATCH --partition=PARTITION_PLACEHOLDER
#SBATCH --account=ACCOUNT_PLACEHOLDER
#SBATCH --mail-type=FAIL
#SBATCH --mail-user=EMAIL_PLACEHOLDER
# =============================================================================
# Step 01: Merge all per-sample VCFs → Normalize → Remove spanning deletions
#
# Input:   ${VCF_DIR}/*.vcf.gz  (auto-discovered via config.sh)
# Output:  ${DIR_MERGED}/clean_merged.vcf.gz  (+.tbi)
#          ${DIR_MERGED}/sample_list.txt
#
# Substeps:
#   1a. bcftools merge  → merged_raw.vcf.gz
#   1b. bcftools norm   → decompose multiallelic + left-align indels
#   1c. bcftools view   → remove spanning deletion alleles (ALT="*")
#   1d. Integrity check → sample count + bcftools stats
#   1e. Write sample_list.txt for downstream per-sample array jobs
# =============================================================================
set -euo pipefail

# Default CPU count when running outside SLURM (e.g. interactive test)
SLURM_CPUS_PER_TASK="${SLURM_CPUS_PER_TASK:-8}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/config.sh"

LOG_FILE="${DIR_LOGS}/01_merge_normalize.log"
mkdir -p "${DIR_MERGED}" "${DIR_LOGS}" "${PIPELINE_TMPDIR}"
export TMPDIR="${PIPELINE_TMPDIR}"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "${LOG_FILE}"; }

log "========== STEP 01: Merge + Normalize =========="
log "N_SAMPLES = ${N_SAMPLES}"
log "VCF_DIR   = ${VCF_DIR}"
log "Output    : ${DIR_MERGED}"

# ── Verify all inputs are present and indexed ─────────────────────────────────
log "Verifying input VCFs..."
for vcf in "${VCF_LIST[@]}"; do
    [[ -f "${vcf}" ]]       || { log "ERROR: Missing VCF: ${vcf}"; exit 1; }
    [[ -f "${vcf}.tbi" ]]   || { log "ERROR: Missing index: ${vcf}.tbi"; exit 1; }
    log "  OK: $(basename "${vcf}")"
done

# ── Step 1a: Merge all VCFs ───────────────────────────────────────────────────
MERGED_RAW="${DIR_MERGED}/merged_raw.vcf.gz"
log "Merging ${N_SAMPLES} VCFs → ${MERGED_RAW}"
${BCFTOOLS} merge \
    --merge all \
    --threads "${SLURM_CPUS_PER_TASK}" \
    --output-type z \
    --output "${MERGED_RAW}" \
    "${VCF_LIST[@]}"
${TABIX} -p vcf "${MERGED_RAW}"
log "Merge complete. Variants: $(${BCFTOOLS} stats "${MERGED_RAW}" | grep ^SN | grep 'number of records' | cut -f4)"

# ── Step 1b: Decompose multiallelic sites and left-align indels ───────────────
SPLIT_VCF="${DIR_MERGED}/merged_split.vcf.gz"
log "Normalizing: decompose multiallelic + left-align → ${SPLIT_VCF}"
${BCFTOOLS} norm \
    -f "${GENOME_FASTA}" \
    -m -any \
    --threads "${SLURM_CPUS_PER_TASK}" \
    -Oz -o "${SPLIT_VCF}" \
    "${MERGED_RAW}"
${TABIX} -p vcf "${SPLIT_VCF}"
log "After norm: $(${BCFTOOLS} stats "${SPLIT_VCF}" | grep ^SN | grep 'number of records' | cut -f4) records"

# ── Step 1c: Remove spanning deletion alleles (ALT="*") ──────────────────────
CLEAN_MERGED="${DIR_MERGED}/clean_merged.vcf.gz"
log "Removing spanning deletions (ALT=*) → ${CLEAN_MERGED}"
${BCFTOOLS} view \
    -e 'ALT="*"' \
    --threads "${SLURM_CPUS_PER_TASK}" \
    -Oz -o "${CLEAN_MERGED}" \
    "${SPLIT_VCF}"
${TABIX} -p vcf "${CLEAN_MERGED}"
log "After filter: $(${BCFTOOLS} stats "${CLEAN_MERGED}" | grep ^SN | grep 'number of records' | cut -f4) records"

# ── Step 1d: Integrity check ──────────────────────────────────────────────────
log "Running bcftools stats on clean_merged..."
${BCFTOOLS} stats "${CLEAN_MERGED}" > "${DIR_MERGED}/clean_merged_stats.txt" 2>&1 || true
SAMPLE_COUNT=$(${BCFTOOLS} query -l "${CLEAN_MERGED}" | wc -l)
log "Samples in clean_merged: ${SAMPLE_COUNT} (expected ${N_SAMPLES})"
[[ "${SAMPLE_COUNT}" -eq "${N_SAMPLES}" ]] || { log "ERROR: Sample count mismatch!"; exit 1; }

# ── Step 1e: Write sample list for downstream per-sample array jobs ───────────
SAMPLE_LIST="${DIR_MERGED}/sample_list.txt"
${BCFTOOLS} query -l "${CLEAN_MERGED}" > "${SAMPLE_LIST}"
log "Sample list written: ${SAMPLE_LIST}"
while IFS= read -r s; do log "  sample: ${s}"; done < "${SAMPLE_LIST}"

log "========== STEP 01 COMPLETE =========="
log "Output: ${CLEAN_MERGED}"
