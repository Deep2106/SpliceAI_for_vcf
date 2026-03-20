#!/usr/bin/env bash
#SBATCH --job-name=merge_spliceai
#SBATCH --output=%x_%j.log
#SBATCH --error=%x_%j.err
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=32G
#SBATCH --time=02:00:00
#SBATCH --partition=PARTITION_PLACEHOLDER
#SBATCH --account=ACCOUNT_PLACEHOLDER
#SBATCH --mail-type=FAIL
#SBATCH --mail-user=EMAIL_PLACEHOLDER
# =============================================================================
# Step 03: Concatenate per-chromosome SpliceAI VCFs into one merged VCF
#
# Input:   ${DIR_SPLICEAI_CHROMS}/spliceai_${CHROM}.vcf.gz  (all 24 chromosomes)
# Output:  ${DIR_SPLICEAI_MERGED}/spliceai_all_chroms.vcf.gz  (+.tbi)
#
# Aborts if any per-chromosome file is missing (step 02 must complete fully).
# Cross-checks variant count vs clean_merged as a sanity check.
# =============================================================================
set -euo pipefail

# Default CPU count when running outside SLURM (e.g. interactive test)
SLURM_CPUS_PER_TASK="${SLURM_CPUS_PER_TASK:-8}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/config.sh"

LOG_FILE="${DIR_LOGS}/03_merge_spliceai.log"
mkdir -p "${DIR_SPLICEAI_MERGED}" "${DIR_LOGS}" "${PIPELINE_TMPDIR}"
export TMPDIR="${PIPELINE_TMPDIR}"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "${LOG_FILE}"; }
log "========== STEP 03: Merge per-chromosome SpliceAI VCFs =========="

# ── Build ordered list of per-chrom SpliceAI VCFs ────────────────────────────
CHROM_VCFS=()
MISSING=0
for CHROM in "${CHROMOSOMES[@]}"; do
    VCF="${DIR_SPLICEAI_CHROMS}/spliceai_${CHROM}.vcf.gz"
    if [[ -f "${VCF}" && -f "${VCF}.tbi" ]]; then
        CHROM_VCFS+=("${VCF}")
        log "  Found: $(basename "${VCF}")"
    else
        log "  ERROR: Missing SpliceAI output for ${CHROM}: ${VCF}"
        MISSING=$(( MISSING + 1 ))
    fi
done

[[ "${MISSING}" -gt 0 ]] && {
    log "ABORT: ${MISSING} chromosome(s) missing. Check step 02 logs."
    exit 1
}

# ── Concatenate all chromosome VCFs ──────────────────────────────────────────
SPLICEAI_MERGED="${DIR_SPLICEAI_MERGED}/spliceai_all_chroms.vcf.gz"
log "Concatenating ${#CHROM_VCFS[@]} chromosome VCFs → ${SPLICEAI_MERGED}"
${BCFTOOLS} concat \
    --allow-overlaps \
    --threads "${SLURM_CPUS_PER_TASK}" \
    -Oz -o "${SPLICEAI_MERGED}" \
    "${CHROM_VCFS[@]}"
${TABIX} -p vcf "${SPLICEAI_MERGED}"

TOTAL_VARS=$(${BCFTOOLS} stats "${SPLICEAI_MERGED}" | grep "^SN" | grep "number of records" | cut -f4)
log "Merged SpliceAI VCF: ${TOTAL_VARS} variants → ${SPLICEAI_MERGED}"

# ── Cross-check variant count vs clean_merged ─────────────────────────────────
CLEAN_MERGED="${DIR_MERGED}/clean_merged.vcf.gz"
CLEAN_VARS=$(${BCFTOOLS} stats "${CLEAN_MERGED}" | grep "^SN" | grep "number of records" | cut -f4)
log "clean_merged: ${CLEAN_VARS} variants | SpliceAI merged: ${TOTAL_VARS} variants"
[[ "${TOTAL_VARS}" -eq "${CLEAN_VARS}" ]] \
    || log "WARNING: variant count differs — verify SpliceAI did not filter or expand variants"

log "========== STEP 03 COMPLETE =========="
log "Output: ${SPLICEAI_MERGED}"
