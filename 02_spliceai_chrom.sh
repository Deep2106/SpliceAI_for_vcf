#!/usr/bin/env bash
#SBATCH --job-name=spliceai_chrom
#SBATCH --output=%x_%A_%a.log
#SBATCH --error=%x_%A_%a.err
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=32G
#SBATCH --time=16:00:00
#SBATCH --partition=PARTITION_PLACEHOLDER
#SBATCH --account=ACCOUNT_PLACEHOLDER
#SBATCH --mail-type=FAIL
#SBATCH --mail-user=EMAIL_PLACEHOLDER
# =============================================================================
# Step 02: SpliceAI — per-chromosome SLURM array job
#
# Input:   ${DIR_MERGED}/clean_merged.vcf.gz
# Output:  ${DIR_SPLICEAI_CHROMS}/spliceai_${CHROM}.vcf.gz  (+.tbi)
#          (empty placeholder created for chromosomes absent in VCF)
#
# Submit:  sbatch --array=1-24 02_spliceai_chrom.sh
#          (master_submit.sh handles this automatically)
# =============================================================================
set -euo pipefail

# Default CPU count when running outside SLURM (e.g. interactive test)
SLURM_CPUS_PER_TASK="${SLURM_CPUS_PER_TASK:-4}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/config.sh"

IDX=$(( SLURM_ARRAY_TASK_ID - 1 ))
CHROM="${CHROMOSOMES[$IDX]}"

LOG_FILE="${DIR_LOGS}/02_spliceai_${CHROM}.log"
mkdir -p "${DIR_SPLICEAI_CHROMS}" "${DIR_LOGS}" "${PIPELINE_TMPDIR}"
export TMPDIR="${PIPELINE_TMPDIR}"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "${LOG_FILE}"; }

CLEAN_MERGED="${DIR_MERGED}/clean_merged.vcf.gz"
CHROM_VCF="${DIR_SPLICEAI_CHROMS}/chrom_${CHROM}.vcf.gz"
SPLICEAI_OUT="${DIR_SPLICEAI_CHROMS}/spliceai_${CHROM}.vcf.gz"

log "========== STEP 02: SpliceAI | ${CHROM} (array task ${SLURM_ARRAY_TASK_ID}) =========="

# Guard: skip if chromosome is absent in VCF (e.g. chrY in female cohorts)
CHR_EXISTS=$(${BCFTOOLS} index --stats "${CLEAN_MERGED}" \
    | awk -v c="${CHROM}" 'BEGIN{r=0} $1==c{r=1} END{print r}')
if [[ "${CHR_EXISTS}" -eq 0 ]]; then
    log "WARNING: ${CHROM} not found in clean_merged — creating empty placeholder."
    ${BCFTOOLS} view -h "${CLEAN_MERGED}" | ${BGZIP} > "${SPLICEAI_OUT}"
    ${TABIX} -p vcf "${SPLICEAI_OUT}"
    log "========== STEP 02 COMPLETE | ${CHROM} (empty placeholder) =========="
    exit 0
fi

# ── Extract chromosome ────────────────────────────────────────────────────────
log "Extracting ${CHROM} from clean_merged..."
${BCFTOOLS} view \
    -r "${CHROM}" \
    --threads "${SLURM_CPUS_PER_TASK}" \
    -Oz -o "${CHROM_VCF}" \
    "${CLEAN_MERGED}"
${TABIX} -p vcf "${CHROM_VCF}"
NVAR=$(${BCFTOOLS} stats "${CHROM_VCF}" | grep "^SN" | grep "number of records" | cut -f4)
log "Variants on ${CHROM}: ${NVAR}"

# ── Run SpliceAI ──────────────────────────────────────────────────────────────
SPLICEAI_RAW="${DIR_SPLICEAI_CHROMS}/spliceai_${CHROM}_raw.vcf"
log "Running SpliceAI (dist=${SPLICEAI_DIST}, genome=${SPLICEAI_GENOME})..."
${SPLICEAI_BIN} \
    -I "${CHROM_VCF}" \
    -O "${SPLICEAI_RAW}" \
    -R "${GENOME_FASTA}" \
    -A "${SPLICEAI_GENOME}" \
    -D "${SPLICEAI_DIST}"

# ── Compress and index ────────────────────────────────────────────────────────
${BGZIP} -f "${SPLICEAI_RAW}"
mv "${SPLICEAI_RAW}.gz" "${SPLICEAI_OUT}"
${TABIX} -p vcf "${SPLICEAI_OUT}"

NVAR_OUT=$(${BCFTOOLS} stats "${SPLICEAI_OUT}" | grep "^SN" | grep "number of records" | cut -f4)
log "SpliceAI output variants: ${NVAR_OUT} (input: ${NVAR})"
[[ "${NVAR_OUT}" -eq "${NVAR}" ]] \
    || log "WARNING: count mismatch post-SpliceAI (check for multiallelic expansion)"

log "========== STEP 02 COMPLETE | ${CHROM} =========="
log "Output: ${SPLICEAI_OUT}"
