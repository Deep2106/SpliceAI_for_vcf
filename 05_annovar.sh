#!/usr/bin/env bash
#SBATCH --job-name=annovar
#SBATCH --output=%x_%A_%a.log
#SBATCH --error=%x_%A_%a.err
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=32G
#SBATCH --time=04:00:00
#SBATCH --partition=PARTITION_PLACEHOLDER
#SBATCH --account=ACCOUNT_PLACEHOLDER
#SBATCH --mail-type=FAIL
#SBATCH --mail-user=EMAIL_PLACEHOLDER
# =============================================================================
# Step 05: ANNOVAR annotation — per-sample SLURM array job
#
# Input:   ${DIR_SAMPLES}/${SAMPLE_SAFE}/${SAMPLE_SAFE}_spliceai.vcf.gz  (step 04)
# Output:  ${DIR_ANNOVAR}/${SAMPLE_SAFE}/${SAMPLE_SAFE}.hg38_multianno.txt
#          ${DIR_ANNOVAR}/${SAMPLE_SAFE}/${SAMPLE_SAFE}.hg38_multianno.vcf.gz  (+.tbi)
#
# Notes:
#   - Input VCF is decompressed for ANNOVAR, then cleaned up afterwards
#   - -vcfinput preserves the SpliceAI INFO field in the output VCF
#   - ANNOVAR is run via a wrapper script to avoid quoting issues with -argument
#   - Output VCF is bgzipped + tabix-indexed after ANNOVAR completes
#
# Submit:  sbatch --array=1-N_SAMPLES 05_annovar.sh
#          (master_submit.sh handles this automatically)
# =============================================================================
set -euo pipefail

# Default CPU count when running outside SLURM (e.g. interactive test)
SLURM_CPUS_PER_TASK="${SLURM_CPUS_PER_TASK:-8}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/config.sh"

SAMPLE_LIST="${DIR_MERGED}/sample_list.txt"
[[ -f "${SAMPLE_LIST}" ]] || { echo "ERROR: sample_list.txt not found: ${SAMPLE_LIST}"; exit 1; }

mapfile -t SAMPLES < "${SAMPLE_LIST}"
IDX=$(( SLURM_ARRAY_TASK_ID - 1 ))
SAMPLE="${SAMPLES[$IDX]}"
SAMPLE_SAFE="${SAMPLE//\//_}"

LOG_FILE="${DIR_LOGS}/05_annovar_${SAMPLE_SAFE}.log"
SAMPLE_DIR="${DIR_SAMPLES}/${SAMPLE_SAFE}"
ANNOVAR_SAMPLE_DIR="${DIR_ANNOVAR}/${SAMPLE_SAFE}"
mkdir -p "${ANNOVAR_SAMPLE_DIR}" "${DIR_LOGS}" "${PIPELINE_TMPDIR}"
export TMPDIR="${PIPELINE_TMPDIR}"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "${LOG_FILE}"; }
log "========== STEP 05: ANNOVAR | ${SAMPLE} =========="

SAMPLE_ANNOTATED="${SAMPLE_DIR}/${SAMPLE_SAFE}_spliceai.vcf.gz"
[[ -f "${SAMPLE_ANNOTATED}" ]] || { log "ERROR: Input not found: ${SAMPLE_ANNOTATED}"; exit 1; }

# ── Decompress for ANNOVAR ────────────────────────────────────────────────────
# table_annovar.pl handles .vcf.gz but uncompressed is safer inside Singularity
INPUT_VCF="${ANNOVAR_SAMPLE_DIR}/${SAMPLE_SAFE}_spliceai.vcf"
log "Decompressing VCF for ANNOVAR → ${INPUT_VCF}"
${BGZIP} -d -c "${SAMPLE_ANNOTATED}" > "${INPUT_VCF}"

OUTPREFIX="${ANNOVAR_SAMPLE_DIR}/${SAMPLE_SAFE}"

log "Running table_annovar.pl..."
log "  Build    : ${ANNOVAR_BUILD}"
log "  Protocol : ${ANNOVAR_PROTOCOL}"
log "  Operation: ${ANNOVAR_OPERATION}"
log "  Threads  : ${SLURM_CPUS_PER_TASK}"

# ── Write ANNOVAR wrapper script ──────────────────────────────────────────────
# A wrapper avoids quoting/escaping issues with -argument when passed through
# singularity exec. The '-splicing 5' literal is safe inside the heredoc.
WRAPPER="${ANNOVAR_SAMPLE_DIR}/run_annovar_${SAMPLE_SAFE}.sh"
cat > "${WRAPPER}" << ANNOVAR_WRAPPER
#!/usr/bin/env bash
perl ${ANNOVAR_BIN_IN_CONTAINER}/table_annovar.pl \\
    ${INPUT_VCF} \\
    ${ANNOVAR_DB} \\
    -buildver ${ANNOVAR_BUILD} \\
    -out ${OUTPREFIX} \\
    -remove \\
    -protocol ${ANNOVAR_PROTOCOL} \\
    -operation ${ANNOVAR_OPERATION} \\
    -argument '-splicing 5',,,,,,,,,,,, \\
    -nastring . \\
    -vcfinput \\
    -polish \\
    -thread ${SLURM_CPUS_PER_TASK}
ANNOVAR_WRAPPER
chmod +x "${WRAPPER}"

singularity exec --no-home --cleanenv \
    --bind "${BIND_MOUNTS}" \
    --env TMPDIR="${PIPELINE_TMPDIR}" \
    "${SIF_ANNOVAR}" bash "${WRAPPER}"

# ── Compress and index ANNOVAR output VCF ────────────────────────────────────
# ANNOVAR produces an uncompressed _multianno.vcf alongside the _multianno.txt.
# Compress + index so check_pipeline_integrity.sh can validate it.
MULTIANNO_VCF="${OUTPREFIX}.${ANNOVAR_BUILD}_multianno.vcf"
MULTIANNO_VCF_GZ="${MULTIANNO_VCF}.gz"
if [[ -f "${MULTIANNO_VCF}" ]]; then
    log "Compressing ANNOVAR output VCF → ${MULTIANNO_VCF_GZ}"
    ${BGZIP} -f "${MULTIANNO_VCF}"
    ${TABIX} -p vcf "${MULTIANNO_VCF_GZ}"
    log "Indexed: ${MULTIANNO_VCF_GZ}.tbi"
else
    log "WARNING: Expected ANNOVAR VCF not found: ${MULTIANNO_VCF}"
fi

# ── Verify multianno.txt was produced ────────────────────────────────────────
MULTIANNO_TXT="${OUTPREFIX}.${ANNOVAR_BUILD}_multianno.txt"
[[ -f "${MULTIANNO_TXT}" ]] \
    || { log "ERROR: multianno.txt not produced: ${MULTIANNO_TXT}"; exit 1; }
log "multianno.txt lines: $(wc -l < "${MULTIANNO_TXT}")"

# ── Clean up decompressed input VCF to save disk space ───────────────────────
log "Removing temporary decompressed VCF: ${INPUT_VCF}"
rm -f "${INPUT_VCF}"

log "========== STEP 05 COMPLETE | ${SAMPLE} =========="
log "Output TXT: ${MULTIANNO_TXT}"
log "Output VCF: ${MULTIANNO_VCF_GZ}"
