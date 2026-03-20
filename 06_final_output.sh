#!/usr/bin/env bash
#SBATCH --job-name=final_output
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
# Step 06: Append SpliceAI scores + OMIM annotations to ANNOVAR multianno.txt
#
# Input:   ${DIR_ANNOVAR}/${SAMPLE_SAFE}/${SAMPLE_SAFE}.hg38_multianno.txt  (step 05)
#          ${DIR_SAMPLES}/${SAMPLE_SAFE}/${SAMPLE_SAFE}_spliceai.vcf.gz     (step 04)
#          ${OMIM_CSV}
# Output:  ${DIR_FINAL}/${SAMPLE_SAFE}_final_annotated.txt
#
# Python utility (utils/add_spliceai_to_annovar.py) runs via python_3.14.2.sif.
# SCRIPT_DIR is bind-mounted so the container can access the .py script.
# OMIM directory is also bound separately (may be outside BASE_OUTDIR).
#
# Submit:  sbatch --array=1-N_SAMPLES 06_final_output.sh
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

LOG_FILE="${DIR_LOGS}/06_final_${SAMPLE_SAFE}.log"
mkdir -p "${DIR_FINAL}" "${DIR_LOGS}" "${PIPELINE_TMPDIR}"
export TMPDIR="${PIPELINE_TMPDIR}"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "${LOG_FILE}"; }
log "========== STEP 06: Final Annotated Output | ${SAMPLE} =========="

MULTIANNO_TXT="${DIR_ANNOVAR}/${SAMPLE_SAFE}/${SAMPLE_SAFE}.${ANNOVAR_BUILD}_multianno.txt"
# SpliceAI source: step 04 output — guaranteed to have unmodified INFO/SpliceAI
SPLICEAI_VCF="${DIR_SAMPLES}/${SAMPLE_SAFE}/${SAMPLE_SAFE}_spliceai.vcf.gz"

[[ -f "${MULTIANNO_TXT}" ]] || { log "ERROR: multianno.txt not found: ${MULTIANNO_TXT}"; exit 1; }
[[ -f "${SPLICEAI_VCF}" ]]  || { log "ERROR: SpliceAI VCF not found (run step 04 first): ${SPLICEAI_VCF}"; exit 1; }
[[ -f "${OMIM_CSV}" ]]      || { log "ERROR: OMIM CSV not found: ${OMIM_CSV}"; exit 1; }

FINAL_OUT="${DIR_FINAL}/${SAMPLE_SAFE}_final_annotated.txt"
OMIM_DIR="$(dirname "${OMIM_CSV}")"

log "Input  TXT: ${MULTIANNO_TXT}"
log "Input  VCF: ${SPLICEAI_VCF}"
log "Input OMIM: ${OMIM_CSV}"
log "Output    : ${FINAL_OUT}"

# ── Run Python utility via Singularity ───────────────────────────────────────
# SCRIPT_DIR bound so the container can read utils/add_spliceai_to_annovar.py
# OMIM_DIR   bound separately (may be outside BASE_OUTDIR / BIND_MOUNTS)
PY_CMD="singularity exec --no-home --cleanenv \
    --bind ${BIND_MOUNTS},${SCRIPT_DIR},${OMIM_DIR} \
    --env TMPDIR=${PIPELINE_TMPDIR} \
    ${SIF_PYTHON} python3"

log "Merging SpliceAI scores and OMIM annotations into ANNOVAR table..."
${PY_CMD} "${SCRIPT_DIR}/utils/add_spliceai_to_annovar.py" \
    --annovar-txt  "${MULTIANNO_TXT}" \
    --spliceai-vcf "${SPLICEAI_VCF}" \
    --omim-csv     "${OMIM_CSV}" \
    --output       "${FINAL_OUT}" \
    --sample       "${SAMPLE}"

[[ -f "${FINAL_OUT}" ]] || { log "ERROR: Final output not created: ${FINAL_OUT}"; exit 1; }
log "Output lines: $(wc -l < "${FINAL_OUT}")"

log "========== STEP 06 COMPLETE | ${SAMPLE} =========="
log "Output: ${FINAL_OUT}"
