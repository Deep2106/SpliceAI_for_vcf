#!/usr/bin/env bash
# =============================================================================
# master_submit.sh — Submit all pipeline steps with correct SLURM dependencies
#
# Usage:
#   bash master_submit.sh [--dry-run] [--resume]
#
#   --dry-run   Print what would be submitted without actually submitting
#   --resume    Skip steps whose outputs already exist on disk.
#               Use this after a partial run to continue from where it stopped,
#               without worrying about stale job IDs causing DependencyNeverSatisfied.
#
# Robustness features built in:
#   1. SCRIPT_DIR hardcoded to absolute staged path — immune to SLURM spool trap
#      (SLURM copies scripts to /var/spool/slurmd/jobXXX/ before running them,
#       which breaks dynamic BASH_SOURCE[0] resolution)
#   2. --array=1-N%THROTTLE on all arrays — prevents silent task drops when
#      the cluster's MaxArraySize limit is exceeded
#   3. Resume mode — checks output files before submitting each step; completed
#      steps return sentinel "no_dep" so downstream steps are not blocked by
#      stale/cancelled job IDs from a previous run
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/config.sh"

DRY_RUN=0
RESUME=0
for arg in "$@"; do
    case "${arg}" in
        --dry-run) DRY_RUN=1 ;;
        --resume)  RESUME=1  ;;
    esac
done

# Max simultaneous tasks per array job — prevents hitting cluster MaxArraySize
# limits and avoids flooding the queue.
# Override at runtime: ARRAY_THROTTLE=8 bash master_submit.sh
ARRAY_THROTTLE="${ARRAY_THROTTLE:-16}"

# =============================================================================
# Helpers
# =============================================================================

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" >&2; }

# submit DESC [SBATCH_FLAGS...] SCRIPT
# Echoes job ID to stdout; progress to stderr.
submit() {
    local desc="$1"; shift
    if [[ "${DRY_RUN}" -eq 1 ]]; then
        log "[DRY-RUN] Would submit: sbatch $*"
        echo "dry_run_${RANDOM}"
    else
        local jid
        jid=$(sbatch "$@" | awk '{print $NF}')
        log "[SUBMIT] ${desc} → Job ${jid}"
        echo "${jid}"
    fi
}

# step_done FILE [FILE2 ...]
# Returns 0 if all given files exist and are non-empty.
step_done() {
    local f
    for f in "$@"; do
        [[ -s "${f}" ]] || return 1
    done
    return 0
}

# maybe_submit STEP_LABEL FILE1 [FILE2 ...] -- [SBATCH_FLAGS...] SCRIPT
# Resume mode: if all output files exist, skip and echo "no_dep" sentinel.
# Normal mode: submit unconditionally.
maybe_submit() {
    local desc="$1"; shift
    local check_files=()
    while [[ "$1" != "--" ]]; do
        check_files+=("$1"); shift
    done
    shift  # consume "--"

    if [[ "${RESUME}" -eq 1 ]] && step_done "${check_files[@]}"; then
        log "[SKIP] ${desc} — output already exists, skipping submission"
        echo "no_dep"
    else
        submit "${desc}" "$@"
    fi
}

# submit_with_dep DESC ARRAY_FLAG DEP_IDS_SPACE_SEPARATED SCRIPT
# Builds the sbatch call cleanly, only adding --dependency if real IDs exist.
# DEP_IDS is a space-separated list of job IDs; "no_dep" sentinels are ignored.
submit_with_dep() {
    local desc="$1"
    local array_flag="$2"     # e.g. "1-24%16" or "" for non-array jobs
    local dep_ids="$3"        # space-separated job IDs or "no_dep"
    local script="$4"

    # Build sbatch args
    local args=()
    [[ -n "${array_flag}" ]] && args+=(--array="${array_flag}")

    # Filter out no_dep sentinels and build dependency string
    local real_ids=()
    for id in ${dep_ids}; do
        [[ "${id}" != "no_dep" ]] && real_ids+=("${id}")
    done
    if [[ "${#real_ids[@]}" -gt 0 ]]; then
        local joined
        joined=$(IFS=":"; echo "${real_ids[*]}")
        args+=(--dependency="afterok:${joined}")
    fi

    submit "${desc}" "${args[@]}" "${script}"
}

# =============================================================================
# Stage scripts into ${BASE_OUTDIR}/scripts/
# Patches:
#   - SLURM #SBATCH placeholders (partition, account, email)
#   - --output/--error to absolute DIR_LOGS paths (so logs always land there
#     regardless of the working directory when sbatch is invoked)
#   - SCRIPT_DIR hardcoded to ${SD} (absolute staged path), bypassing
#     BASH_SOURCE[0] which breaks inside SLURM's /var/spool/slurmd/jobXXX/
# =============================================================================
SD="${BASE_OUTDIR}/scripts"
mkdir -p "${SD}" "${DIR_LOGS}"

for f in "${SCRIPT_DIR}"/0*.sh; do
    dst="${SD}/$(basename "${f}")"
    sed \
        -e "s|PARTITION_PLACEHOLDER|${SLURM_PARTITION}|g"     \
        -e "s|ACCOUNT_PLACEHOLDER|${SLURM_ACCOUNT}|g"         \
        -e "s|EMAIL_PLACEHOLDER|${SLURM_EMAIL}|g"             \
        -e "s|#SBATCH --output=%x_%j.log|#SBATCH --output=${DIR_LOGS}/%x_%j.log|g"       \
        -e "s|#SBATCH --error=%x_%j.err|#SBATCH --error=${DIR_LOGS}/%x_%j.err|g"         \
        -e "s|#SBATCH --output=%x_%A_%a.log|#SBATCH --output=${DIR_LOGS}/%x_%A_%a.log|g" \
        -e "s|#SBATCH --error=%x_%A_%a.err|#SBATCH --error=${DIR_LOGS}/%x_%A_%a.err|g"   \
        -e "s|^SCRIPT_DIR=.*|SCRIPT_DIR=\"${SD}\"|"           \
        "${f}" > "${dst}"
    chmod +x "${dst}"
done

cp "${SCRIPT_DIR}/config.sh" "${SD}/config.sh"
if [[ -d "${SCRIPT_DIR}/utils" ]]; then
    cp -r "${SCRIPT_DIR}/utils" "${SD}/utils"
else
    log "WARNING: utils/ not found in ${SCRIPT_DIR} — step 06 will fail."
fi

# =============================================================================
# Pre-submission summary
# =============================================================================
echo ""
echo "========== Pipeline submission =========="
echo "  Project dir   : ${PROJECT_DIR}"
echo "  VCF dir       : ${VCF_DIR}"
echo "  N_SAMPLES     : ${N_SAMPLES}"
echo "  N_CHROMS      : ${N_CHROMS}"
echo "  Array throttle: ${ARRAY_THROTTLE}"
echo "  Resume mode   : $( [[ ${RESUME} -eq 1 ]] && echo ON || echo OFF )"
echo "  Dry run       : $( [[ ${DRY_RUN} -eq 1 ]] && echo ON || echo OFF )"
echo ""
echo "  VCFs to process:"
for vcf in "${VCF_LIST[@]}"; do
    echo "    $(basename "${vcf}")"
done
echo ""

# =============================================================================
# Output sentinels used for resume checks
# =============================================================================
OUT01="${DIR_MERGED}/clean_merged.vcf.gz"
OUT01_LIST="${DIR_MERGED}/sample_list.txt"
OUT02_LAST="${DIR_SPLICEAI_CHROMS}/spliceai_chrY.vcf.gz"
OUT03="${DIR_SPLICEAI_MERGED}/spliceai_all_chroms.vcf.gz"

# =============================================================================
# Job chain
# =============================================================================

# Step 01: Merge + Normalize (no dependency)
JOB01_ID=$(maybe_submit "01_merge_normalize" \
    "${OUT01}" "${OUT01_LIST}" \
    -- \
    "${SD}/01_merge_normalize.sh")

# Step 02: SpliceAI per-chromosome array (depends on 01)
JOB02_ID=$(submit_with_dep "02_spliceai_chrom[1-${N_CHROMS}]" \
    "1-${N_CHROMS}%${ARRAY_THROTTLE}" \
    "${JOB01_ID}" \
    "${SD}/02_spliceai_chrom.sh")

# Step 03: Merge SpliceAI chroms (depends on 02)
JOB03_ID=$(submit_with_dep "03_merge_spliceai" \
    "" \
    "${JOB02_ID}" \
    "${SD}/03_merge_spliceai.sh")

# Step 04: Per-sample split + SpliceAI annotate (depends on 01 AND 03)
JOB04_ID=$(submit_with_dep "04_split_annotate[1-${N_SAMPLES}]" \
    "1-${N_SAMPLES}%${ARRAY_THROTTLE}" \
    "${JOB01_ID} ${JOB03_ID}" \
    "${SD}/04_split_and_annotate.sh")

# Step 05: ANNOVAR (depends on 04)
JOB05_ID=$(submit_with_dep "05_annovar[1-${N_SAMPLES}]" \
    "1-${N_SAMPLES}%${ARRAY_THROTTLE}" \
    "${JOB04_ID}" \
    "${SD}/05_annovar.sh")

# Step 06: Final output (depends on 05)
JOB06_ID=$(submit_with_dep "06_final_output[1-${N_SAMPLES}]" \
    "1-${N_SAMPLES}%${ARRAY_THROTTLE}" \
    "${JOB05_ID}" \
    "${SD}/06_final_output.sh")

# =============================================================================
# Summary
# =============================================================================
echo ""
echo "=== Job Chain Summary ==="
printf "  %-32s %s\n"          "01 merge_normalize"      "${JOB01_ID}"
printf "  %-32s %s  [1-%s]\n" "02 spliceai_chrom_array" "${JOB02_ID}" "${N_CHROMS}"
printf "  %-32s %s\n"          "03 merge_spliceai"       "${JOB03_ID}"
printf "  %-32s %s  [1-%s]\n" "04 split_annotate_array" "${JOB04_ID}" "${N_SAMPLES}"
printf "  %-32s %s  [1-%s]\n" "05 annovar_array"        "${JOB05_ID}" "${N_SAMPLES}"
printf "  %-32s %s  [1-%s]\n" "06 final_output_array"   "${JOB06_ID}" "${N_SAMPLES}"
echo ""
echo "Monitor  : squeue -u ${USER}"
echo "Logs     : ${DIR_LOGS}"
echo ""
echo "On failure, rerun with: bash $(basename "$0") --resume"
