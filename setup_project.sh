#!/usr/bin/env bash
# =============================================================================
# setup_project.sh
#
# Stages raw VCF files into a new project directory before pipeline submission.
#
# What it does:
#   1. Creates ${PROJECT_DIR}/vcf/
#   2. Finds all *.vcf.gz files in SOURCE_VCF_DIR (non-recursive)
#   3. Moves each .vcf.gz + .vcf.gz.tbi into ${PROJECT_DIR}/vcf/
#   4. Auto-generates any missing .tbi indexes via tabix (bcftools container)
#   5. Validates each staged file with bcftools stats
#   6. Prints the next steps (config.sh edits + master_submit.sh command)
#
# Usage:
#   bash setup_project.sh --project-dir PROJECT_DIR --vcf-source SOURCE_VCF_DIR
#
# Example:
#   bash setup_project.sh \
#       --project-dir /home/data/human_genetics/DATA/myproject_2026 \
#       --vcf-source  /home/data/incoming/batch_march2026
#
# After this completes:
#   1. Edit config.sh: set PROJECT_DIR and SCRIPT_DIR
#   2. bash master_submit.sh --dry-run   # preview
#   3. bash master_submit.sh             # submit
# =============================================================================
set -euo pipefail

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"; }

# =============================================================================
# Parse arguments
# =============================================================================
PROJECT_DIR=""
SOURCE_VCF_DIR=""

usage() {
    echo "Usage: bash setup_project.sh --project-dir PROJECT_DIR --vcf-source SOURCE_VCF_DIR"
    exit 1
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --project-dir) PROJECT_DIR="${2:-}";    shift 2 ;;
        --vcf-source)  SOURCE_VCF_DIR="${2:-}"; shift 2 ;;
        -h|--help)     usage ;;
        *) echo "Unknown argument: $1"; usage ;;
    esac
done

[[ -z "${PROJECT_DIR}"    ]] && { echo "ERROR: --project-dir is required"; usage; }
[[ -z "${SOURCE_VCF_DIR}" ]] && { echo "ERROR: --vcf-source is required";  usage; }
[[ -d "${SOURCE_VCF_DIR}" ]] || { echo "ERROR: Source dir not found: ${SOURCE_VCF_DIR}"; exit 1; }

VCF_DIR="${PROJECT_DIR}/vcf"
PIPELINE_TMPDIR="${PROJECT_DIR}/tmp"

# =============================================================================
# Tool setup — prefer native binaries on login node; fall back to Singularity
# setup_project.sh runs interactively where 'module load singularity' may not
# have been called. The move + index steps are what matter; bcftools stats is
# a validation bonus that is skipped gracefully if neither is available.
# =============================================================================
SIF_DIR="/home/ONE4ALL/Docker_Images"
SIF_BCFTOOLS="${SIF_DIR}/bcftool_1.23.sif"

mkdir -p "${VCF_DIR}" "${PIPELINE_TMPDIR}"

# Resolve tabix and bcftools — native first, then Singularity, then warn
if command -v tabix &>/dev/null; then
    TABIX_CMD="tabix"
    BCFTOOLS_CMD="bcftools"
    log "Using native tabix/bcftools: $(command -v tabix)"
elif command -v singularity &>/dev/null; then
    SING_BASE="singularity exec --no-home --cleanenv \
        --bind ${VCF_DIR},${PIPELINE_TMPDIR} \
        --env TMPDIR=${PIPELINE_TMPDIR}"
    TABIX_CMD="${SING_BASE} ${SIF_BCFTOOLS} tabix"
    BCFTOOLS_CMD="${SING_BASE} ${SIF_BCFTOOLS} bcftools"
    log "Using Singularity bcftools: ${SIF_BCFTOOLS}"
else
    TABIX_CMD=""
    BCFTOOLS_CMD=""
    log "WARNING: Neither tabix nor singularity found in PATH."
    log "         Auto-indexing and stats validation will be skipped."
    log "         Load the module first if you need them:"
    log "           module load singularity   # or: module load htslib"
fi

# =============================================================================
# Discover source VCFs
# =============================================================================
log "========== setup_project.sh =========="
log "Source VCF dir : ${SOURCE_VCF_DIR}"
log "Project dir    : ${PROJECT_DIR}"
log "Staging to     : ${VCF_DIR}"
echo ""

mapfile -t SRC_VCFS < <(find "${SOURCE_VCF_DIR}" -maxdepth 1 -name "*.vcf.gz" | sort)

if [[ "${#SRC_VCFS[@]}" -eq 0 ]]; then
    log "ERROR: No *.vcf.gz files found in ${SOURCE_VCF_DIR}"
    exit 1
fi

log "Found ${#SRC_VCFS[@]} VCF(s) to stage:"
for vcf in "${SRC_VCFS[@]}"; do
    log "  $(basename "${vcf}")"
done
echo ""

# =============================================================================
# Confirm before moving (files will be MOVED, not copied)
# =============================================================================
read -rp "Proceed? Files will be MOVED into ${VCF_DIR} [y/N]: " CONFIRM
[[ "${CONFIRM,,}" == "y" ]] || { log "Aborted."; exit 0; }
echo ""

# =============================================================================
# Move VCFs and indexes; auto-index if .tbi missing
# =============================================================================
STAGED=0
INDEXED=0
ERRORS=0

for SRC_VCF in "${SRC_VCFS[@]}"; do
    BASENAME="$(basename "${SRC_VCF}")"
    DEST_VCF="${VCF_DIR}/${BASENAME}"
    SRC_TBI="${SRC_VCF}.tbi"
    DEST_TBI="${DEST_VCF}.tbi"

    log "Staging: ${BASENAME}"

    # Move VCF (skip if already in place)
    if [[ -f "${DEST_VCF}" ]]; then
        log "  SKIP (already exists): ${DEST_VCF}"
    else
        mv "${SRC_VCF}" "${DEST_VCF}"
        log "  Moved VCF → ${DEST_VCF}"
    fi
    STAGED=$(( STAGED + 1 ))

    # Move or generate .tbi index
    if [[ -f "${DEST_TBI}" ]]; then
        log "  Index already present: ${DEST_TBI}"
    elif [[ -f "${SRC_TBI}" ]]; then
        mv "${SRC_TBI}" "${DEST_TBI}"
        log "  Moved index → ${DEST_TBI}"
    elif [[ -n "${TABIX_CMD}" ]]; then
        log "  No .tbi found at source — generating index..."
        ${TABIX_CMD} -p vcf "${DEST_VCF}"
        log "  Index generated: ${DEST_TBI}"
        INDEXED=$(( INDEXED + 1 ))
    else
        log "  WARNING: No .tbi found and tabix unavailable — skipping auto-index."
        log "           Run manually: tabix -p vcf ${DEST_VCF}"
    fi

    # Validate via bcftools stats (skipped if bcftools unavailable)
    if [[ -n "${BCFTOOLS_CMD}" ]]; then
        N_VARS=$(${BCFTOOLS_CMD} stats "${DEST_VCF}" \
            | grep "^SN" | grep "number of records" | cut -f4 || echo "ERR")
        if [[ "${N_VARS}" == "ERR" ]]; then
            log "  ERROR: bcftools stats failed for ${BASENAME}"
            ERRORS=$(( ERRORS + 1 ))
        else
            log "  OK — variants: ${N_VARS}"
        fi
    else
        log "  SKIP stats (bcftools unavailable) — file staged and index present."
    fi
done

# =============================================================================
# Summary and next steps
# =============================================================================
echo ""
log "========== Setup complete =========="
log "  Staged    : ${STAGED} VCF(s)"
log "  Re-indexed: ${INDEXED} (missing .tbi generated)"
log "  Errors    : ${ERRORS}"
log "  VCF dir   : ${VCF_DIR}"
echo ""

if [[ "${ERRORS}" -gt 0 ]]; then
    log "ACTION REQUIRED: ${ERRORS} file(s) failed validation. Check logs above."
    exit 1
fi

log "Next steps:"
log "  1. Edit config.sh:"
log "       PROJECT_DIR=\"${PROJECT_DIR}\""
log "       SCRIPT_DIR=\"/path/to/pipeline/scripts\""
log "  2. bash master_submit.sh --dry-run   # preview job chain"
log "  3. bash master_submit.sh             # submit to SLURM"
