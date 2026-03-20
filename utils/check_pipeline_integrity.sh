#!/usr/bin/env bash
# =============================================================================
# utils/check_pipeline_integrity.sh
#
# Verifies all pipeline outputs are present and non-empty after a run.
# Run from anywhere after the full pipeline completes:
#
#   bash /path/to/pipeline/utils/check_pipeline_integrity.sh
#
# Checks:
#   Step 01 : clean_merged.vcf.gz (+.tbi), sample_list.txt
#   Step 02 : spliceai_${CHROM}.vcf.gz (+.tbi) for all 24 chromosomes
#   Step 03 : spliceai_all_chroms.vcf.gz (+.tbi)
#   Step 04 : ${SAMPLE}_spliceai.vcf.gz (+.tbi)
#   Step 05 : ${SAMPLE}.hg38_multianno.txt
#             ${SAMPLE}.hg38_multianno.vcf.gz (+.tbi)   [bgzipped in step 05]
#   Step 06 : ${SAMPLE}_final_annotated.txt
#
# Uses ${BCFTOOLS} from config.sh (Singularity-wrapped) for variant counts.
# =============================================================================
set -euo pipefail

# Navigate up one level from utils/ to reach the pipeline SCRIPT_DIR
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${SCRIPT_DIR}/config.sh"

SAMPLE_LIST="${DIR_MERGED}/sample_list.txt"
[[ -f "${SAMPLE_LIST}" ]] || { echo "ERROR: sample_list.txt not found: ${SAMPLE_LIST}"; exit 1; }
mapfile -t SAMPLES < "${SAMPLE_LIST}"

PASS=0
FAIL=0

chk() {
    local desc="$1" path="$2"
    if [[ -s "${path}" ]]; then
        echo "  [PASS] ${desc}"
        PASS=$(( PASS + 1 ))
    else
        echo "  [FAIL] ${desc}"
        echo "         path: ${path}"
        FAIL=$(( FAIL + 1 ))
    fi
}

chkvcf() {
    local desc="$1" path="$2"
    chk "${desc}" "${path}"
    chk "${desc} .tbi" "${path}.tbi"
    if [[ -s "${path}" ]]; then
        N=$(${BCFTOOLS} stats "${path}" 2>/dev/null \
            | grep "^SN.*number of records" | cut -f4 || echo "err")
        echo "         variants: ${N}"
    fi
}

echo "========== Pipeline Integrity Check =========="
echo "  Project : ${PROJECT_DIR}"
echo "  Samples : ${#SAMPLES[@]}"
echo ""

# ── Step 01 ───────────────────────────────────────────────────────────────────
echo "--- Step 01: Merge / Normalize ---"
chkvcf "clean_merged" "${DIR_MERGED}/clean_merged.vcf.gz"
chk    "sample_list"  "${DIR_MERGED}/sample_list.txt"

# ── Step 02 ───────────────────────────────────────────────────────────────────
echo ""
echo "--- Step 02: SpliceAI per chromosome ---"
for C in "${CHROMOSOMES[@]}"; do
    chkvcf "spliceai ${C}" "${DIR_SPLICEAI_CHROMS}/spliceai_${C}.vcf.gz"
done

# ── Step 03 ───────────────────────────────────────────────────────────────────
echo ""
echo "--- Step 03: Merged SpliceAI ---"
chkvcf "spliceai_all_chroms" "${DIR_SPLICEAI_MERGED}/spliceai_all_chroms.vcf.gz"

# ── Steps 04-06 ───────────────────────────────────────────────────────────────
echo ""
echo "--- Steps 04–06: Per-sample ---"
for SAMPLE in "${SAMPLES[@]}"; do
    SS="${SAMPLE//\//_}"
    echo "  [ ${SAMPLE} ]"
    chkvcf "    04 spliceai-annotated" \
        "${DIR_SAMPLES}/${SS}/${SS}_spliceai.vcf.gz"
    chk    "    05 annovar txt" \
        "${DIR_ANNOVAR}/${SS}/${SS}.${ANNOVAR_BUILD}_multianno.txt"
    chkvcf "    05 annovar vcf" \
        "${DIR_ANNOVAR}/${SS}/${SS}.${ANNOVAR_BUILD}_multianno.vcf.gz"
    chk    "    06 final annotated" \
        "${DIR_FINAL}/${SS}_final_annotated.txt"
done

# ── Summary ───────────────────────────────────────────────────────────────────
echo ""
echo "========== PASS=${PASS}  FAIL=${FAIL} =========="
if [[ "${FAIL}" -eq 0 ]]; then
    echo "All checks PASSED."
else
    echo "ACTION REQUIRED: ${FAIL} check(s) failed — see above."
    exit 1
fi
