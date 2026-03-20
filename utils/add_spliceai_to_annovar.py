#!/usr/bin/env python3
"""
add_spliceai_to_annovar.py
Merges SpliceAI scores + OMIM annotations into ANNOVAR multianno.txt.
  - SpliceAI matched by (CHROM, POS, REF, ALT)
  - OMIM matched by Gene.refGene symbol (semicolon-delimited for multi-gene rows)

Location: utils/add_spliceai_to_annovar.py
Called by: 06_final_output.sh
"""
import argparse, csv, gzip, logging, re, sys
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

SPLICEAI_COLS = [
    "SpliceAI_SYMBOL",
    "SpliceAI_DS_AG", "SpliceAI_DS_AL",
    "SpliceAI_DS_DG", "SpliceAI_DS_DL",
    "SpliceAI_DP_AG", "SpliceAI_DP_AL",
    "SpliceAI_DP_DG", "SpliceAI_DP_DL",
]

OMIM_COLS = [
    "OMIM_MIM_Number",
    "OMIM_Gene_Name",
    "OMIM_Phenotypes",
]

NA = "."

# Zygosity value from ANNOVAR Otherinfo column -> GT label
# avinput column 6: 1 = homozygous, 0.5 = heterozygous, 0 = hom-ref
ZYGOSITY_MAP = {
    "1":   "Hom",
    "0.5": "Het",
    "0":   "Hom_ref",
}


# =============================================================================
# SpliceAI parsing
# =============================================================================

def parse_spliceai(info):
    """Parse INFO/SpliceAI field; return dict of SPLICEAI_COLS, best-scoring allele."""
    result = {c: NA for c in SPLICEAI_COLS}
    m = re.search(r"SpliceAI=([^;]+)", info)
    if not m:
        return result
    best, bvals = -1.0, None
    for entry in m.group(1).split(","):
        p = entry.split("|")
        if len(p) < 10:
            continue
        _, sym, dag, dal, ddg, ddl, pag, pal, pdg, pdl = p[:10]
        try:
            mx = max(float(dag), float(dal), float(ddg), float(ddl))
            if mx > best:
                best = mx
                bvals = [sym, dag, dal, ddg, ddl, pag, pal, pdg, pdl]
        except ValueError:
            continue
    if bvals:
        for k, v in zip(SPLICEAI_COLS, bvals):
            result[k] = v if v not in ("", ".") else NA
    return result


def vkey(chrom, pos, ref, alt):
    """Variant key normalised to no-chr prefix, uppercase alleles."""
    return (chrom.lstrip("chr"), pos, ref.upper(), alt.upper())


def load_vcf(path):
    """Load SpliceAI VCF into dict keyed by (chrom_nochr, pos, ref, alt)."""
    log.info(f"Loading SpliceAI VCF: {path}")
    store = {}
    opener = gzip.open if path.endswith(".gz") else open
    with opener(path, "rt") as fh:
        for line in fh:
            if line.startswith("#"):
                continue
            p = line.rstrip().split("\t")
            if len(p) < 8:
                continue
            chrom, pos, _, ref, alts, _, _, info = p[:8]
            for alt in alts.split(","):
                store[vkey(chrom, pos, ref, alt)] = parse_spliceai(info)
    log.info(f"  -> {len(store)} SpliceAI variants loaded")
    return store


# =============================================================================
# OMIM parsing
# =============================================================================

def load_omim(path):
    """
    Build dict: gene_symbol (uppercase) -> {OMIM_MIM_Number, OMIM_Gene_Name, OMIM_Phenotypes}

    OMIM CSV columns used: Approved Symbol, Gene Symbols, Gene Name, MIM Number, Phenotypes
    Indexes both Approved Symbol and all comma-separated aliases in Gene Symbols.
    """
    log.info(f"Loading OMIM: {path}")
    store = {}

    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            record = {
                "OMIM_MIM_Number": row.get("MIM Number", NA).strip() or NA,
                "OMIM_Gene_Name":  row.get("Gene Name",  NA).strip() or NA,
                "OMIM_Phenotypes": row.get("Phenotypes", NA).strip() or NA,
            }
            sym = row.get("Approved Symbol", "").strip().upper()
            if sym:
                store[sym] = record
            for alias in row.get("Gene Symbols", "").split(","):
                alias = alias.strip().upper()
                if alias and alias not in store:
                    store[alias] = record

    log.info(f"  -> {len(store)} OMIM gene entries loaded")
    return store


def lookup_omim(gene_field, omim_store):
    """
    ANNOVAR Gene.refGene can be: GENE1, GENE1;GENE2, or NONE.
    Try each semicolon/comma-delimited symbol; return first hit or NA record.
    """
    empty = {c: NA for c in OMIM_COLS}
    for sym in re.split(r"[;,]", gene_field or ""):
        sym = sym.strip().upper()
        if sym in omim_store:
            return omim_store[sym]
    return empty


# =============================================================================
# Main merge
# =============================================================================

def merge(annovar_txt, spliceai_store, omim_store, output, sample):
    opener = gzip.open if annovar_txt.endswith(".gz") else open
    total = sai_matched = omim_matched = 0

    with opener(annovar_txt, "rt") as fin, open(output, "w", newline="") as fout:
        reader = csv.DictReader(fin, delimiter="\t")
        # ANNOVAR trailing tab creates a None key — strip it
        reader.fieldnames = [f for f in (reader.fieldnames or []) if f is not None]

        missing = {"Chr", "Start", "Ref", "Alt"} - set(reader.fieldnames)
        if missing:
            raise ValueError(f"ANNOVAR table missing required columns: {missing}")

        # Detect gene column (refGene annotation)
        gene_col = None
        for candidate in ("Gene.refGene", "Gene_refGene", "GeneName"):
            if candidate in reader.fieldnames:
                gene_col = candidate
                break
        if gene_col is None:
            log.warning("Gene.refGene column not found — OMIM annotation will be skipped")

        # Detect Otherinfo column (carries avinput zygosity: 1/0.5/0)
        otherinfo_col = None
        for candidate in reader.fieldnames:
            if candidate.lower().startswith("otherinfo"):
                otherinfo_col = candidate
                break
        if otherinfo_col is None:
            log.warning("Otherinfo column not found — GT will not be added")

        # Build output field list: insert GT immediately after Alt
        raw_fields = list(reader.fieldnames)
        alt_idx = raw_fields.index("Alt") + 1
        out_fields = raw_fields[:alt_idx] + ["GT"] + raw_fields[alt_idx:]
        out_fields += SPLICEAI_COLS
        if gene_col:
            out_fields += OMIM_COLS

        writer = csv.DictWriter(
            fout, fieldnames=out_fields, delimiter="\t", lineterminator="\n"
        )
        writer.writeheader()

        for row in reader:
            total += 1

            # GT — derived from Otherinfo zygosity value (1=Hom, 0.5=Het, 0=Hom_ref)
            if otherinfo_col:
                zyg = str(row.get(otherinfo_col, "")).strip()
                row["GT"] = ZYGOSITY_MAP.get(zyg, NA)
            else:
                row["GT"] = NA

            # SpliceAI lookup
            key = vkey(row["Chr"], row["Start"], row["Ref"], row["Alt"])
            sai_data = spliceai_store.get(key)
            if sai_data:
                sai_matched += 1
                row.update(sai_data)
            else:
                row.update({c: NA for c in SPLICEAI_COLS})

            # OMIM lookup
            if gene_col:
                omim_data = lookup_omim(row.get(gene_col, ""), omim_store)
                if omim_data["OMIM_MIM_Number"] != NA:
                    omim_matched += 1
                row.update(omim_data)

            # Strip None keys (ANNOVAR trailing tab artefact)
            row = {k: v for k, v in row.items() if k is not None}
            writer.writerow(row)

    sai_pct  = f"{sai_matched / total * 100:.1f}" if total else "0.0"
    omim_pct = f"{omim_matched / total * 100:.1f}" if total else "0.0"
    log.info(
        f"{sample}: {total} rows | "
        f"SpliceAI: {sai_matched} ({sai_pct}%) | "
        f"OMIM: {omim_matched} ({omim_pct}%)"
    )
    if sai_matched == 0 and total > 0:
        log.error("Zero SpliceAI matches — check chr prefix consistency (chr1 vs 1)")
        sys.exit(1)


def main():
    ap = argparse.ArgumentParser(
        description="Merge SpliceAI scores and OMIM annotations into ANNOVAR multianno.txt"
    )
    ap.add_argument("--annovar-txt",  required=True, help="ANNOVAR *_multianno.txt file")
    ap.add_argument("--spliceai-vcf", required=True, help="Per-sample SpliceAI-annotated VCF (.vcf.gz)")
    ap.add_argument("--omim-csv",     required=True, help="OMIM_Summary_File.csv")
    ap.add_argument("--output",       required=True, help="Output TSV path")
    ap.add_argument("--sample",       default="",    help="Sample name for log messages")
    a = ap.parse_args()

    for p in [a.annovar_txt, a.spliceai_vcf, a.omim_csv]:
        if not Path(p).is_file():
            log.error(f"Input not found: {p}")
            sys.exit(1)

    Path(a.output).parent.mkdir(parents=True, exist_ok=True)

    spliceai_store = load_vcf(a.spliceai_vcf)
    omim_store     = load_omim(a.omim_csv)
    merge(a.annovar_txt, spliceai_store, omim_store, a.output, a.sample)
    log.info("Done.")


if __name__ == "__main__":
    main()
