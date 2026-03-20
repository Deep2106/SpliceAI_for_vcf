# ── How to run ──────────────────────────────────────────────────────────────────
# The pipeline designed for HPC usage with singulaity
# we assume the relavant singulaity container already available on HPC
# The version of all singularity containers mentioned in configuration file (annovar,bcftools,spliceAI,python)
# This pipeline works over vcf files >=v4.1. 
# This normalizes the vcf files
# merge all files and then performs choromosomewise SpliceAI 
# merges SpliceAI chromosomes and undergoes Annovar annotation
# adds SpliceAI score to Annovar outputs


# 1. Stage batch VCFs into a NEW project directory
bash /path/to/pipeline/setup_project.sh \
    --project-dir /home/data/proj/wes/batch \
    --vcf-source  /home/data/incoming/batch_vcfs

# 2. Edit config.sh — only these two lines change:
#    PROJECT_DIR="/home/data/wes/batch"
#    SCRIPT_DIR="/path/to/pipeline"
#                ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
#                SCRIPT_DIR always points to where the pipeline scripts live — never changes

# 3. Preview
bash /path/to/pipeline/master_submit.sh --dry-run

# 4. Submit
bash /path/to/pipeline/master_submit.sh
