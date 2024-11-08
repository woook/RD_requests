## Find and merge VCFs for creation of a 38 POP AF VCF

### Description
This folder holds code to create an internal GRCh38 POP AF VCF based for [EBH-435](https://cuhbioinformatics.atlassian.net/browse/DI-435).

### Python script to find VCFs
The Python script `find_vcfs_to_merge.py` takes inputs:
- `-a --assay`: The project prefix to look for in DNAnexus, e.g. `"*CEN38"`
- `-o --outfile_prefix`: What to name the output TSV file which lists VCF files to merge
- `-s --start (optional)`: A date used to find DNAnexus projects created after
- `-e --end (optional)`: A date used to find DNAnexus projects created before

Example Python script command:
`python3 find_vcfs_to_merge.py --assay "*CEN38" --end 2024-05-03 --outfile_prefix CEN38`

Output:
- A TSV listing the VCF files for all non-validation samples to merge (named by `{outfile_prefix}_files_to_merge.txt`)
- A CSV of all validation samples found (`{outfile_prefix}_validation_samples.csv`)

How the script works:
1. Finds all DNAnexus projects with suffix `--assay` and between `--start` and `--end` dates (if provided).
2. Finds the related GRCh37 project for each of these projects and reads in all of the QC status files into one merged dataframe. If multiple QC status files exist in a project, the one created last is used.
3. Finds all raw VCFs in each of the DNAnexus projects.
4. Splits these VCFs into a list of validation (including control) samples and non-validation samples based on naming conventions
5. Removes any samples which are duplicates or any which failed QC at any time based on information within the QC status files.
6. Creates a final list of all VCFs to merge and writes this out to file with`--outfile_prefix`.

### Bash script to merge VCFs
The bash script is run in a DNAnexus cloud workstation and requires positional inputs of:
- The output file generated from the Python script above
- The job ID for the cloud workstation running
- The reference genome for GRCh38

Example bash script command:
`bash merge_VCF_AF.sh CEN38_vcf_to_merge.txt job-Gjb39Z04bxf82XZ12gPJ2bbV GRCh38_GIABv3_no_alt_analysis_set_maskedGRC_decoys_MAP2K3_KMT2C_KCNJ18_noChr.fasta`

Output:
Merged VCF file and index named from the job ID given, i.e. `final_merged_job-Gjb39Z04bxf82XZ12gPJ2bbV.vcf.gz` and `final_merged_job-Gjb39Z04bxf82XZ12gPJ2bbV.vcf.gz.tbi` which are both uploaded to the DNAnexus project the cloud workstation is running within.