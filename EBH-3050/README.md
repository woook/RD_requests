## Workbooks released per clinical indication

### Original request
"Please can you pull the data showing the breakdown of output of the new filtering system per clinical indication (per panel) since the implementation in April 24. By output I mean: how many samples in total, and within these, how many had no variant to review (SNV and CNV, understanding that CNV no workbook was implemented later) with no workbook generated vs. how many had variants to review and a workbook generated.

e.g. R208, 500 tests, 200 with no workbook, 300 with a workbook"

### Inputs
The script `get_workbook_release_per_clin_ind.py` takes inputs:
- `--start_date (str)`: The date the new filtering was introduced (format %Y-%m-%d, e.g. 2024-04-09).
- `--process_change (str)`: The date the CNV release process change was introduced, where we no longer release CNV workbooks for samples with no CNVs (format %Y-%m-%d, e.g. 2024-07-10).
- `--ignore_files (file)`: A txt file with any files to ignore (one DNAnexus file ID per line)
- `--outfile_name (str)`: The name of the output file.

### How it works
The script:
- Searches for all 002 CEN/TWE DNAnexus projects since `--start_date`
- Within the projects found above, finds all SNV (`*SNV*.xlsx`) workbooks and CNV (`*CNV*.xlsx`) workbooks
- For each of these workbooks, gets the DNAnexus 'details' which contains the clinical indication and the number of variants contained within the report
- Writes this information to a Pandas dataframe, removes any files we've told it to ignore via `--ignore_files`
- Merges all reports found for one sample and clinical indication into one row
- Adds columns `SNV_report_released` and `CNV_report_released` to indicate whether a report was released (taking into account `--process_change`) and based on this, creates another column `any_report_released` if at least one of these columns are 'Yes'
- Groups by `clinical_indication` and counts how many samples for each clinical indication and within this, how many with no SNV or CNV report released (`no_workbook_released`) and how many had a SNV or CNV report released (or both)
- Writes the final grouped dataframe to an Excel named by `--outfile_name`.

### Output
The output of the script is an Excel file with one sheet (`by_workbook_release`) showing clinical indications, total samples and within this, number with a workbook released / not released. There is also another sheet (`by_variants`) which has a similar breakdown but showing clinical indications, total samples and within this, number with any variants / no variants.