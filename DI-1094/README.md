## CEN/WES GRCh38 egg_sex_check thresholds - August 2024

### Original request
Find a suitable eggd_sex_check thresholds for CEN/WES GRCh38 using data from the 20 most recent runs.

### Inputs
The main script `plot_sex_check_thresholds.py` takes inputs:
- `--samples (str)`: Path to the dias_b38_samples.csv file, which contains sample information for the 20 most recent CEN and TWE runs.
- `--somalier (str)`: Path to the b38_somalier_report.csv file, which contains the somalier predictions for the input samples.
- `--sex_check_table (str)`: Path to the multiqc_sex_check_table_[CEN|TWE].txt, which contains the results from `eggd_sex_check` app for the specified assay (CEN or TWE).
- `--assay (str)`: Specifies the assay type (CEN or TWE) for which the plots should be generated. Choices are "CEN" or "TWE".
- `--calculate_threshold (bool)`: If set, the script will calculate the thresholds using the standard deviation method. If not set, manual thresholds must be provided.
- `--male_threshold (float)`: The male threshold to use if `--calculate_threshold` is not set. This value must be provided manually if the threshold calculation option is not used.
- `--female_threshold (float)`: The female threshold to use if `--calculate_threshold` is not set. This value must also be provided manually if the threshold calculation option is not used.

### Running
Example command:
```python plot_sex_check_thresholds.py --samples dias_b38_samples.csv --somalier b38_somalier_report.csv --sex_check_table multiqc_sex_check_table_TWE.txt --assay TWE --male_threshold 4.3 --female_threshold 5.1
```

### How it works
The script:
- `get_somalier_pred.py`: Retrieves the somalier predictions for the input samples and writes the b38_somalier_report.csv file.
- `run_eggd_sex_check.py`: Runs eggd_sex_check jobs for all samples from the 20 most recent CEN and TWE runs.
- `run_multiqc.sh`: Collates output from eggd_sex_check jobs into multiqc tables.
- `plot_sex_check_thresholds.py`: Plots the distribution of sex check scores of the input samples.

### Output

The output of the main script plot_sex_check_thresholds.py consists of two types of HTML plots:

- `distribution_of_scores_{assay}.html`: A histogram distribution of sex check scores grouped by reported sex of samples.

- `sex_check_thresholds_{assay}.html`: A scatter plot visualising score trends across different runs and highlighted thresholds.
