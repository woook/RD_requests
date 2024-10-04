## Gather and plot QC metrics

### Original request
Collect and plot QC metric values for each PASS/FAIL metric we use for latest 20 GRCh38 re-runs of CEN/TWE:

| Metric | File | Column(s) |
| :---:   | :---: | :---: |
| M Reads Mapped | `multiqc_samtools_flagstat.txt` | mapped_passed |
| Contamination | `multiqc_verifybamid.txt` | FREEMIX |
| Target Bases 20x | `multiqc_picard_HsMetrics.txt` | PCT_TARGET_BASES_20X |
| Mean Het Ratio | `multiqc_het-hom_table.txt` | mean het ratio |
| Fold 80 Base Penalty | `multiqc_picard_HsMetrics.txt` | FOLD_80_BASE_PENALTY |
| Fold Enrichment | `multiqc_picard_HsMetrics.txt` | FOLD_ENRICHMENT |
| Happy | `<control-sample-id>.summary.csv` | METRIC.Recall, METRIC.Precision |

### Inputs
The script `qc_metrics_plotter.py` takes inputs:
- `-c --config`: Filepath of JSON config with parameters for searching and plotting QC metrics
- `-r --runmode`: String (either `gather_and_plot` or `plot_only`) determining whether to find and output merged QC metric .tsv's and plot (gather_and_plot) or use existing locally available .tsvs (plot_only)

### Running
Example command:
`python3 'qc_metrics_plotter.py' -c qc_threshold_config_cen.json -r gather_and_plot`

### How it works
The script:
1. Searches for relevant GRCh38 projects according to search parameters defined in the "project_search" section of the config.
2. Within the projects found above, finds all desired QC files as specified in the "file" section of the config and reads the files in as pandas dataframes.
3. Data is merged for each file type across all the selected projects into a single dataframe per file type.
4. QC status information derived from the corresponding GRCh37 project is appended to the merged metrics dataframes to aid plotting.
5. The merged metrics dataframes with QC information are outputted as .tsv's for future use.
6. Data is plotted in a .html plot which is saved and opens in a browser window. Parameters for plotting are specified in the "plots" section of the config.

### Runmodes
- `gather_and_plot` - runs steps 1 to 6
- `plot_only` - runs steps 5 to 6 (requires existing merged metrics .tsv's)

### Output
- TSVs containing merged QC data across all the selected projects
- .html plots which were plotted according to the "plots" section of the config