## Create PanelApp JSON file

### Description
This script is used to create a PanelApp JSON file which is used as an input to the [eggd_optimised_filtering](https://github.com/eastgenomics/eggd_optimised_filtering) app to annotate the MOI of a gene in the context of a panel.

### Inputs
The script `create_panelapp_json.py` takes inputs:
- `--genepanels (str)`: genepanels file (optional)
- `--extra_panels (str)`: Any extra panel IDs to retain, comma separated (optional)
- `--outfile_name (str)`: Name of output PanelApp JSON file

### Running
Example command:
`python create_panelapp_json.py --genepanels 241024_genepanels.tsv --extra_panels 1570 --outfile_name 241030_panelapp_dump.json`

### How it works
The script:
- Queries all PanelApp panels using the panelapp library.
- If provided, retains only panels with an ID present in the input `--genepanels` file, plus any IDs given as `--extra_panels`
- Formats these panels as dictionaries and adds to list
- Checks for any duplicate genes or regions (by gene symbol/region name) within a panel
  - If only the MOI is different between any duplicates, retains the first instance with the MOI replaced to 'Other'.

### Output
The output of the script is a JSON file named by `--outfile_name`, containing an array of panels.