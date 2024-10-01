#!/bin/bash

mkdir CEN && cd CEN

dx find data --name "*_mqc.json" --path /output/CEN38 --brief | xargs -n1 -P100 -I{} bash -c "dx download {}"

# Run multiqc in the CEN directory
multiqc .

mv multiqc_data/multiqc_sex_check_table.txt multiqc_data/multiqc_sex_check_table_CEN38.txt
dx upload multiqc_data/multiqc_sex_check_table_CEN38.txt --brief

cd ..
mkdir TWE && cd TWE
dx find data --name "*_mqc.json" --path /output/TWE38 --brief | xargs -n1 -P100 -I{} bash -c "dx download {}"

# Run multiqc in the TWE directory
multiqc .

mv multiqc_data/multiqc_sex_check_table.txt multiqc_data/multiqc_sex_check_table_TWE38.txt
dx upload multiqc_data/multiqc_sex_check_table_TWE38.txt --brief

echo "DONE"
