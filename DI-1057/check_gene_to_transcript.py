"""Check that all genes in the new genepanels file are present in the list of
genes that mapped to a clinical transcript in the current (prod) g2t file.
"""
import pandas as pd
import argparse
import dxpy

def read_dxfile(file_id, headers):
    """
    Reads a TSV file and returns a list of rows.
    
    """
    with dxpy.open_dxfile(file_id) as dx_file:
        df = pd.read_csv(
            dx_file,
            delimiter='\t',
            header=None,
            names=headers
        )
    return df

def check_genes_in_g2t(genepanels, gene_to_transcript):
    """Checks if the genes in the genepanels are present in
    gene_to_transcript DataFrame

    Args:
        genepanels (pd.DataFrame): df containing the new genepanels
        gene_to_transcript (pd.DataFrame): df containing current g2t
    """
    # subset g2t to only clinical transcript
    g2t_genes = gene_to_transcript[
        gene_to_transcript.transcript_type == "clinical_transcript"
    ].genes.tolist()
    
    genepanels["is_in_g2t"] = genepanels.genes.apply(
        lambda x: True if x in g2t_genes else False
    )
    
    genes_not_in_g2t = genepanels[~genepanels["is_in_g2t"]]
    
    if genes_not_in_g2t.empty:
        print("All genes in new genepanels file are present in g2t file")
    else:
        print("The following genepanels are missing from g2t:")
        print(genes_not_in_g2t)
        

def main():
    """Entry point."""
    parser = argparse.ArgumentParser(
        description="Check genes in g2t"
    )
    parser.add_argument(
        '--genepanels', required=True,
        help="File ID of genepanels file."
    )
    parser.add_argument(
        '--g2t', required=True,
        help="File ID of g2t file."
    )
    args = parser.parse_args()
    
    headers_genepanels = ["testId", "panelName", "genes", "panelId"]
    headers_g2t = ["genes", "transcript", "transcript_type", "canonical"]
    
    genepanels = read_dxfile(args.genepanels, headers_genepanels)
    g2t = read_dxfile(args.g2t, headers_g2t)
    
    check_genes_in_g2t(genepanels, g2t)
    
    
if __name__ == "__main__":
    main()
