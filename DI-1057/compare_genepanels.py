import pandas as pd
import argparse
import dxpy

def read_gene_panel(file_id: str):
    """
    Reads a genepanels file file and returns a pandas DataFrame
    with specified headers.

    Args:
        file_id (str): File if of genepanels file

    Returns:
        pd.DataFrame: DataFrame with cols:'TestID', 'PanelName', 'HGNCID', and
        'PanelID' plus additional computed columns 'Rcode' and 'genepanel'.
    """
    # Define the headers manually
    headers = [
        "TestID_CI",
        "PanelName_V",
        "HGNCID",
        "PanelID"
    ]
    
    with dxpy.open_dxfile(file_id) as dx_file:

        df = pd.read_csv(
            dx_file,
            delimiter='\t',
            header=None,
            names=headers
        )
        df["Rcode"] = df["TestID_CI"].apply(lambda x: x.split("_")[0])
        df["genepanel"] = df["Rcode"] + "-" + df["HGNCID"]
    
    return df


def compare_rcodes(new_df, old_df):
    """
    Compares the 'TestID' columns between the new and old genepanels and
    prints the unique TestIDs added and removed in the new genepanels.

    Args:
        new_df (pd.DataFrame): Dataframe containing the new genepanels.
        old_df (pd.DataFrame): Dataframe containing the old genepanels.

    Returns:
        set: The set of Rcodes that are present in the new file.
    """
    new_rcodes = set(new_df["Rcode"])
    old_rcodes = set(old_df["Rcode"])

    # Rcodes added in the new file
    added_rcodes = new_rcodes - old_rcodes
    added_test_ids = new_df[new_df["Rcode"].isin(
        added_rcodes)]["TestID_CI"].unique()

    # Rcodes removed in the old file
    removed_rcodes = old_rcodes - new_rcodes
    removed_test_ids = old_df[old_df["Rcode"].isin(
        removed_rcodes)]["TestID_CI"].unique()

    print("Added TestIDs:")
    for id in added_test_ids:
        print(id)

    print("\nRemoved TestIDs:")
    for id in removed_test_ids:
        print(id)

    return new_rcodes

    
def compare_genepanels(new_df, old_df, rcodes_in_new):
    """
    Compares the 'genepanel' columns between the new and old DataFrames

    Args:
        new_df (pd.DataFrame): Dataframe containing the new genepanels.
        old_df (pd.DataFrame): DataFrame containing the old genepanels
        rcodes_in_new (set): Unique Rcodes present in the new genepanels.

    Returns:
        None
    """
    
    # Filter to only comapre the rcodes in new file
    new_filtered = new_df[new_df["Rcode"].isin(rcodes_in_new)]
    old_filtered = old_df[old_df["Rcode"].isin(rcodes_in_new)]

    new_genepanels = set(new_filtered["genepanel"])
    old_genepanels = set(old_filtered["genepanel"])
    
    added_genepanels = new_genepanels - old_genepanels
    if added_genepanels:
        print("\nAdded Genepanels:")
        print("GenePanel\tCI")
        for gp in sorted(added_genepanels):
            df = new_filtered[new_filtered["genepanel"] == gp]
            test_id = df["TestID_CI"].values[0]
            print(f"{gp} - {test_id}")
    else:
        print("\nNo genepanels were added to the new file")

    removed_genepanels = old_genepanels - new_genepanels
    if removed_genepanels:
        print("\nRemoved Genepanels:")
        print("GenePanel\tCI")
        for gp in sorted(removed_genepanels):
            df = old_filtered[old_filtered["genepanel"] == gp]
            test_id = df["TestID_CI"].values[0]
            print(f"{gp} - {test_id}")
    else:
        print("\nNo genepanels were removed from the old file.")

def row_wise_comparison(new_df, old_df):
    """
    compares dfs row_wise and save diff to a spreadsheet
    
    Args:
        new_df (pd.DataFrame): Dataframe containing the new genepanels.
        old_df (pd.DataFrame): Dataframe containing the old genepanels.

    Returns:
        None
    """
    
    # Set 'genepanel' as index and sort both DataFrames
    new_df = new_df.set_index("genepanel").sort_index()
    old_df = old_df.set_index("genepanel").sort_index()

    # Get the common genepanels in both DataFrames
    common_index = new_df.index.intersection(old_df.index)

    # Filter both DataFrames to keep only the common genepanels
    new_df = new_df.loc[common_index]
    old_df = old_df.loc[common_index]

    print("\n\nComaparing cols of common genepanels in both files...")
    for col in new_df.columns:
        if col == "Rcode":
            continue  # Already compared 'Rcode' earlier
        
        diff_col = new_df[col].compare(old_df[col])
        diff_col = diff_col.drop_duplicates()
        diff_col.columns = ["New", "Old"]
        
        if diff_col.empty:
            print(f"No diff in {col} column.\n")
        else:
            print(f"Diff in {col} column:")
            print(diff_col)
            print()

    # Save diff summary to a file
    output_file = "genepanels_diff.xlsx"
    diff = new_df.compare(old_df, result_names=("New", "Old"))
    
    if not diff.empty:
        diff.to_excel(output_file)
        print(f"\nSummary of diff saved to {output_file}")
    else:
        print("No differences found between the files.")


def main():
    """
    Entry point for the script.
    """
    # Setup argparse to get file IDs
    parser = argparse.ArgumentParser(
        description="Compare new and old genepanel files by file ID."
    )
    parser.add_argument(
        '--new_file_id', required=True,
        help="File ID of the new genepanels file."
    )
    parser.add_argument(
        '--old_file_id', required=True,
        help="File ID of the old genepanels file."
    )
    args = parser.parse_args()

    new_df = read_gene_panel(args.new_file_id)
    old_df = read_gene_panel(args.old_file_id)

    # Compare Rcodes and get the Rcodes present in the new file
    rcodes_in_new = compare_rcodes(new_df, old_df)

    # Compare Genepanels using the filtered Rcodes
    compare_genepanels(new_df, old_df, rcodes_in_new)

    # Compare rows of common genepanels
    row_wise_comparison(new_df, old_df)


if __name__ == "__main__":
    main()
    