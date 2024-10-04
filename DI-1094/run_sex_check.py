"""Runs eggd_sex_check for dias b38 samples from recent runs
"""

import sys
import subprocess
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import dxpy


def get_project_ids(pattern: str) -> list:
    """
    Retrieve the 20 most recent project ids matching pattern
    
    Args:
        pattern (str): The pattern used to match project names.

    Returns:
        list: A list of the 20 most recent project IDs.
    """
    res = list(
        dxpy.find_projects(
            level="VIEW", name=pattern, name_mode="glob", describe=True
        )
    )

    df = pd.DataFrame(
        [
            {
                "project": x["id"],
                "name": x["describe"]["name"],
            }
            for x in res
        ]
    )

    df["date"] = df.name.apply(lambda x: x.split("_")[1])
    df["date"] = pd.to_datetime(df["date"], format="%y%m%d")

    df = df.sort_values(by="date", ascending=False).head(20)

    return list(df.project.values)


def find_files(project_id: str, pattern: str) -> pd.DataFrame:
    """
    find file_names that matches pattern in a given project
    
    Args:
        project_id (str): The project ID to search within.
        pattern (str): The file name pattern to search for.

    Returns:
        pd.DataFrame: DataFrame containing file IDs and metadata
    """
    res = list(
        dxpy.find_data_objects(
            project=project_id,
            folder="/output/",
            recurse=True,
            name=f"*{pattern}",
            classname="file",
            name_mode="glob",
            describe={
                "fields": {
                    "name": True, "modified": True, "archivalState": True
                }
            },
        )
    )

    if not res:
        print(f"No file matching {pattern} found in {project_id}")
        return

    print(f"Found {len(res)} matches in {project_id}")

    df = pd.DataFrame(
        [
            {
                "file_id": x["id"],
                "project_id": x["project"],
                "samples": x["describe"]["name"].rstrip(pattern),
                "archival_state": x["describe"]["archivalState"],
            }
            for x in res
        ]
    )

    print("Checking for duplicated samples")
    duplicate_mask = df.duplicated(subset="samples", keep=False)
    if duplicate_mask.any():
        duplicates = df.loc[duplicate_mask, "samples"].unique()
        print("Duplicated samples:", duplicates)

        print("Dropping duplicates but last occurrence")
        df = df.drop_duplicates(subset="samples", keep="last")
    else:
        print("No duplicate found")

    return df


def unarchive_files(df: pd.DataFrame) -> pd.DataFrame | None:
    """
    Check the archival state of files and unarchive them if necessary.
    
    Args:
        df (pd.DataFrame): DataFrame containing files to unarchive

    Returns:
        pd.DataFrame | None: Returns the DataFrame if all files are live;
        otherwise, exits after unarchiving files.
    """
    df_to_unarchive = df[df.state.isin(["archived", "archival"])]
    if df_to_unarchive.empty:
        print("No files to unarchive.")
        if (df["state"] == "live").all():
            print("All files are live.")
            return df
        else:
            print("Files are still unarchiving. Exiting!")
            sys.exit()

    for project_id, files in df_to_unarchive.groupby("project_id")["files"]:
        response = dxpy.api.project_unarchive(
            project_id, {"files": list(files)}
        )
        print(response)

    print(f"Unarchive request sent for {len(df_to_unarchive)} files.")
    print("Please rerun script after a few hours. Exiting!")
    sys.exit()


def get_files() -> pd.DataFrame:
    """
    Find BAM and index files in dias b38 projects and unarchive them if needed.
    
    Returns:
        pd.DataFrame: DataFrame of BAM and index files.
    """
    # Retrieve project IDs for both CEN38 and TWE38 projects
    cen_projects = get_project_ids("*_CEN38")
    twe_projects = get_project_ids("*_TWE38")
    projects = cen_projects + twe_projects

    print(
        f"Using {len(cen_projects)} CEN and {len(twe_projects)} TWE projects"
    )

    # Find BAM and index files in the projects
    df_bam = pd.concat(
        [find_files(project_id, "_markdup.bam") for project_id in projects],
        ignore_index=True,
    )
    df_index = pd.concat(
        [find_files(project_id, "_markdup.bam.bai") for project_id in projects],
        ignore_index=True,
    )

    # Merge the BAM and index DataFrames on 'samples' and 'project_id'
    df_merged = pd.merge(
        df_bam,
        df_index,
        on=["samples", "project_id"],
        suffixes=("_bam", "_index"),
        how="inner",
    )

    # Check archival state and prepare files for unarchiving
    df = pd.concat(
        [
            df_merged[
                ["project_id", "file_id_bam", "archival_state_bam"]
                ].assign(
                files=df_merged["file_id_bam"],
                state=df_merged["archival_state_bam"]
            ),
            df_merged[
                ["project_id", "file_id_index", "archival_state_index"]
                ].assign(
                files=df_merged["file_id_index"],
                state=df_merged["archival_state_index"],
            ),
        ],
        ignore_index=True,
    )

    # Unarchive files if necessary
    unarchive_files(df)

    return df_merged


def process_input_files(df: pd.DataFrame) -> pd.DataFrame:
    """
    Fetch project names and add metadata columns (run, assay, date).
    
    Args:
        df (pd.DataFrame): DataFrame containing sample and project ids.

    Returns:
        pd.DataFrame: DataFrame with added meta columns; run, assay, and date.
    """
    # Fetch project names concurrently
    with ThreadPoolExecutor(max_workers=32) as executor:
        df["project_name"] = list(
            executor.map(lambda x: dxpy.DXProject(x).name, df["project_id"])
        )

    # Extract metadata: 'run', 'assay', and 'date'
    df["run"] = df["project_name"].apply(lambda x: "_".join(x.split("_")[2:5]))
    df["assay"] = df["project_name"].apply(lambda x: x.split("_")[-1])
    df["date"] = pd.to_datetime(
        df["project_name"].apply(lambda x: x.split("_")[1]), format="%y%m%d"
    )
    df = df.sort_values(by="date", ascending=False)

    return df


def run_eggd_sex_check(df: pd.DataFrame) -> None:
    """
    Run eggd_sex_check for each sample in df.
    
    Args:
        df (pd.DataFrame): DataFrame containing input samples
    """
    for _, row in df.iterrows():
        project_id = row["project_id"]
        assay = row["assay"]
        bam_file = row["file_id_bam"]
        index_file = row["file_id_index"]

        # Set thresholds based on assay type
        # Thresholds chosen arbitrary
        male_threshold = 4.40 if assay == "CEN38" else 4.05
        female_threshold = 5.02 if assay == "CEN38" else 5.05

        # Construct dx run command
        dx_command = [
            "dx",
            "run",
            "eggd_sex_check/1.1.0",
            f"-iinput_bam={project_id}:{bam_file}",
            f"-iindex_file={project_id}:{index_file}",
            f"-imale_threshold={male_threshold}",
            f"-ifemale_threshold={female_threshold}",
            f'--destination=/output/{assay}/{row["project_name"]}',
            f"--name={assay}_threshold",
            "-y",
        ]

        # Execute dx command
        subprocess.run(dx_command, check=True)


def write_inputs_to_disk(df: pd.DataFrame, file_name: str) -> None:
    """
    Writes a DataFrame to a CSV file.
    
    Args:
        df (pd.DataFrame): DataFrame to write to file.
        file_name (str): Name of the output CSV file.
    """
    # Select relevant columns and save to CSV
    cols = [
        "samples",
        "project_id",
        "run",
        "date",
        "assay",
        "file_id_bam",
        "file_id_index",
    ]
    df = df[cols]
    df.to_csv(file_name, index=False)


def main():
    """entry point to run jobs"""
    # Fetch files and unarchive if necessary
    df = get_files()

    # Process input files to get project metadata (run, assay, date)
    df = process_input_files(df)

    run_eggd_sex_check(df)

    write_inputs_to_disk(df, "dias_b38_samples.csv")
    print("DONE")


if __name__ == "__main__":
    main()
