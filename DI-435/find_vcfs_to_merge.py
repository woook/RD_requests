import argparse
import dxpy
import pandas as pd
import re

from collections import Counter


def parse_args() -> argparse.Namespace:
    """
    Parse command line arguments

    Returns
    -------
    args : Namespace
        Namespace of passed command line argument inputs
    """
    parser = argparse.ArgumentParser(
        description="Information required to find CEN VCFs"
    )

    parser.add_argument(
        "-a",
        "--assay",
        type=str,
        required=True,
        help="DNAnexus project suffix to search for",
    )

    parser.add_argument(
        "-s",
        "--start",
        type=str,
        help="Start date to search for projects",
    )

    parser.add_argument(
        "-e",
        "--end",
        type=str,
        help="End date to search for projects",
    )

    parser.add_argument(
        "-o",
        "--outfile_name",
        type=str,
        required=True,
        help="Name of the output file",
    )

    return parser.parse_args()


def find_projects(project_name, start=None, end=None):
    """
    Find DNANexus projects by name

    Parameters
    ----------
    project_name : str
        project name
    start : str (optional)
        start date to look for projects from
    end: str (optional)
        end date to look for projects until

    Returns
    -------
    projects : list
        list of DNAnexus projects
    """
    projects = list(
        dxpy.find_projects(
            name=project_name,
            created_before=end,
            created_after=start,
            name_mode="glob",
            describe=True
        )
    )

    return projects


def find_data(file_name, project_id):
    """
    Find files in DNAnexus project

    Parameters
    ----------
    file_name : str
        file name to search for
    project_id : str
        DX project ID

    Returns
    -------
    files : list
        list of files found in project
    """
    files = list(
        dxpy.find_data_objects(
            name=file_name,
            name_mode="glob",
            project=project_id,
            describe=True
        )
    )
    return files


def read_in_qc_file_to_df(qc_file, b37_proj):
    """
    Read in QC status file to a pandas dataframe

    Parameters
    ----------
    qc_file : dict
        dict with info about a qc status file in DNAnexus
    b37_proj : dict
        dict with info about the b37 project it's present in

    Returns
    -------
    qc_df : pd.DataFrame
        the QC status file read in as a dataframe
    """
    file = dxpy.open_dxfile(
        qc_file["id"], project=b37_proj["id"], mode='rb'
    )
    try:
        file_contents = file.read()
        params = {
            "engine": "openpyxl",
            "usecols": range(8),
            "names": [
                "Sample",
                "M Reads Mapped",
                "Contamination (S)",
                "% Target Bases 20X",
                "% Aligned",
                "Insert Size",
                "QC_status",
                "Reason",
            ],
        }
        try:
            qc_df = pd.read_excel(file_contents, **params)
        # One QC status file weirdly has two sheets so read in from the second
        except ValueError:
            qc_df = pd.read_excel(
                file_contents, sheet_name="Sheet2", **params
            )
    except dxpy.exceptions.InvalidState as e:
        print(
            f"Trying to access {qc_file['id']} {e}"
            "\nNow requesting unarchiving"
        )
        file_object = dxpy.DXFile(
            qc_file["id"], project=b37_proj["id"]
        )
        file_object.unarchive()
        return

    return qc_df


def get_qc_files(b38_projects):
    """
    Find QC status files and create list of dataframes for each

    Parameters
    ----------
    b38_projects : list
        list of dicts, each representing a DNAnexus project

    Returns
    -------
    qc_file_dfs : list
        list of dfs, each representing a QC status file
    """
    qc_file_dfs = []
    for b38_proj in b38_projects:
        folder_002 = (
            b38_proj["describe"]["name"]
            .rsplit("_", maxsplit=1)[0]
            .split("_", maxsplit=1)[1]
        )
        run_name = f"002_{folder_002}*"
        b37_project = find_projects(run_name)

        for b37_proj in b37_project:
            qc_files = find_data("*QC*.xlsx", b37_proj["describe"]["id"])
            if len(qc_files) > 1:
                print(
                    f"\n{len(qc_files)} QC files found in {b37_proj['id']}. "
                    "Taking latest QC status file"
                )
                qc_file = max(qc_files, key=lambda x:x['describe']['created'])
            else:
                qc_file = qc_files[0]
            qc_df = read_in_qc_file_to_df(qc_file, b37_proj)
            qc_file_dfs.append(qc_df)

    print(f"Read in {len(qc_file_dfs)} QC status files")

    return qc_file_dfs


def get_failed_samples(qc_status_df):
    """
    Get any failed samples

    Parameters
    ----------
    qc_status_df : df
        pandas df which is a merge of all QC status files

    Returns
    -------
    fail_sample_names : list
        list of sample names which have failed
    """
    df_fail = qc_status_df.loc[
        qc_status_df['QC_status'].str.upper() == 'FAIL'
    ]
    fail_samples = list(df_fail['Sample'])
    fail_sample_names = list(set([
        sample.split("-")[0] + "-" + sample.split("-")[1]
        for sample in fail_samples
    ]))

    return fail_sample_names


def get_sample_types(projects):
    """
    Get validation and non-validation samples

    Parameters
    ----------
    projects : list
        _description_

    Returns
    -------
    _type_
        _description_
    """
    all_validation_sample = []
    all_non_validation_sample = []

    for project in projects:
        vcf_files = find_data(
            "*_markdup_recalibrated_Haplotyper.vcf.gz",
            project["describe"]["id"]
        )
        non_validation_samples_in_run = []

        for vcf in vcf_files:
            instrument_id = vcf["describe"]["name"].split("-")[0]
            sample_id = vcf["describe"]["name"].split("-")[1]
            file_id = vcf["describe"]["id"]

            if (
                re.match(r"^\d{9}$", instrument_id)
                or re.match(r"^[X]\d{6}$", instrument_id)
            ) and (
                re.match(r"^[G][M]\d{7}$", sample_id)
                or re.match(r"^\d{5}[R]\d{4}$", sample_id)
            ):
                all_non_validation_sample.append(
                    {
                        "sample": instrument_id + "-" + sample_id,
                        "project": project["describe"]["id"],
                        "file_id": file_id
                    }
                )
                non_validation_samples_in_run.append(
                    instrument_id + "-" + sample_id
                )

            else:
                all_validation_sample.append(
                    {
                        "sample": instrument_id + "-" + sample_id,
                        "project": project["describe"]["id"],
                        "file_id": file_id
                    }
                )

        if (
            len(non_validation_samples_in_run)
            != len(list(set(non_validation_samples_in_run)))
        ):
            print("Sample duplication in the same run", project['id'])

    return all_non_validation_sample, all_validation_sample


def main():
    args = parse_args()
    # Find projects for our assay
    b38_projects = find_projects(args.assay, args.start, args.end)
    print("Number of projects found:", len(b38_projects))
    projects_to_print='\n\t'.join([
        f"{x['describe']['name']} - {x['id']}" for x in b38_projects
    ])
    print(f"\nProjects:\n\t{projects_to_print}")

    # Get QC status files and read them in
    qc_files = get_qc_files(b38_projects)
    merged_qc_df = pd.concat(qc_files)

    # Check failed samples from QC status reports
    fail_sample_names = get_failed_samples(merged_qc_df)
    print("\nFailed samples:")
    print("\n".join(sample for sample in fail_sample_names))

    # Get validation and duplicated samples in 38 folders
    (
        all_non_validation_sample,
        validation_samples,
    ) = get_sample_types(b38_projects)

    # Check duplicated samples from all CEN38 folders
    sample_names = [item['sample'] for item in all_non_validation_sample]
    duplicated_samples = [
        item for item, count in Counter(sample_names).items()
        if count > 1
    ]
    print("\nDuplicated_samples:")
    print("\n".join(sample for sample in duplicated_samples))

    # Create df
    df_validation_samples = pd.DataFrame(validation_samples)
    df_all_non_validation_samples = pd.DataFrame(all_non_validation_sample)
    # Write out non-validation samples to CSV
    df_validation_samples.to_csv("validation_samples.csv", index=False)

    # Drop the duplicated samples and keep once (8 samples are duplicated)
    df_non_duplicated = df_all_non_validation_samples.drop_duplicates(
        subset=["sample"], keep="last"
    )
    df_non_duplicated.reset_index(drop=True, inplace=True)
    print(
        f"\n{len(df_all_non_validation_samples)} non-validation samples found"
    )
    print(
        f"{len(df_non_duplicated)} non-duplicated non-validation samples "
        "found"
    )

    # Get file list to merge if they are not failed samples (14 failed samples)
    print("Removing failed samples")
    df_file_to_merge = df_non_duplicated[
        ~df_non_duplicated['sample'].isin(fail_sample_names)
    ]
    df_file_to_merge.to_csv(args.outfile_name, sep="\t", header=False)
    print("Number of final VCF files to merge:", len(df_file_to_merge))

    # Counter check
    for i in fail_sample_names:
        if i in list(df_file_to_merge["sample"]):
            print("Failed file found")

    for i in duplicated_samples:
        if i in list(df_file_to_merge["sample"]):
            print('Duplicated sample found')


if __name__ == '__main__':
    main()
