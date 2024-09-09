import argparse
import concurrent.futures
import datetime
import dxpy as dx
import numpy as np
import pandas as pd


def parse_args() -> argparse.Namespace:
    """
    Parse command line arguments

    Returns
    -------
    args : Namespace
        Namespace of passed command line argument inputs
    """
    parser = argparse.ArgumentParser(
        description='Information required to find workbooks'
    )

    parser.add_argument(
        '-s',
        '--start_date',
        required=True,
        type=str,
        help=(
            "Date to filter projects created after in %y%m%d format e.g. "
            "2024-04-09"
        )
    )

    parser.add_argument(
        '-e',
        '--end_date',
        required=True,
        type=str,
        help=(
            "Date to filter projects created before in %y%m%d format e.g. "
            "2024-09-04"
        )
    )

    parser.add_argument(
        '-p',
        '--process_change',
        required=True,
        type=str,
        help=(
            "Date that change to the CNV release process was introduced (in "
            "%y%m%d format) e.g. 2024-07-10"
        )
    )

    parser.add_argument(
        '-i',
        '--ignore_files',
        required=True,
        type=str,
        help="Path to file containing file IDs to ignore"
    )

    parser.add_argument(
        '-o',
        '--outfile_name',
        required=True,
        type=str,
        help='Name of the output Excel file'
    )

    return parser.parse_args()

def find_dx_projects(start_date, end_date):
    """
    Find 002 CEN/TWE projects between certain dates

    Parameters
    ----------
    start_date : str
        date to filter projects created after e.g. '2024-04-09'
    end_date : str
        date to filter projects created before e.g. '2024-09-04'

    Returns
    -------
    projects : list
        list of dicts, each with info about a DX project
    """
    projects = list(dx.find_projects(
        name="^002.*(TWE|CEN)$",
        name_mode="regexp",
        level="VIEW",
        describe={
            "fields": {
                "id": True,
                "name": True
            }
        },
        created_after=start_date,
        created_before=end_date
    ))

    return projects

def find_reports(project_id, report_type):
    """
    Find xlsx reports in the DX project given. This searches the /output
    folder as one project (project-GkX7Qf84ByB0F0bpzxy4FZ57) has a folder
    where CNV was re-run on request with a failed sample and put in a
    different folder

    Parameters
    ----------
    project_id : str
        DX project ID
    report_type : str
        'SNV' or 'CNV'

    Returns
    -------
    reports : list
        list of dicts, each representing a report in the project
    """
    reports = list(dx.find_data_objects(
        name=f"*{report_type}*xlsx",
        name_mode="glob",
        project=project_id,
        folder='/output',
        describe={
            "fields": {
                "id": True,
                "name": True
            }
        }
    ))

    return reports


def get_reports(projects_002):
    """
    Get all reports in all 002 projects and put them in a big list

    Parameters
    ----------
    projects_002 : list
        list of dicts, each with info about a DX project

    Returns
    -------
    all_reports : list
        list of dicts, each representing a SNV or CNV report
    """
        # Find SNV and then CNV reports in all of those projects and store
    # as list of dictionaries, each with info about a report
    all_reports = []
    for project in projects_002:
        print(project['describe']['name'])

        # Find SNV reports in project and save info about them
        snv_reports = find_reports(project['id'], 'SNV')
        print(f"{len(snv_reports)} SNV reports found")
        for snv_report in snv_reports:
            sample_name = "-".join(
                snv_report['describe']['name'].split("-", 2)[:2]
            )
            all_reports.append({
                'run': project['describe']['name'],
                'sample': sample_name,
                'snv_file_id': snv_report['id'],
                'type': 'SNV',
            })

        # Find CNV reports in project and save info about them
        cnv_reports = find_reports(project['id'], 'CNV')
        print(f"{len(cnv_reports)} CNV reports found")
        for cnv_report in cnv_reports:
            sample_name = "-".join(
                cnv_report['describe']['name'].split("-", 2)[:2]
            )
            all_reports.append({
                'run': project['describe']['name'],
                'sample': sample_name,
                'cnv_file_id': cnv_report['id'],
                'type': 'CNV',
            })

        print("")

    return all_reports


def get_details_in_parallel(list_of_files) -> list:
    """
    Call dx.dxFile().get_details() in parallel for given list of files

    Parameters
    ----------
    list_of_files : list
        list of dicts, each with a file ID and type

    Returns
    -------
    results : list
        list of dicts, each with a file ID and type with extra details about
        the variants included (filtered in) and clinical indication tested
    """
    def _get_details(file_dict):
        """
        Get details of a file in DNAnexus

        Parameters
        ----------
        file_dict : dict
            A dictionary representing info on one SNV or CNV report

        Returns
        -------
        file_dict : dict
            the same dict with extra keys for the included variants and
            clinical indication
        """
        file_type = file_dict['type']

        # If SNV, the variants are in the DX report details under 'included'
        if file_type =='SNV':
            file_id = file_dict['snv_file_id']
            details = dx.DXFile(file_id).get_details()
            included_variants = details.get('included')
            file_dict['snv_included_variants'] = included_variants

        # If CNV, the variants are in the DX report details under 'variants'
        elif file_type == 'CNV':
            file_id = file_dict['cnv_file_id']
            details = dx.DXFile(file_id).get_details()
            included_variants = details.get('variants')
            file_dict['cnv_included_variants'] = included_variants

        clinical_indication = details.get('clinical_indication')
        file_dict['clinical_indication'] = clinical_indication

        return file_dict

    results = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=32) as executor:
        concurrent_jobs = {
            executor.submit(_get_details, item): item for item in list_of_files
        }
        for future in concurrent.futures.as_completed(concurrent_jobs):
            # access returned output as each is returned in any order
            try:
                results.append(future.result())
            except Exception as exc:
                # catch any errors that might get raised during querying
                print(
                    f"Error getting data for {future}: {exc}"
                )
                raise exc

    return results


def remove_ignore_files(sample_df, ignore_txt_file):
    """
    Remove any files (by ID) if they are in the input txt file containing
    files to ignore

    Parameters
    ----------
    sample_df : pd.DataFrame
        dataframe with all SNV/CNV reports found
    ignore_txt_file : str
        name of txt file with file IDs to ignore

    Returns
    -------
    variants_df_all_ignored_removed : df
        df with rows removed if the file ID is in the ignore list
    """
    # Ignore certain files as they're ad hoc requests (e.g. Sticklers) or
    # reports run for testing of dias_reports_bulk_reanalysis
    with open(ignore_txt_file, mode ='r', encoding='utf8') as file_with_ids:
        file_ignore_list = [line.rstrip('\n') for line in file_with_ids]

    files_ignore = '\n\t'.join([file for file in file_ignore_list])
    print(f"Ignoring following files:\n\t{files_ignore}")

    # Remove rows from df if the file ID is in the SNV file ID column
    variants_df_snv_files_removed = sample_df[
        ~sample_df.snv_file_id.isin(file_ignore_list)
    ]
    # Remove rows from df if the file ID is in the CNV file ID column
    variants_df_all_ignored_removed = variants_df_snv_files_removed[
        ~variants_df_snv_files_removed.cnv_file_id.isin(file_ignore_list)
    ]

    return variants_df_all_ignored_removed


def group_by_test_and_add_run_date(reports_df):
    """
    Group by run, sample and clinical indication

    Parameters
    ----------
    reports_df : pd.DataFrame
        dataframe with one row per report found

    Returns
    -------
    grouped_df : pd.DataFrame
        dataframe with one row per sample with info on SNV/CNV reports found
    """
    # Group by run, sample and clinical indication so we end up with one row
    # per sample (and can see which have SNV+CNV or just SNV)
    grouped_df = reports_df.groupby(
        ['run', 'sample', 'clinical_indication']
    ).agg({
        'type': lambda x: ','.join(x),
        'snv_file_id': 'first',
        'snv_included_variants': 'first',
        'cnv_file_id': 'first',
        'cnv_included_variants': 'first'
    }).reset_index()

    # Add run date so later we can work out whether CNV report was released
    # due to artemis update, and convert to date type
    grouped_df['run_date'] = grouped_df['run'].apply(lambda s:s.split('_')[1])
    grouped_df['run_date'] = pd.to_datetime(
        grouped_df['run_date'], format="%y%m%d"
    ).dt.date

    return grouped_df


def determine_whether_any_report_released(grouped_df, process_change_date):
    """
    Create new columns with info on whether any report was released

    Parameters
    ----------
    grouped_df : pd.DataFrame
        df containing info on each test and variants found
    process_change_date : str
        date that the artemis release process changed so CNV reports were
        no longer released if had 0 variants

    Returns
    -------
    grouped_df : pd.DataFrame
        dataframe with extra columns 'SNV_report_released',
        'CNV_report_released' and 'any_report_released'
    """
    # Add column to say whether SNV report was released (yes if > 0 variants
    # otherwise no)
    grouped_df['SNV_report_released'] = np.where(
        grouped_df['snv_included_variants'] > 0,
        'Yes', 'No'
    )

    # Set conditions for whether CNV report was released based on when the run
    # was released and whether there were CNVs identified
    cnv_process_change = datetime.datetime.strptime(
        process_change_date, '%Y-%m-%d'
    ).date()

    conditions = [
        (   # Before new artemis and CNV report exists
            (grouped_df['run_date'] < cnv_process_change)
            & (grouped_df['cnv_included_variants'].notna())
        ),
        (   # Before new artemis and CNV report does not exist
            (grouped_df['run_date'] < cnv_process_change)
            & (grouped_df['cnv_included_variants'].isna())
        ),
        (   # After new artemis and CNV report has variants so is released
            (grouped_df['run_date'] > cnv_process_change)
            & (grouped_df['cnv_included_variants'] > 0)
        ),
        (   # After new artemis and CNV report has no variants so not released
            (grouped_df['run_date'] > cnv_process_change)
            & (grouped_df['cnv_included_variants'] == 0)
        ),
        (   # After new artemis and CNV report does not exist
            (grouped_df['run_date'] > cnv_process_change)
            & (grouped_df['cnv_included_variants'].isna())
        ),
    ]

    values = ["Yes", "No", "Yes", "No", "No"]
    # Add column to say whether CNV report was released
    grouped_df['CNV_report_released'] = np.select(conditions, values)


    # Add extra column to say whether any report was released for the sample
    second_conditions = [
        (
            (grouped_df['SNV_report_released'] == 'Yes')
            & (grouped_df['CNV_report_released'] == 'Yes')
        ),
        (
            (grouped_df['SNV_report_released'] == 'Yes')
            & (grouped_df['CNV_report_released'] == 'No')
        ),
        (
            (grouped_df['SNV_report_released'] == 'No')
            & (grouped_df['CNV_report_released'] == 'Yes')
        ),
        (
            (grouped_df['SNV_report_released'] == 'No')
            & (grouped_df['CNV_report_released'] == 'No')
        )
    ]
    second_values = ["Yes", "Yes", "Yes", "No"]

    grouped_df['any_report_released'] = np.select(
        second_conditions, second_values
    )

    return grouped_df


def group_by_report_release(grouped_df):
    """
    Group by clinical indication and count how many samples, how many had no
    report released and how many had at least one report released

    Parameters
    ----------
    grouped_df : pd.DataFrame
        df with columns 'SNV_report_released', 'CNV_report_released' and
        'any_report_released'

    Returns
    -------
    sorted_any_workbook : pd.DataFrame
        df with info on how many samples had no report released per clinical
        indication
    """

    # Group by clinical indication and count how many samples and of these,
    # how many had no workbook and how many had at least one workbook released
    grouped_any_workbook = grouped_df.groupby('clinical_indication').agg(
        total_samples=('sample', 'size'),
        no_workbook_released=('any_report_released', lambda x: (x == 'No').sum()),
        workbook_released=('any_report_released', lambda x: (x == 'Yes').sum())
    ).reset_index()

    # Sort by total samples and add short R code
    sorted_any_workbook = grouped_any_workbook.sort_values(
        by=['total_samples'], ascending=False, ignore_index=True
    )

    # # Add in column with short R code
    # sorted_any_workbook['R_code'] = sorted_any_workbook[
    #     'clinical_indication'
    # ].apply(lambda s:s.split('_')[0])

    sorted_any_workbook = sorted_any_workbook[[
        'clinical_indication', 'total_samples',
        'no_workbook_released', 'workbook_released'
    ]]

    return sorted_any_workbook


def group_by_any_variants(grouped_df):
    """
    Group by clinical indication and count how many had no variants in SNV or
    CNV or both

    Parameters
    ----------
    grouped_df : pd.DataFrame
        df with columns 'snv_included_variants' and 'cnv_included_variants'

    Returns
    -------
    sorted_any_variants : pd.DataFrame
        df with info on how many samples had no variants in SNV, CNV or both
        per clinical indication
    """
    grouped_df['variants_sum'] = grouped_df[
        ['snv_included_variants', 'cnv_included_variants']
    ].sum(axis=1)

    # Group and count how many have 0 SNVs+CNVs and how many have >0 SNVs+CNVs
    grouped_by_any_variants = grouped_df.groupby('clinical_indication').agg(
        total_samples=('sample', 'size'),
        no_variants=('variants_sum', lambda x: (x == 0).sum()),
        any_variants=('variants_sum', lambda x: (x > 0).sum())
    ).reset_index()

    # Sort by total samples
    sorted_any_variants = grouped_by_any_variants.sort_values(
        by=['total_samples'], ascending=False, ignore_index=True
    )

    sorted_any_variants = sorted_any_variants[[
        'clinical_indication', 'total_samples', 'no_variants', 'any_variants'
    ]]

    return sorted_any_variants


def write_out_intermediate_excel(grouped_df):
    """
    Write out the Excel with the raw data

    Parameters
    ----------
    grouped_df : pd.DataFrame
        pandas df with all info on each test
    """
    # Subset to remove file ID columns
    grouped_df = grouped_df[[
        'run', 'run_date', 'sample', 'clinical_indication', 'type',
        'snv_included_variants', 'cnv_included_variants',
        'SNV_report_released', 'CNV_report_released', 'any_report_released'
    ]]

    writer = pd.ExcelWriter('EBH-3050_raw_data.xlsx')
    grouped_df.to_excel(
        writer, sheet_name='all_data', index=False
    )
    # Automatically set column widths to fit content
    for column in grouped_df:
        column_length = max(
            grouped_df[column].astype(str).map(len).max(),
            len(column)
        )
        col_idx = grouped_df.columns.get_loc(column)
        writer.sheets['all_data'].set_column(
            col_idx, col_idx, column_length
        )

    writer.save()


def write_out_final_excel(
        by_workbook_df, by_variants_df, sheet_1, sheet_2, outfile_name
    ):
    """
    Write out pandas dfs to sheets of Excel file

    Parameters
    ----------
    by_workbook_df : pd.DataFrame
        pandas df to write out
    outfile_name : str
        name of Excel file to write out
    """
    writer = pd.ExcelWriter(outfile_name)
    by_workbook_df.to_excel(
        writer, sheet_name=sheet_1, index=False
    )
    # Automatically set column widths to fit content
    for column in by_workbook_df:
        column_length = max(
            by_workbook_df[column].astype(str).map(len).max(),
            len(column)
        )
        col_idx = by_workbook_df.columns.get_loc(column)
        writer.sheets[sheet_1].set_column(
            col_idx, col_idx, column_length
        )

    by_variants_df.to_excel(
        writer, sheet_name=sheet_2, index=False
    )
    for column in by_variants_df:
        column_length = max(
            by_variants_df[column].astype(str).map(len).max(),
            len(column)
        )
        col_idx = by_variants_df.columns.get_loc(column)
        writer.sheets[sheet_2].set_column(
            col_idx, col_idx, column_length
        )

    writer.save()


def main():
    """Main function"""
    args = parse_args()
    # Find all 002 Dias projects since new filtering introduced
    # start_date = '2024-04-10'
    projects_002 = find_dx_projects(args.start_date, args.end_date)
    print(f"Found {len(projects_002)} 002 projects since {args.start_date}")

    # Find SNV and then CNV reports in all of those projects and store
    # as list of dictionaries, each with info about a report
    all_reports = get_reports(projects_002)

    # Add in details of included variants and clinical indication for all
    # reports in parallel
    reports_with_details = get_details_in_parallel(all_reports)

    # Make df of all reports (multiple rows per sample, one for each report)
    variants_df = pd.DataFrame(reports_with_details)

    # Ignore certain files as they're ad hoc requests (e.g. Sticklers) or
    # reports run for testing of dias_reports_bulk_reanalysis
    variants_df_all_ignored_removed = remove_ignore_files(
        variants_df, args.ignore_files
    )

    # Group by run, sample and clinical indication so we end up with one row
    # per sample (and can see which have SNV+CNV or just SNV)
    grouped_df = group_by_test_and_add_run_date(
        variants_df_all_ignored_removed
    )

    # Set conditions for whether CNV report was released based on when the run
    # was released and whether there were CNVs identified
    report_release_df = determine_whether_any_report_released(
        grouped_df, args.process_change
    )

    # Write out intermediate Excel with all data
    write_out_intermediate_excel(report_release_df)

    # Create df grouped by clinical indication with counts of samples and
    # how many with workbook released
    sorted_any_workbook = group_by_report_release(report_release_df)

    # Create df grouped by clinical indication with counts of samples and
    # how many with any variants
    sorted_any_variants = group_by_any_variants(report_release_df)

    # Write out final Excel with two sheets, grouped by workbook release
    # and grouped by any variants
    write_out_final_excel(
        sorted_any_workbook, sorted_any_variants, 'by_workbook_release',
        'by_variants', args.outfile_name
    )


if __name__ == '__main__':
    main()
