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

def find_dx_projects(start_date):
    """
    Find 002 CEN/TWE projects made after certain date

    Parameters
    ----------
    start_date : str
        date to filter projects created after e.g. '2024-04-09'

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
        created_after=start_date
    ))

    return projects

def find_reports(project_id, report_type):
    """
    Find xlsx reports in the DX project given. This searches the /output
    folder as one project (project-GkX7Qf84ByB0F0bpzxy4FZ57) has a folder
    where CNV was re-run on request with a failed sample and put in another
    folder

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

def get_details_in_parallel(list_of_files) -> list:
    """
    Call dx.dxFile().get_details() in parallel for given list of files.

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


def main():
    """Main function"""
    args = parse_args()
    # Find all 002 Dias projects since new filtering introduced
    # start_date = '2024-04-10'
    projects_002 = find_dx_projects(args.start_date)
    print(f"Found {len(projects_002)} 002 projects since {args.start_date}")

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

    # Add in details of included variants and clinical indication for all
    # reports in parallel
    reports_with_details = get_details_in_parallel(all_reports)

    # Make df of all reports (multiple rows per sample, one for each report)
    variants_df = pd.DataFrame(reports_with_details)

    # Ignore certain files as they're ad hoc requests (e.g. Sticklers) or
    # reports run for testing of dias_reports_bulk_reanalysis
    with open(args.ignore_files, mode ='r', encoding='utf8') as file_with_ids:
        file_ignore_list = [line.rstrip('\n') for line in file_with_ids]

    files_ignore = '\n\t'.join([file for file in file_ignore_list])
    print(f"Ignoring following files:\n\t{files_ignore}")

    # Remove rows from df if the file ID is in the row
    variants_df_snv_files_removed = variants_df[
        ~variants_df.snv_file_id.isin(file_ignore_list)
    ]
    variants_df_all_ignored_removed = variants_df_snv_files_removed[
        ~variants_df_snv_files_removed.cnv_file_id.isin(file_ignore_list)
    ]


    # Group by run, sample and clinical indication so we end up with one row
    # per sample (and can see which have SNV+CNV or just SNV)
    grouped_df = variants_df_all_ignored_removed.groupby(
        ['run', 'sample', 'clinical_indication']
    ).agg({
        'type': lambda x: ','.join(x),
        'snv_file_id': 'first',
        'snv_included_variants': 'first',
        'cnv_file_id': 'first',
        'cnv_included_variants': 'first'
    }).reset_index()

    # Add run date so we can work out whether CNV report was released due to
    # artemis update, and convert to date type
    grouped_df['run_date'] = grouped_df['run'].apply(lambda s:s.split('_')[1])
    grouped_df['run_date'] = pd.to_datetime(
        grouped_df['run_date'], format="%y%m%d"
    ).dt.date

    # Add column to say whether SNV report was released (if > 0 variants)
    grouped_df['SNV_report_released'] = np.where(
        grouped_df['snv_included_variants'] > 0,
        'Yes', 'No'
    )

    # Set conditions for whether CNV report was released based on when the run
    # was released and whether there were CNVs identified
    cnv_process_change = datetime.datetime.strptime(
        args.process_change, '%Y-%m-%d'
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
    # based on conditions
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
    sorted_any_workbook['R_code'] = sorted_any_workbook[
        'clinical_indication'
    ].apply(lambda s:s.split('_')[0])
    sorted_any_workbook = sorted_any_workbook[[
        'R_code', 'clinical_indication', 'total_samples',
        'no_workbook_released', 'workbook_released'
    ]]

    writer = pd.ExcelWriter(args.outfile_name)
    sorted_any_workbook.to_excel(
        writer, sheet_name='by_workbook_release', index=False
    )
    for column in sorted_any_workbook:
        column_length = max(
            sorted_any_workbook[column].astype(str).map(len).max(),
            len(column)
        )
        col_idx = sorted_any_workbook.columns.get_loc(column)
        writer.sheets['by_workbook_release'].set_column(
            col_idx, col_idx, column_length
        )

    writer.save()


if __name__ == '__main__':
    main()
