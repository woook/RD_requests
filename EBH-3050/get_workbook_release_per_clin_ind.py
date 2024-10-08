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
            "Date to filter projects created after in %Y-%M-%d format e.g. "
            "2024-04-09"
        )
    )

    parser.add_argument(
        '-e',
        '--end_date',
        required=True,
        type=str,
        help=(
            "Date to filter projects created before in %Y-%M-%d format e.g. "
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
            "%Y-%M-%d format) e.g. 2024-07-10"
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

    print(f"Found {len(projects)} 002 projects\n")

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
                "name": True,
                "createdBy": True
            }
        }
    ))

    return reports


def get_cnv_excluded_regions(cnv_report):
    """
    Get the CNV excluded regions file based on the job which made the CNV
    .xlsx report

    Parameters
    ----------
    cnv_report : dict
        dictionary with info about a CNV report

    Returns
    -------
    excluded_regions_id : str
        file ID of the excluded regions file for that sample
    """
    cnv_workbooks_job = cnv_report["describe"]["createdBy"]["job"]

    excluded_regions_id = dx.DXJob(
        dxid=cnv_workbooks_job
    ).describe()["input"]["additional_files"][0]["$dnanexus_link"]

    return excluded_regions_id


def read_excluded_regions_to_df(file_id, project_id):
    """
    Read in excluded regions file to a pandas dataframe

    Parameters
    ----------
    file_id : str
        DNAnexus file ID of excluded regions file
    project_id : str
        DNAnexus project ID

    Returns:
        pd.DataFrame: file read in as pandas dataframe (or None if there are
        no excluded regions)
    """

    file = dx.open_dxfile(
        file_id,
        project=project_id,
        mode="r",
    )

    excluded_df = pd.read_csv(file, sep="\t", header=0)

    if excluded_df.empty:
        return None

    return excluded_df


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
                'project_id': project['id'],
                'sample': sample_name,
                'snv_file_id': snv_report['id'],
                'type': 'SNV',
            })

        # Find CNV reports in project and save info about them
        cnv_reports = find_reports(project['id'], 'CNV')
        print(f"{len(cnv_reports)} CNV reports found")
        for cnv_report in cnv_reports:
            excluded_regions_file = get_cnv_excluded_regions(cnv_report)
            sample_name = "-".join(
                cnv_report['describe']['name'].split("-", 2)[:2]
            )
            all_reports.append({
                'run': project['describe']['name'],
                'project_id': project['id'],
                'sample': sample_name,
                'cnv_file_id': cnv_report['id'],
                'excluded_regions_id': excluded_regions_file,
                'type': 'CNV',
            })

    return all_reports


def get_details_and_read_excluded_regions_in_parallel(list_of_files) -> list:
    """
    Call get_details() and read in excluded regions in parallel for given list
    of files

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
        project_id = file_dict['project_id']

        # If SNV, the variants are in the DX report details under 'included'
        if file_type == 'SNV':
            file_id = file_dict['snv_file_id']
            details = dx.DXFile(dxid=file_id, project=project_id).get_details()
            included_variants = details.get('included')
            file_dict['snv_included_variants'] = included_variants

        # If CNV, the variants are in the DX report details under 'variants'
        elif file_type == 'CNV':
            file_id = file_dict['cnv_file_id']
            excluded_regions_id = file_dict['excluded_regions_id']
            details = dx.DXFile(dxid=file_id, project=project_id).get_details()
            excluded_regions_df = read_excluded_regions_to_df(
                excluded_regions_id, project_id
            )
            included_variants = details.get('variants')
            file_dict['cnv_included_variants'] = included_variants
            file_dict['excluded_regions_df'] = excluded_regions_df

        clinical_indication = details.get('clinical_indication')
        file_dict['clinical_indication'] = clinical_indication

        return file_dict

    results = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
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


def remove_ignore_files(reports_with_details, ignore_txt_file):
    """
    Remove any files (by ID) if they are in the input txt file containing
    files to ignore

    Parameters
    ----------
    reports_with_details : list
        list of dictionaries, each with info on a report
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

    samples_with_files_removed = [
        report for report in reports_with_details
        if not (
            report.get('cnv_file_id') in file_ignore_list
            or report.get('snv_file_id') in file_ignore_list
        )
    ]

    return samples_with_files_removed


def group_by_sample_and_add_run_date(reports_df):
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
        'cnv_included_variants': 'first',
        'excluded_regions_id': 'first',
        'excluded_regions_df': 'first'
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

    grouped_df['excluded_regions_df'] = (
        grouped_df['excluded_regions_df'].astype(str)
    )
    grouped_df['CNV_excluded_regions'] = np.where(
        grouped_df['excluded_regions_df'] == "None", "No", "Yes"
    )

    return grouped_df


def subset_raw_data(raw_data):
    """
    Subset the raw data to remove columns not needed

    Parameters
    ----------
    raw_data : pd.DataFrame
        df of all data

    Returns
    -------
    raw_data_subset :  pd.DataFrame
        df with only the columns needed
    """
    # Subset to remove file ID and run date columns
    raw_data = raw_data[[
        'run', 'sample', 'clinical_indication', 'type',
        'snv_included_variants', 'cnv_included_variants',
        'excluded_regions_df', 'SNV_report_released', 'CNV_report_released',
        'any_report_released', 'CNV_excluded_regions'
    ]]

    raw_data = raw_data.sort_values(
        by=['clinical_indication'], ignore_index=True
    )

    return raw_data


def group_and_count_by_workbook_type_release(report_release_df):
    """
    Count how many had an SNV report released/not released and how many
    had a CNV report released/not released per clinical indication

    Parameters
    ----------
    report_release_df : pd.DataFrame
        dataframe with columns 'SNV_report_released',
        'CNV_report_released' and 'any_report_released' which are Yes or No

    Returns
    -------
    sorted_grouped_by_each_release : pd.DataFrame
        df with counts of each workbook released per clinical indication
    """
    # Group and count how many have 0 SNVs+CNVs and how many have >0 SNVs+CNVs
    grouped_by_each_release = report_release_df.groupby('clinical_indication').agg(
        total_samples=('sample', 'size'),
        snv_released=('SNV_report_released', lambda x: (x == 'Yes').sum()),
        snv_not_released=('SNV_report_released', lambda x: (x == 'No').sum()),
        cnv_released=('CNV_report_released', lambda x: (x == 'Yes').sum()),
        cnv_not_released=('CNV_report_released', lambda x: (x == 'No').sum()),
    ).reset_index()

    sorted_grouped_by_each_release = grouped_by_each_release.sort_values(
        by=['total_samples'], ascending=False, ignore_index=True
    )

    return sorted_grouped_by_each_release


def group_and_count_by_variant_existence_per_type(report_release_df):
    """
    Count how many had any SNVs and how many had any CNVs per clinical
    indication

    Parameters
    ----------
    report_release_df : pd.DataFrame
        df with columns with number of variants per sample -
        'snv_included_variants' and 'cnv_included_variants'

    Returns
    -------
    sorted_grouped_by_variant_type : pd.DataFrame
        df with counts of each variant type per clinical indication
    """
    conditions = [
        (
            ((report_release_df['cnv_included_variants'] == 0)
            | (report_release_df['cnv_included_variants'].isna()))
            & (report_release_df['CNV_excluded_regions'] == 'No')
        ),
        (
            ((report_release_df['cnv_included_variants'] == 0)
            | (report_release_df['cnv_included_variants'].isna()))
            & (report_release_df['CNV_excluded_regions'] == 'Yes')
        ),
        (
            ((report_release_df['cnv_included_variants'] != 0)
            | (~report_release_df['cnv_included_variants'].isna()))
            & (report_release_df['CNV_excluded_regions'] == 'No')
        ),
        (
            ((report_release_df['cnv_included_variants'] != 0)
            | (~report_release_df['cnv_included_variants'].isna()))
            & (report_release_df['CNV_excluded_regions'] == 'Yes')
        )
    ]

    values = [
        "no_cnvs_no_excluded", "no_cnvs_has_excluded",
        "has_cnvs_no_excluded", "has_cnvs_has_excluded"
    ]

    report_release_df['cnv_status'] = np.select(conditions, values)

    no_snv_condition = (
        (report_release_df['snv_included_variants'] == 0)
        | (report_release_df['snv_included_variants'].isna())
    )

    grouped_by_each_variant_type = report_release_df.groupby(
        'clinical_indication'
    ).agg(
        total_samples=('sample', 'size'),
        no_snvs=(
            'snv_included_variants',
            lambda x: no_snv_condition[x.index].sum()
        ),
        has_snvs=(
            'snv_included_variants',
            lambda x: (~no_snv_condition)[x.index].sum()
        ),
        no_cnvs_no_excluded=(
            'cnv_status', lambda x: (x == 'no_cnvs_no_excluded').sum()
        ),
        no_cnvs_has_excluded=(
            'cnv_status', lambda x: (x == 'no_cnvs_has_excluded').sum()
        ),
        has_cnvs_no_excluded=(
            'cnv_status', lambda x: (x == 'has_cnvs_no_excluded').sum()
        ),
        has_cnvs_has_excluded=(
            'cnv_status', lambda x: (x == 'has_cnvs_has_excluded').sum()
        ),
    ).reset_index()

    sorted_grouped_by_variant_type = grouped_by_each_variant_type.sort_values(
        by=['total_samples'], ascending=False, ignore_index=True
    )

    return sorted_grouped_by_variant_type


def create_df_of_just_excluded_regions(reports_list):
    """
    Create df of the excluded regions, with one row per excluded region,
    each associated with a sample tested for a clinical indication

    Parameters
    ----------
    reports_with_ignore_removed : list
        list of dicts, each with info on a report

    Returns
    -------
    excluded_exists_subset : pd.DataFrame
        df of samples with excluded regions
    """
    # Create new nested key with the df as dict because it's much easier
    # to work with
    for report in reports_list:
        regions_df = report.get('excluded_regions_df')
        if regions_df is not None:
            if not regions_df.empty:
                regions_dict = regions_df.to_dict('list')
                report['excluded_regions_dict'] = regions_dict
            else:
                report['excluded_regions_dict'] = None
        else:
            report['excluded_regions_dict'] = None

    # Create df of only excluded regions
    rows = []
    for report in reports_list:
        regions_df = report.get('excluded_regions_df')
        excluded_regions_dict = report['excluded_regions_dict']
        nested_df = pd.DataFrame(excluded_regions_dict)
        nested_df['sample'] = report['sample']
        nested_df['clinical_indication'] = report['clinical_indication']
        rows.append(nested_df)
    result_df = pd.concat(rows, ignore_index=True)

    # Create new column with all info about the excluded region in one cell
    result_df[['Start', 'End', 'Length', 'Exon']] = result_df[
        ['Start', 'End', 'Length', 'Exon']
    ].astype(int)
    result_df['excluded_region'] = result_df[result_df.columns[2:]].apply(
        lambda x: ' '.join(x.dropna().astype(str)),
        axis=1
    )
    excluded_exists_subset = result_df[
        ['sample', 'clinical_indication', 'excluded_region']
    ]

    return excluded_exists_subset


def find_commonly_excluded_regions(excluded_regions_df):
    """
    Work out how commonly a region is excluded in each panel

    Parameters
    ----------
    excluded_regions_df : pd.DataFrame
        df of excluded regions, one per row

    Returns
    -------
    merged_excluded : pd.DataFrame
        df with counts of samples with excluded regions per clinical
        indication and counts of unique regions excluded
    """
    # count how many samples total per clinical indication
    total_samples = excluded_regions_df.groupby(
        'clinical_indication'
    )['sample'].nunique().reset_index()
    total_samples.columns = [
        'clinical_indication', 'total_samples_with_excluded'
    ]

    # Count how many times excluded regions occur per panel
    excluded_counts = excluded_regions_df.groupby(
        ['clinical_indication', 'excluded_region']
    ).agg('nunique').reset_index()
    excluded_counts.columns = [
        'clinical_indication', 'excluded_region', 'region_excluded_count'
    ]

    # Merge (because pandas does not seem to work properly with nunique and
    # doing other aggregations at the same time)
    merged_excluded = pd.merge(
        total_samples, excluded_counts, on='clinical_indication', how='outer'
    )
    merged_excluded['proportion_of_panel_tests_excluded'] = (
        merged_excluded['region_excluded_count']
        / merged_excluded['total_samples_with_excluded']
    )

    merged_excluded['excluded_in_all_tests_for_panel'] = np.where(
        merged_excluded['proportion_of_panel_tests_excluded'] == 1,
        "Yes", "No"
    )

    merged_excluded = merged_excluded.set_index(
        ['clinical_indication', 'total_samples_with_excluded']
    )

    return merged_excluded

def write_out_excel(dataframes_sheets, outfile_name, write_index):
    """
    Write out pandas dfs to sheets of Excel file

    Parameters
    ----------
    dataframes_sheets : list
        list of tuples with pandas df and sheet name
    outfile_name: str
        name of Excel file to write out
    write_index : bool
        whether to write the index
    """
    with pd.ExcelWriter(outfile_name) as writer:
        for dataframe, sheet_name in dataframes_sheets:
            dataframe.to_excel(
                writer, sheet_name=sheet_name, index=write_index
            )
            # Automatically set column widths to fit content
            for column in dataframe:
                column_length = max(
                    dataframe[column].astype(str).map(len).max(),
                    len(column)
                )
                col_idx = dataframe.columns.get_loc(column)
                writer.sheets[sheet_name].set_column(col_idx, col_idx, column_length)


def main():
    """Main function"""
    args = parse_args()
    # Find all 002 Dias projects since new filtering introduced
    projects_002 = find_dx_projects(args.start_date, args.end_date)

    # Find SNV and then CNV reports in all of those projects and store
    # as list of dictionaries, each with info about a report
    all_reports = get_reports(projects_002)

    # Add in details of included variants and clinical indication for all
    # reports in parallel
    reports_with_details = get_details_and_read_excluded_regions_in_parallel(
        all_reports
    )

    # Ignore certain files as they're ad hoc requests (e.g. Sticklers) or
    # reports run for testing of dias_reports_bulk_reanalysis
    reports_with_ignore_removed = remove_ignore_files(
        reports_with_details, args.ignore_files
    )

    # Make df of all reports (multiple rows per sample, one for each report)
    variants_df = pd.DataFrame(reports_with_ignore_removed)

    # Group by run, sample and clinical indication so we end up with one row
    # per sample (and can see which have SNV+CNV or just SNV)
    grouped_df = group_by_sample_and_add_run_date(
        variants_df
    )

    # Set conditions for whether CNV report was released based on when the run
    # was released and whether there were CNVs identified
    report_release_df = determine_whether_any_report_released(
        grouped_df, args.process_change
    )

    raw_data_df = subset_raw_data(report_release_df)

    # Create df grouped by clinical indication with counts of samples
    # and how many workbooks of each type released
    report_type_release = group_and_count_by_workbook_type_release(
        report_release_df
    )

    # Create df grouped by clinical indication with counts of samples and
    # how many had variants of each type
    variant_type_release = group_and_count_by_variant_existence_per_type(
        report_release_df
    )

    # Write out Excel with two sheets, grouped by release of each
    # workbook type and grouped by existence of each variant type
    write_out_excel(
        dataframes_sheets=[
            (variant_type_release, "by_variant_type_plus_excluded"),
            (raw_data_df, "raw_data")
        ],
        outfile_name=args.outfile_name,
        write_index=False
    )

    excluded_regions_df = create_df_of_just_excluded_regions(
        reports_with_ignore_removed
    )

    excluded_regions_count = find_commonly_excluded_regions(
        excluded_regions_df
    )
    write_out_excel(
        dataframes_sheets=[
            (excluded_regions_count, 'excluded_regions_count')
        ],
        outfile_name='EBH_3050_commonly_excluded_regions.xlsx',
        write_index=True
    )


if __name__ == '__main__':
    main()
