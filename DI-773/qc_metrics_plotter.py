import dxpy
import pandas as pd
import plotly.express as px
import plotly.io as pio
import json
import sys
import argparse
pio.renderers.default = 'browser'

def parse_args():
    """
    Parse arguments given at cmd line.

    Args: None

    Returns:
        - args (Namespace): object containing parsed arguments.
    """

    parser = argparse.ArgumentParser(
        description='Gather and/or plot QC data.'
    )

    parser.add_argument(
        '-c', '--config',
        help="Filepath for configuration JSON file",
        type=str,
        required=True
    )

    parser.add_argument(
        '-r', '--runmode',
        type=str,
        help='Runmode',
        choices=['gather_and_plot', 'plot_only'],
        required=True
    )

    args = parser.parse_args()

    return args


def get_projects(
    search,
    number_of_projects=None,
    after_date=None,
    before_date=None,
    search_mode="regexp"
):
    """
    Find projects within specified data range using specified search mode
    """

    projects = list(dxpy.bindings.search.find_projects(
        name=search,
        name_mode=search_mode,
        created_after=after_date,
        created_before=before_date,
        describe={'fields': {'name': True}}))

    projects = sorted(projects, key=lambda x: x['describe']['name'])

    if number_of_projects is not None:
        projects = projects[-int(number_of_projects):]

    return projects


def read2df(file_id, project):
    """Read in tsv file to df"""
    file = dxpy.bindings.dxfile_functions.open_dxfile(
        file_id,
        project=project["id"],
        mode="r",
    )
    df = pd.read_csv(file, sep="\t")
    df['run'] = project['describe']['name']
    return df


def get_files(projects, config):
    """
    Retrieve and read in QC files into dfs using the search terms specified in
    the config.

    Parameters:
        - List of dxpy derived dictionary objects containing the project IDs
          and names
    Raises:
        - RuntimeError if dxpy file search returns 0 files
          file
    Returns:
        - Nested dict object containing a list of dfs for each metric file
          (one per run)
    """

    print(f"Number of projects: {len(projects)}")
    dfs_dict = {}
    for key in config["file"].keys():
        dfs_dict[key] = {"dfs": []}

    for proj in projects:
        for key in config["file"].keys():
            project_id = proj["id"]

            if key != "qc_status":
                # Find files in project using search term specified in config
                search_results = list(dxpy.bindings.search.find_data_objects(
                    classname="file",
                    name=config["file"][key]["pattern"],
                    name_mode="regexp",
                    project=project_id))

            else:
                project_name_b37 = proj['describe']['name'][4:-6]
                search_term_b37 = f"002_{project_name_b37}_{config['project_search']['assay']}"
                projects_b37 = get_projects(
                    search=search_term_b37,
                    search_mode="exact"
                )

                if len(projects_b37) != 1:
                    raise RuntimeError(f"Error finding GRCh37 project found for {search_term_b37}")

                project_id = projects_b37[0]['id']

                search_results = list(dxpy.bindings.search.find_data_objects(
                    classname="file",
                    name=config["file"][key]["pattern"],
                    name_mode="regexp",
                    project=project_id))

            if len(search_results) > 0:
                search_result = search_results[0]

                if len(search_results) > 1:
                    # Print warning that more than one file was found, but
                    # first file in search result will be selected
                    print(
                        f"More than one file found for "
                        f"{config['file'][key]['pattern']} in {project_id}:\n"
                        f"{[result['id'] for result in search_results]}\n"
                        f"Using {search_results[0]['id']}"
                    )

                if key != "qc_status":
                    # Select first search result
                    dfs_dict[key]["dfs"].append(
                        read2df(search_result["id"], proj)
                    )

                else:
                    # QC status xlsx's could not be accessed via dxpy as there
                    # is an encoding error / unreadable byte and are therefore
                    # downloaded locally first before being accessed
                    filename = search_result["id"] + ".xlsx"

                    try:
                        dxpy.bindings.dxfile_functions.download_dxfile(
                            dxid=search_result["id"],
                            filename=filename
                        )
                    except dxpy.exceptions.InvalidState as e:
                        print(
                            f"Trying to download {search_result['id']} {e}"
                            "\nNow requesting unarchiving"
                        )
                        print()
                        file_object = dxpy.DXFile(search_result["id"], project=project_id)
                        file_object.unarchive()
                        continue

                    df = pd.read_excel(
                        filename,
                        engine="openpyxl",
                        usecols=range(8),
                        names=[
                            'Sample', 'M Reads Mapped',
                            'Contamination (S)', '% Target Bases 20X',
                            '% Aligned', 'Insert Size', 'QC_status', 'Reason'
                        ]
                    )
                    df['run'] = proj['describe']['name']
                    dfs_dict[key]["dfs"].append(df)

    return dfs_dict


def make_plot(
        df: pd.DataFrame,
        col_name: str,
        assay: str,
        y_range_low: float = None,
        y_range_high: float = None,
        plot_failed: bool = True,
        warning_line: float = None,
        fail_line: float = None,
        plot_std: bool = True

):
    """
    Generate plotly box plot.

    Parameters:
        df (pd.Dataframe) - df containing data to plot
        col_name (str) - name of column you wish to plot on the y axis
        y_range_low (float) - min value for y axis
        y_range_high (float) - max value for y axis
        plot_failed (bool) - boolean for plotting values from failed samples
        warning_line (float) - y axis value along which a warning line is drawn
        fail_line (float) - y axis value along which a fail line is drawn
        plot_std (bool) - boolean for plotting std lines

    Outputs:
        View of plotly plot in browser window
        .html of the plotly plot named after the col_name input.
    """
    passed_df = df[(df['QC_status'] == 'PASS') | (df['QC_status'] == 'WARNING')].sort_values('run')

    fig = px.box(
        passed_df,
        x='run',
        y=col_name,
        hover_data={
            'run': True,
            'Sample': True,
            col_name: True
        }
    )
    fig.update_xaxes(tickangle=45)
    fig.update_layout(
        title_text=f"{col_name} values from selected {assay} runs",
        title_x=0.5)
    fig.add_hline(
        y=passed_df[col_name].mean(),
        line_color='green',
        annotation_text=f"<b>Mean: {passed_df[col_name].mean():.5f} <br> STD:{passed_df[col_name].std():.5f}</b>",
        annotation_position="right"
    )
    if plot_std:
        fig.add_hline(
            y=passed_df[col_name].mean()+passed_df[col_name].std(),
            line_dash='dot',
            annotation_text=f"<b>+Std: {passed_df[col_name].mean()+passed_df[col_name].std():.5f}</b>",
            annotation_position="right"
        )
        fig.add_hline(
            y=passed_df[col_name].mean()-passed_df[col_name].std(),
            line_dash='dot',
            annotation_text=f"<b>-Std: {passed_df[col_name].mean()-passed_df[col_name].std():.5f}</b>",
            annotation_position="right"
        )
    if plot_failed:
        failed_df = df[df['QC_status'] == "FAIL"].sort_values('run')
        fig.add_scatter(
            x=failed_df['run'],
            y=failed_df[col_name],
            mode="markers",
            hoverinfo='text',
            text=failed_df['Sample'] + "<br>" + failed_df[col_name].astype(str) + "<br>" + failed_df['Reason'],
            name="Failed samples"
        )

    if (y_range_low is None) != (y_range_high is None):
        raise ValueError("Please provide both high/low values for Y range values")
    elif y_range_low is not None and y_range_high is not None:
        fig.update_yaxes(
            range=[y_range_low, y_range_high]
        )

    if warning_line is not None:
        fig.add_hline(
            y=warning_line,
            line_color="orange",
            annotation_text="<b>Warning</b>",
            annotation_position="right"
        )

    if fail_line is not None:
        fig.add_hline(
            y=fail_line,
            line_color="red",
            annotation_text="<b>Fail</b>",
            annotation_position="right"
        )
    fig.show()
    fig.write_html(f"{col_name}.html")


def main():
    args = parse_args()

    with open(args.config, encoding="UTF-8") as f:
        config = json.load(f)

    if args.runmode == "gather_and_plot":

        projects = get_projects(
            search=config['project_search']['pattern'],
            number_of_projects=config['project_search']['number_of_projects'],
            after_date=config['project_search']["after_date"],
            before_date=config['project_search']["before_date"],
            search_mode=config['project_search']["mode"]
        )

        dfs_dict = get_files(projects, config)

        # Merge dfs for each QC file
        dfs_dict = {
            key: pd.concat(dfs_dict[key]["dfs"], ignore_index=True) for key in dfs_dict.keys()
        }

        # output merged qc_status .xlsx's to .tsv
        qc_df = dfs_dict['qc_status']
        qc_df.to_csv('merged_qc_status.tsv', sep='\t')

        for key in dfs_dict.keys():
            if key != 'qc_status':
                # add reason and pass/fail columns to merged dfs
                final_df = pd.merge(
                    dfs_dict[key],
                    qc_df[['Sample', 'QC_status', 'Reason']],
                    on='Sample'
                )
                # write merged dataframes out to tsv files for future reference/use
                final_df.to_csv(f"{key}.tsv", sep='\t', index=False)

                # make plots as specified in config
                for plot_config in config["file"][key]["plots"]:
                    make_plot(
                        df=final_df,
                        col_name=plot_config["col_name"],
                        assay=config["project_search"]["assay"],
                        y_range_low=plot_config["y_range_low"],
                        y_range_high=plot_config["y_range_high"],
                        plot_failed=plot_config["plot_failed"],
                        warning_line=plot_config["warning_line"],
                        fail_line=plot_config["fail_line"],
                        plot_std=plot_config["plot_std"]
                    )

    elif args.runmode == "plot_only":
        for key in config["file"].keys():
            plot_file = key + ".tsv"
            for plot_config in config["file"][key]["plots"]:
                make_plot(
                        df=pd.read_csv(plot_file, sep="\t"),
                        col_name=plot_config["col_name"],
                        assay=config["project_search"]["assay"],
                        y_range_low=plot_config["y_range_low"],
                        y_range_high=plot_config["y_range_high"],
                        plot_failed=plot_config["plot_failed"],
                        warning_line=plot_config["warning_line"],
                        fail_line=plot_config["fail_line"],
                        plot_std=plot_config["plot_std"]
                    )


if __name__ == "__main__":
    main()
