import dxpy
import pandas as pd
import plotly.express as px
import plotly.io as pio
import json
import argparse

from plotly.subplots import make_subplots

pio.renderers.default = "browser"


def parse_args():
    """
    Parse arguments given at cmd line.

    Returns
    -------
    args : Namespace object
        object containing parsed arguments
    """

    parser = argparse.ArgumentParser(description="Gather and/or plot QC data")

    parser.add_argument(
        "-c",
        "--config",
        help="Filepath for configuration JSON file",
        type=str,
        required=True,
    )

    parser.add_argument(
        "-r",
        "--runmode",
        type=str,
        help="Runmode",
        choices=["gather_and_plot", "plot_only"],
        required=True,
    )

    args = parser.parse_args()

    return args


def get_projects(
    search_term,
    number_of_projects=None,
    after_date=None,
    before_date=None,
    search_mode="regexp",
):
    """
    Find projects within specified data range using specified search term and
        mode.

    Parameters
    ----------
    search_term : str
        Search term or regexp pattern used to find projects.
    number_of_projects : int (optional)
        Number of projects to use. If set, takes most recent projects based
        on the date in the project name rather than creation date.
        Defaults to None.
    after_date : str (optional)
        Select projects created after this date.
    before_date : str (optional)
        Select projects created before this date. Defaults to None.
    search_mode : str (optional)
        Type of dxpy search mode to use, acceptable search_modes "regexp",
        "glob" and "exact". Defaults to "regexp".

    Returns
    -------
    projects : list
        list of dictionaries containing information (project ID/name)
        on the selected projects
    """
    projects = list(
        dxpy.bindings.search.find_projects(
            name=search_term,
            name_mode=search_mode,
            created_after=after_date,
            created_before=before_date,
            describe={"fields": {"name": True}},
        )
    )

    projects = sorted(projects, key=lambda x: x["describe"]["name"])

    if number_of_projects is not None:
        projects = projects[-int(number_of_projects) :]

    return projects


def find_files(filename_pattern, project_id, name_mode="regexp"):
    """
    Find files in a project using the specified search term and mode.

    Parameters
    ----------
    filename_pattern : str
        Search term or regexp pattern used to find files.
    project_id : str
        DNAnexus project ID
    name_mode : str
        Type of dxpy search mode to use, acceptable search
        modes "regexp", "glob" and "exact"

    Returns
    -------
    files_found : list
        list of dictionaries containing info (file ID/name) about
        the selected files

    Raises
    ------
    AssertionError
        Raised if no files are found for the specified search term
    """
    files_found = list(
        dxpy.bindings.search.find_data_objects(
            classname="file",
            name=filename_pattern,
            name_mode=name_mode,
            project=project_id,
            describe={"fields": {"name": True}},
        )
    )

    assert (
        files_found
    ), f"No files found for {filename_pattern} in {project_id}"

    if len(files_found) > 1:
        print(
            f"More than one file found for {filename_pattern} in {project_id}"
        )

    return files_found


def read2df(
    file_id: str,
    project: dict,
    separator,
    mode,
    file_type,
    genome_build=None,
    sample_name=None,
):
    """
    Read in file to pandas df

    Parameters
    ----------
    file_id : str
        DNAnexus file ID of tsv file to be read in.
    project : dict
        Dictionary object containing info (name/ID) of the project where the
        file is located
    separator : str
        Separator used within the file
    mode : str
        the mode to open the file with using dxpy
    file_type : str
        the file type - excel, csv or tsv
    genome_build : str (optional)
        The relevant genome (GRCh37 or GRCh38)
    sample_name : str (optional)
        Name of the sample. Optional because the sample name is already
        included within most of the files were are gathering, but not within
        hap.py files

    Returns:
    df : pd.DataFrame
        pd.DataFrame object of file
    """
    # Create file obj
    file = dxpy.open_dxfile(file_id, project=project["id"], mode=mode)

    # Read in or unarchive if necessary
    try:
        if file_type in ["csv", "tsv"]:
            df = pd.read_csv(file, sep=separator)
        elif file_type == "excel":
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
                df = pd.read_excel(file_contents, **params)
            # One QC status file weirdly has two sheets so read in from the second
            except ValueError:
                df = pd.read_excel(
                    file_contents, sheet_name="Sheet2", **params
                )
    except dxpy.exceptions.InvalidState as e:
        print(f"Trying to access {file_id} {e}" "\nNow requesting unarchiving")
        file_object = dxpy.DXFile(file_id, project=project["id"])
        file_object.unarchive()
        return

    df["run"] = project["describe"]["name"]
    if genome_build:
        df["Genome"] = genome_build
    if sample_name:
        df["Sample"] = sample_name

    return df


def get_b37_project(project_b38, assay):
    """
    Get the b37 project related from the b38 project name
    Parameters
    ----------
    project_b38 : dict
        Dictionary object containing info (name/ID) of the b38 project
    assay : str
        The assay being used

    Returns
    -------
    project_b37 : dict
        Dictionary object containing info (name/ID) of the b37 project
    """
    run_name = project_b38["describe"]["name"][4:-6]
    search_term_b37 = f"002_{run_name}_{assay}"
    projects_b37 = get_projects(
        search_term=search_term_b37, search_mode="exact"
    )
    if len(projects_b37) != 1:
        raise RuntimeError(
            f"Error finding GRCh37 project found for {search_term_b37}"
        )

    return projects_b37[0]


def add_qc_metric_dfs(projects, config):
    """
    Retrieve, read in QC files into dfs using the search terms specified in
    the config and add to our dict

    Parameters
    ----------
    projects : list
        List of dxpy derived dictionary objects containing the project IDs
        and names
    config : dict
        Dictionary object containing the configuration settings for the
        files to search for

    Returns
    -------
    dfs_dict : dict
        Nested dict object containing a list of dfs for each metric file
        (one per run)
    """
    print(f"Number of projects: {len(projects)}")
    dfs_dict = {}
    for key in config["file"].keys():
        dfs_dict[key] = {"dfs": []}

    assay = config["project_search"]["assay"]
    for proj_b38 in projects:
        project_b37 = get_b37_project(proj_b38, assay)

        for key in config["file"].keys():
            if key == "happy":
                b38_happy_files = find_files(
                    filename_pattern=config["file"][key]["pattern"],
                    project_id=proj_b38["id"],
                    name_mode="regexp",
                )
                b37_happy_files = find_files(
                    filename_pattern=".*.summary.csv$",
                    name_mode="regexp",
                    project_id=project_b37["id"],
                )
                for b38_happy_file in b38_happy_files:
                    sample_name = b38_happy_file["describe"]["name"].split(
                        "."
                    )[0]

                    dfs_dict[key]["dfs"].append(
                        read2df(
                            file_id=b38_happy_file["id"],
                            project=proj_b38,
                            separator=config["file"][key]["file_sep"],
                            mode="r",
                            file_type="csv",
                            genome_build="GRCh38",
                            sample_name=sample_name,
                        )
                    )
                for b37_happy_file in b37_happy_files:
                    sample_name = b37_happy_file["describe"]["name"].split(
                        "."
                    )[0]
                    dfs_dict[key]["dfs"].append(
                        read2df(
                            file_id=b37_happy_file["id"],
                            project=project_b37,
                            separator=config["file"][key]["file_sep"],
                            mode="r",
                            file_type="csv",
                            genome_build="GRCh37",
                            sample_name=sample_name,
                        )
                    )

            elif key == "qc_status":
                search_results = find_files(
                    filename_pattern=config["file"][key]["pattern"],
                    name_mode="regexp",
                    project_id=project_b37["id"],
                )
                dfs_dict[key]["dfs"].append(
                    read2df(
                        file_id=search_results[0]["id"],
                        project=project_b37,
                        separator=config["file"][key]["file_sep"],
                        mode="rb",
                        file_type="excel",
                    )
                )

            else:
                search_results = find_files(
                    filename_pattern=config["file"][key]["pattern"],
                    name_mode="regexp",
                    project_id=proj_b38["id"],
                )

                dfs_dict[key]["dfs"].append(
                    read2df(
                        file_id=search_results[0]["id"],
                        project=proj_b38,
                        separator=config["file"][key]["file_sep"],
                        mode="r",
                        file_type="tsv",
                        genome_build="GRCh38",
                    )
                )

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
    plot_std: bool = True,
):
    """
    Generate Plotly box plot for the QC metric of interest. This is opened in
    the browser and saved as an HTML file.

    Parameters
    ----------
    df: pd.DataFrame
        Dataframe containing data to plot
    col_name: str
        Name of column being plotted on the y axis
    assay: str
        The assay being plotted
    y_range_low: float
        Min value for y axis (default None)
    y_range_high: float
        Max value for y axis (default None)
    plot_failed: bool
        Boolean for plotting values from failed samples (default True)
    warning_line: float
        y axis value(s) along which a warning line is drawn (default None)
    fail_line: float
        y axis value(s) along which a fail line is drawn
    plot_std: bool
        Boolean whether to plot std lines (default True)
    """
    passed_df = df[
        (df["QC_status"].str.strip().str.lower() == "pass")
        | (df["QC_status"].str.strip().str.lower() == "warning")
    ].sort_values("run")

    failed_df = df[
        (df["QC_status"].str.strip().str.lower() == "fail")
        | (df["QC_status"].str.strip().str.lower() == "cancelled")
    ].sort_values("run")

    n_filtered_rows = len(passed_df) + len(failed_df)
    assert n_filtered_rows == len(df), (
        "QC_Status column contains invalid values: "
        f"{df['QC_status'].unique().tolist()}"
    )

    fig = px.box(
        passed_df,
        x="run",
        y=col_name,
        hover_data={"run": True, "Sample": True, col_name: True},
    )
    fig.update_xaxes(tickangle=45)
    fig.update_layout(
        title_text=f"{col_name} values from selected {assay} runs", title_x=0.5
    )

    fig.add_hline(
        y=passed_df[col_name].mean(),
        line_color="green",
        annotation_text=(
            f"<b>Mean: {passed_df[col_name].mean():.5f} "
            f"<br>STD: {passed_df[col_name].std():.5f}</b>"
        ),
        annotation_position="right",
    )
    if plot_std:
        fig.add_hline(
            y=passed_df[col_name].mean() + passed_df[col_name].std(),
            line_dash="dot",
            annotation_text=(
                f"<b>+STD: "
                f"{passed_df[col_name].mean() + passed_df[col_name].std():.5f}"
                "</b>"
            ),
            annotation_position="right",
        )
        fig.add_hline(
            y=passed_df[col_name].mean() - passed_df[col_name].std(),
            line_dash="dot",
            annotation_text=(
                f"<b>-STD: "
                f"{passed_df[col_name].mean()-passed_df[col_name].std():.5f}"
                "</b>"
            ),
            annotation_position="right",
        )

    if plot_failed:
        fig.add_scatter(
            x=failed_df["run"],
            y=failed_df[col_name],
            mode="markers",
            hoverinfo="text",
            text=(
                failed_df["Sample"]
                + "<br>"
                + failed_df[col_name].astype(str)
                + "<br>"
                + failed_df["Reason"]
            ),
            name="Failed samples",
        )

    if (y_range_low is None) != (y_range_high is None):
        raise ValueError(
            "Please provide both high/low values for Y range values"
        )
    elif y_range_low is not None and y_range_high is not None:
        fig.update_yaxes(range=[y_range_low, y_range_high])

    if warning_line is not None:
        for line in warning_line:
            fig.add_hline(
                y=line,
                line_color="orange",
                annotation_text="<b>Warning</b>",
                annotation_position="right",
            )

    if fail_line is not None:
        for line in fail_line:
            fig.add_hline(
                y=line,
                line_color="red",
                annotation_text="<b>Fail</b>",
                annotation_position="right",
            )
    fig.show()
    fig.write_html(f"{col_name}_{assay}.html")


def make_main_happy_plot(
    happy_df,
    data_type,
    col_name_x,
    col_name_y,
):
    """
    _summary_

    Parameters
    ----------
    happy_df : pd.DataFrame
        dataframe of hap.py data
    data_type : str
        Type of data to plot ('SNP' or 'INDEL')
    col_name_x : str
        name of the column to plot on x
    col_name_y : str
        name of the column to plot on y

    Returns
    -------
    fig : plotly.graph_objs._figure.Figure obj
        Plotly figure object
    """
    data_subset = happy_df[
        (happy_df["Type"] == data_type) & (happy_df["Filter"] == "ALL")
    ]

    fig = px.scatter(
        data_subset,
        x=col_name_x,
        y=col_name_y,
        color="Genome",
        symbol="Sample",
        hover_data={
            "run": True,
            "Sample": True,
            col_name_x: True,
            col_name_y: True,
        },
    )

    return fig


def format_happy_plot(
    main_fig,
    plot_row,
    plot_column,
    col_name_x,
    col_name_y,
    assay,
    y_range_low=None,
    y_range_high=None,
    x_range_low=None,
    x_range_high=None,
    x_warning_line=None,
    x_fail_line=None,
    y_warning_line=None,
    y_fail_line=None,
):
    """
    Add formatting and warning/fail lines to each subplot of the hap.py plot

    Parameters
    ----------
    main_fig : Plotly figure object
        the whole Plotly figure (which has subplots)
    plot_row : int
        which row the subplot is to add the formatting to
    plot_column : int
        which column the subplot is to add the formatting to
    y_range_low : float, optional
        what to set the Y axis min to, default None
    y_range_high : float, optional
        what to set the Y axis max to, default None
    x_range_low : float, optional
        what to set the X axis min to, default None
    x_range_high : float, optional
        what to set the X axis max to, default None
    x_warning_line : list, optional
        list of floats of any warning line to add to the X axis, default None
    x_fail_line : list, optional
        list of floats of any fail lines to add to the X axis, default None
    y_warning_line : list, optional
        list of floats of any warning lines to add to the Y axis, default None
    y_fail_line : _type_, optional
        list of floats of any fail lines to add to the Y axis, default None

    Returns
    -------
    main_fig : plotly.graph_objs._figure.Figure obj
        Plotly figure object

    Raises
    ------
    ValueError
        If a min and max are not given for x or y ranges
    """
    if (y_range_low is not None) or (y_range_high is not None):
        if not all([y_range_low, y_range_high]):
            raise ValueError(
                "Please provide both high/low values for Y range values"
            )

    if (y_range_low is not None) and (y_range_high is not None):
        main_fig.update_yaxes(
            range=[y_range_low, y_range_high], row=plot_row, col=plot_column
        )

    if (x_range_low is not None) or (x_range_high is not None):
        if not all([x_range_low, x_range_high]):
            raise ValueError(
                "Please provide both high/low values for Y range values"
            )

    if (x_range_low is not None) and (x_range_high is not None):
        main_fig.update_xaxes(
            range=[x_range_low, x_range_high], row=plot_row, col=plot_column
        )

    # Add warning and fail lines
    if y_warning_line is not None:
        for line in y_warning_line:
            main_fig.add_hline(
                y=line,
                line_dash="dot",
                line_color="orange",
                row=plot_row,
                col=plot_column,
            )

    if y_fail_line is not None:
        for line in y_fail_line:
            main_fig.add_hline(
                y=line,
                line_dash="dot",
                line_color="red",
                row=plot_row,
                col=plot_column,
            )

    if x_warning_line is not None:
        for line in x_warning_line:
            main_fig.add_vline(
                x=line,
                line_dash="dot",
                line_color="orange",
                row=plot_row,
                col=plot_column,
            )

    if x_fail_line is not None:
        for line in x_fail_line:
            main_fig.add_vline(
                x=line,
                line_dash="dot",
                line_color="red",
                row=plot_row,
                col=plot_column,
            )

    main_fig.update_xaxes(title_text=col_name_x, row=plot_row, col=plot_column)
    main_fig.update_yaxes(title_text=col_name_y, row=plot_row, col=plot_column)
    main_fig.update_layout(
        title_text=f"hap.py values from selected {assay} runs", title_x=0.5
    )

    return main_fig


def make_happy_plot(happy_df, config):
    """
    Make the full hap.py plot with b37 and b38 data

    Parameters
    ----------
    happy_df : pd.DataFrame
        dataframe of hap.py data
    config : dict
        the config file in dict format

    """
    subplot_fig = make_subplots(
        rows=1,
        cols=2,
        subplot_titles=("SNP", "INDEL"),
    )
    counter = 0

    assay = config["project_search"]["assay"]

    for plot_config in config["file"]["happy"]["plots"]:
        counter += 1
        fig = make_main_happy_plot(
            happy_df.sort_values(by="Sample"),
            plot_config["data_type"],
            plot_config["col_x"],
            plot_config["col_y"],
        )

        for i in fig.data:
            subplot_fig.add_trace(i, row=1, col=counter)

        fig_with_lines = format_happy_plot(
            subplot_fig,
            1,
            counter,
            plot_config["col_x"],
            plot_config["col_y"],
            config["project_search"]["assay"],
            plot_config["y_range_low"],
            plot_config["y_range_high"],
            plot_config["x_range_low"],
            plot_config["x_range_high"],
            plot_config["x_warning_line"],
            plot_config["x_fail_line"],
            plot_config["y_warning_line"],
            plot_config["y_fail_line"],
        )

    # Remove duplicate legends
    legend_names = set()
    fig_with_lines.for_each_trace(
        lambda trace: (
            trace.update(showlegend=False)
            if (trace.name in legend_names)
            else legend_names.add(trace.name)
        )
    )
    fig_with_lines.show()
    fig_with_lines.write_html(f"happy_{assay}.html")


def main():
    args = parse_args()

    with open(args.config, encoding="UTF-8") as f:
        config = json.load(f)

    assay = config["project_search"]["assay"]
    if args.runmode == "gather_and_plot":

        projects = get_projects(
            search_term=config["project_search"]["pattern"],
            number_of_projects=config["project_search"]["number_of_projects"],
            after_date=config["project_search"]["after_date"],
            before_date=config["project_search"]["before_date"],
            search_mode=config["project_search"]["mode"],
        )

        dfs_dict = add_qc_metric_dfs(projects, config)

        # Merge dfs for each QC file
        dfs_dict = {
            key: pd.concat(dfs_dict[key]["dfs"], ignore_index=True)
            for key in dfs_dict.keys()
        }

        # output merged qc_status .xlsx's to .tsv
        qc_df = dfs_dict["qc_status"]
        qc_df.to_csv(f"qc_status_{assay}.tsv", sep="\t", index=False)

        # # output merged happy .csvs to .tsv
        happy_df = dfs_dict["happy"]
        happy_df = happy_df.sort_values(by="Sample")
        happy_df.to_csv(f"happy_{assay}.tsv", sep="\t", index=False)

        for key in dfs_dict.keys():
            if key == "happy":
                make_happy_plot(happy_df, config)

            elif key == "qc_status":
                continue
            else:
                # add reason and pass/fail columns to merged dfs
                final_df = pd.merge(
                    dfs_dict[key],
                    qc_df[["Sample", "QC_status", "Reason"]],
                    on="Sample",
                )
                # Write merged dataframes out to TSV
                final_df.to_csv(f"{key}_{assay}.tsv", sep="\t", index=False)

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
                        plot_std=plot_config["plot_std"],
                    )

    elif args.runmode == "plot_only":
        assay = config["project_search"]["assay"]

        for key in config["file"].keys():
            plot_file = f"{key}_{assay}.tsv"
            if key == "happy":
                happy_df = pd.read_csv(plot_file, sep="\t")
                make_happy_plot(happy_df, config)
            elif key == "qc_status":
                continue
            else:
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
                        plot_std=plot_config["plot_std"],
                    )


if __name__ == "__main__":
    main()
