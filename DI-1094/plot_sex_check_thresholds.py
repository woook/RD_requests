#!/usr/bin/env python
"""
Sex_check thresholds for DIAS
This script reads input files, processes sex check results, and generates
histograms and scatter plots for the CEN and TWE assays.
"""

import argparse
import pandas as pd
import plotly.express as px
from typing import Dict, Tuple


def parse_arguments() -> argparse.Namespace:
    """
    Parse command-line arguments.

    Returns:
        argparse.Namespace: The parsed command-line arguments.
    """
    parser = argparse.ArgumentParser(
        description="Process and plot sex_check thresholds for CEN or TWE assays."
    )

    parser.add_argument(
        "--samples", required=True, help="Path to dias_b38_samples.csv"
    )
    parser.add_argument(
        "--somalier", required=True, help="Path to b38_somalier_report.csv"
    )
    parser.add_argument(
        "--sex_check_table",
        required=True,
        help="Path to the multiqc sex check report (for CEN or TWE)",
    )
    parser.add_argument(
        "--assay",
        required=True,
        choices=["CEN", "TWE"],
        help="Specify the assay type (CEN or TWE).",
    )
    parser.add_argument(
        "--calculate_threshold",
        action="store_true",
        help="If set, calculate thresholds using standard deviation.",
    )
    parser.add_argument(
        "--male_threshold",
        type=float,
        help="Manually set the male threshold if not calculating automatically.",
    )
    parser.add_argument(
        "--female_threshold",
        type=float,
        help="Manually set the female threshold if not calculating automatically.",
    )

    return parser.parse_args()


def read_samples(file_path: str) -> pd.DataFrame:
    """
    Read DIAS samples from a CSV file, filtering out control samples.

    Args:
        file_path (str): Path to the samples CSV file.

    Returns:
        pd.DataFrame: df of sample data.
    """
    df = pd.read_csv(file_path)
    # Remove control samples
    df = df[~df["samples"].str.contains("NA12878-")]

    # Confirm no duplicates
    assert len(df) == len(df.samples.unique())
    return df


def map_samples_to_run_and_date(
    df: pd.DataFrame,
) -> Tuple[Dict[str, str], Dict[str, str]]:
    """
    Map samples to their run ID and date.

    Args:
        df (pd.DataFrame): Sample data.

    Returns:
        Tuple[Dict[str, str], Dict[str, str]]: Mappings for run and date.
    """
    samples_to_run = {row["samples"]: row["run"] for _, row in df.iterrows()}
    samples_to_date = {row["samples"]: row["date"] for _, row in df.iterrows()}
    return samples_to_run, samples_to_date


def read_somalier_report(file_path: str) -> Dict[str, bool]:
    """
    Read and process the somalier report, filtering out control samples.

    Args:
        file_path (str): Path to somalier report CSV file.

    Returns:
        Dict[str, bool]: Mapping of samples to their somalier predictions.
    """
    df_somalier = pd.read_csv(file_path)
    df_somalier = df_somalier[~df_somalier["sample_id"].str.contains("NA12878-")]
    assert len(df_somalier) == len(df_somalier.sample_id.unique())
    return {
        row["sample_id"]: row["Match_Sexes"]
        for _, row in df_somalier.iterrows()
    }


def read_sex_check_table(
    file_path: str,
    samples_to_run: Dict[str, str],
    samples_to_date: Dict[str, str],
    samples_to_somalier: Dict[str, bool],
) -> pd.DataFrame:
    """
    Read and process the sex check table, adding meta columns like run, date, 
    and somalier status.

    Args:
        file_path (str): Path to the sex check table file.
        samples_to_run (Dict[str, str]): Mapping of samples to run.
        samples_to_date (Dict[str, str]): Mapping of samples to dates.
        samples_to_somalier (Dict[str, bool]): Mapping of samples to 
        somalier predictions.

    Returns:
        pd.DataFrame: Processed sex check table with additional metadata.
    """
    df = pd.read_csv(file_path, sep="\t")

    # Filter out samples with unknown sexes
    df = df[df.reported_sex.isin(["M", "F"])]
    df["run"] = df["Sample"].map(samples_to_run)
    df["date"] = df["Sample"].map(samples_to_date)
    df["somalier_sex_check"] = df["Sample"].map(samples_to_somalier)
    return df


def calculate_thresholds(
    df: pd.DataFrame, male_multiplier: float = 1, female_multiplier: float = 1
) -> Tuple[float, float]:
    """
    Calculate male and female thresholds based on the standard deviation.

    Args:
        df (pd.DataFrame): The data used to calculate thresholds.
        male_multiplier (float): Multiplier to adjust the male threshold.
        female_multiplier (float): Multiplier to adjust the female threshold.

    Returns:
        Tuple[float, float]: The calculated male and female thresholds.
    """
    df = df[df["somalier_sex_check"] == True]
    mean_score = df["score"].mean()
    std_score = df["score"].std()
    male_threshold = mean_score - male_multiplier * std_score
    female_threshold = mean_score + female_multiplier * std_score
    return male_threshold, female_threshold


def plot_histogram_with_thresholds(
    df: pd.DataFrame, assay: str, male_threshold: float,
    female_threshold: float
) -> None:
    """
    Plot a histogram of scores with male and female thresholds.

    Args:
        df (pd.DataFrame): The data for the histogram.
        assay (str): The assay type (CEN or TWE).
        male_threshold (float): The male threshold value.
        female_threshold (float): The female threshold value.
    """
    color_discrete_map = {"M": "purple", "F": "navy"}
    fig = px.histogram(
        df,
        x="score",
        color="reported_sex",
        marginal="box",
        color_discrete_map=color_discrete_map,
        hover_data=[
            "Sample",
            "run",
            "somalier_sex_check",
            "mapped_chrY",
            "mapped_chr1",
        ],
    )

    # Add threshold lines
    fig.add_vline(
        x=male_threshold,
        line_width=1,
        line_dash="dash",
        line_color="purple",
        annotation_text=f"male_threshold: {male_threshold:.2f}",
        annotation_position="top left",
    )
    fig.add_vline(
        x=female_threshold,
        line_width=1,
        line_dash="dash",
        line_color="navy",
        annotation_text=f"female_threshold: {female_threshold:.2f}",
    )

    fig.update_layout(
        width=900, height=600, title=f"Distribution of {assay} Scores by reported_sex"
    )
    fig.show()
    fig.write_html(f"distribution_of_scores_{assay}.html")


def plot_score_trend(
    df: pd.DataFrame, assay: str, male_threshold: float, female_threshold: float
) -> None:
    """
    Plot a scatter trend of scores over time with male and female thresholds.

    Args:
        df (pd.DataFrame): The data for the scatter plot.
        assay (str): The assay type (CEN or TWE).
        male_threshold (float): The male threshold value.
        female_threshold (float): The female threshold value.
    """
    color_discrete_map = {"False": "red", "True": "green"}
    df["size"] = 1
    fig = px.scatter(
        df,
        x="date",
        y="score",
        symbol="reported_sex",
        color="somalier_sex_check",
        size="size",
        color_discrete_map=color_discrete_map,
        hover_data=["Sample", "run", "mapped_chrY", "mapped_chr1"],
    )

    # Add threshold lines
    fig.add_hline(
        y=male_threshold,
        line_width=1,
        line_dash="dash",
        annotation_text=f"male_threshold: {male_threshold:.2f}",
        annotation_position="top right",
    )
    fig.add_hline(
        y=female_threshold,
        line_width=1,
        line_dash="dash",
        annotation_text=f"female_threshold: {female_threshold:.2f}",
        annotation_position="bottom right",
    )

    fig.update_layout(
        width=900, height=600,
        title=f"Trends of sex_check Scores for {assay} Samples"
    )
    fig.show()
    fig.write_html(f"sex_check_thresholds_{assay}.html")


def main() -> None:
    """Main function to execute the sex check threshold calculations and plotting."""
    # Parse command-line arguments
    args = parse_arguments()

    # Read samples and somalier report
    df_samples = read_samples(args.samples)

    # Create mappings from sample to run, date, and somalier results
    samples_to_run, samples_to_date = map_samples_to_run_and_date(df_samples)
    samples_to_somalier = read_somalier_report(args.somalier)

    # Read sex check table
    df_assay = read_sex_check_table(
        args.sex_check_table, samples_to_run, samples_to_date, samples_to_somalier
    )

    # Calculate thresholds or set them manually
    if args.calculate_threshold:
        male_threshold, female_threshold = calculate_thresholds(df_assay)
    else:
        if args.male_threshold is None or args.female_threshold is None:
            raise ValueError(
                "You must provide thresholds if not calculating them."
            )
        male_threshold = args.male_threshold
        female_threshold = args.female_threshold

    # Plot histogram and scatter plot
    plot_histogram_with_thresholds(
        df_assay, args.assay, male_threshold, female_threshold
    )
    plot_score_trend(df_assay, args.assay, male_threshold, female_threshold)


if __name__ == "__main__":
    main()
