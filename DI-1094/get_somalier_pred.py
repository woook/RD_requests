"""get result of somalier predictions
"""

import pandas as pd
import dxpy
from concurrent.futures import ThreadPoolExecutor


def find_somalier_report(project_id: str) -> pd.DataFrame:
    """
    Retrieve somalier predictions from
    path /output/*/eggd_somalier_relate2multiqc_v1.0.1/
    """
    res = list(
        dxpy.find_data_objects(
            project=project_id,
            folder="/output/",
            recurse=True,
            name="Multiqc_somalier.samples.tsv",
            classname="file",
            name_mode="glob",
            describe={
                "fields": {"name": True, "modified": True, "archivalState": True}
            },
        )
    )

    if not res:
        print(f"match not found in {project_id}")
        return

    print(f"found {len(res)} matches in {project_id}")

    res = [
        {
            "project_id": x["project"],
            "file_id": x["id"],
            "file_name": x["describe"]["name"],
            "archival_state": x["describe"]["archivalState"],
        }
        for x in res
    ]

    return pd.DataFrame(res)


def read_somalier_report(file_id: str, project_id: str) -> pd.DataFrame:
    with dxpy.open_dxfile(file_id, project=project_id) as dx_file:
        data = pd.read_csv(dx_file, sep="\t")
        data = data[["sample_id", "Predicted_Sex", "Match_Sexes"]]
        data["project"] = project_id

    return data


def main():
    df = pd.read_csv("dias_b38_samples.csv")

    df = pd.concat(
        [find_somalier_report(proj) for proj in df.project_id.unique()],
        ignore_index=True,
    )
    df.to_csv("b38_somalier_files.csv", index=False)

    with ThreadPoolExecutor(max_workers=32) as exec:
        df = pd.concat(
            exec.map(
                lambda row: read_somalier_report(row.file_id, row.project_id),
                df.itertuples(index=False),
            ),
            ignore_index=True,
        )
    df.to_csv("b38_somalier_report.csv", index=False)

    print("DONE")


if __name__ == "__main__":
    main()
