"""compares and summarises differences in TD data in spreadsheet and postgres db
"""
import pandas as pd

TD_SPREADSHEET = pd.read_csv("internal_east_glh_td.csv").set_index("test-id")
TD_SQL = pd.read_csv("td_sql.csv").set_index("test-id")

def compare_col(col):
    """compares the values of a col btw 2 dataframes

    Args:
        col (str): name of column to compare
    """
    excel = TD_SPREADSHEET
    sql = TD_SQL

    diff = excel[col].compare(sql[col])
    diff.columns = ["Spreadsheet", "Postgress_DB"]
    if diff.empty:
        print(f"No diff in {col} column")
        return
    print(f"diff in {col} column")
    print(diff)


def compare_df():
    """compare and save diff btw dfs
    """
    excel = TD_SPREADSHEET
    sql = TD_SQL
    excel = excel[sql.columns]
    
    diff = excel.compare(sql, result_names=("Spreadsheet", "Postgres_DB"))
    
    # Save result to an Excel file
    output_file = "td_diff.xlsx"
    diff.to_excel(output_file)
    print(f"Summary of diff saved to {output_file}")


def main():
    """entry point to script
    """
    for col in TD_SQL.columns:
        compare_col(col)
        print()
        
    compare_df()

if __name__ == "__main__":
    main()
