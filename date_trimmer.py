import pandas as pd
from pathlib import Path

def trim_csv_by_date(
    file_path,
    date_column="date",
    before=None,
    after=None,
    output_path=None
):
    """
    Trims a CSV file by a date column.

    Parameters:
        file_path (str): Path to input CSV
        date_column (str): Name of the date column
        before (str): Keep rows strictly before this date (YYYY-MM-DD)
        after (str): Keep rows strictly after this date (YYYY-MM-DD)
        output_path (str): Optional output path. If None, overwrites original file.

    Returns:
        pd.DataFrame: Trimmed dataframe
    """

    # Load CSV
    df = pd.read_csv(file_path, low_memory=False).dropna(how="all")

    # Convert date column to datetime
    df[date_column] = pd.to_datetime(df[date_column], errors="coerce")

    # Apply filters
    if before:
        before_date = pd.to_datetime(before)
        df = df[df[date_column] < before_date]

    if after:
        after_date = pd.to_datetime(after)
        df = df[df[date_column] > after_date]

    # Format back to YYYY-MM-DD
    df[date_column] = df[date_column].dt.strftime("%Y-%m-%d")

    # Determine output path
    if output_path is None:
        output_path = file_path  # overwrite original

    # Save CSV
    df.to_csv(output_path, index=False)

    return df


def main():
    files = ["data/london_energy.csv", "data/london_weather.csv"]
    destinations = ["docker-db-stuff/final-project/data/london_energy.csv", "docker-db-stuff/final-project/data/london_weather.csv"]
    columns = ["Date", "date"]
    for file, destination, column in zip(files, destinations, columns):
        trim_csv_by_date(file_path=file, before='2014-02-28', date_column=column, after='2011-12-31', output_path=destination)


if __name__ == "__main__":
    main()
