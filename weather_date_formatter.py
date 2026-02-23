import pandas as pd


def transform_date_format(file_name):
    with open(file_name) as csv_file:
        df = pd.read_csv(csv_file)
        df['date'] = pd.to_datetime(df['date'], format='%Y%m%d').dt.strftime('%Y-%m-%d')

        df.to_csv('data/london_weather.csv', index=False)



def main():
    transform_date_format('data/raw/london_weather_raw.csv')


if __name__ == '__main__':
    main()
