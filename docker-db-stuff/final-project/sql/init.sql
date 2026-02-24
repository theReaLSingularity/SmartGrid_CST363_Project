-- Alexander Moore
-- Bryan Rivera
-- Lee Cao
-- CST 363-30
-- 02/14/2026


 			-- **** docker-compose.yml contents: *****
-- services:
--   pg:
--     image: pgvector/pgvector:pg16
--     environment:
--       POSTGRES_USER: smart
--       POSTGRES_PASSWORD: smart1
--       POSTGRES_DB: smartgrid_db
--     ports:
--       - "5435:5432"
--     volumes:
--       - smartgrid_pgdata:/var/lib/postgresql/data
--       - ./data:/data
--	 - ./sql:/docker-entrypoint-initdb.d

-- volumes:
--   smartgrid_pgdata:

 			-- **** Running Docker compose command: *****
-- docker compose up -d

 			-- **** Dropping tables if exist: *****
DROP VIEW IF EXISTS v_daily_model_input;
DROP TABLE IF EXISTS avg_consumption;
DROP TABLE IF EXISTS energy;
DROP TABLE IF EXISTS weather;
DROP TABLE IF EXISTS calendar;
DROP TABLE IF EXISTS features_daily;


 			-- **** Enabling pgvector extension: *****
CREATE EXTENSION IF NOT EXISTS vector;


 			-- **** Creating energy table: *****
CREATE TABLE energy (
	household_id 		VARCHAR(50),
	date 				DATE,
	kwhr 				DOUBLE PRECISION,
	PRIMARY KEY (household_id, date)
);

COPY energy (household_id, date, kwhr)
	FROM '/data/london_energy.csv'
	WITH (FORMAT CSV, HEADER);

DELETE FROM energy 
	WHERE date >= '2014-02-28' -- anomolous data points on '2014-02-28'
		OR date < '2012-01-01'; -- too few samples before '2012-01-01' (<500)


 			-- **** Creating weather table: *****
CREATE TABLE weather (
	date 				DATE PRIMARY KEY,
	cloud_cover 		FLOAT,
	sunshine 			FLOAT,
	global_radiation 	FLOAT,
	max_temp 			FLOAT,
	mean_temp 			FLOAT,
	min_temp 			FLOAT,
	precipitation		FLOAT,
	pressure			FLOAT,
	snow_depth			FLOAT
);

COPY weather (	date,
				cloud_cover,
				sunshine,
				global_radiation,
				max_temp,
				mean_temp,
				min_temp,
				precipitation,
				pressure,
				snow_depth)
	FROM '/data/london_weather.csv'
	WITH (FORMAT CSV, HEADER);
	
DELETE FROM weather
	WHERE date < '2012-01-01' -- limiting weather data to match available energy data date range
		OR date > '2014-02-27';

UPDATE weather
	SET cloud_cover = (SELECT AVG(cloud_cover) -- dealing with NULL value
					   FROM weather
					   WHERE date
					       IN ('2012-03-01', '2012-03-03'))
	WHERE date = '2012-03-02';



 			-- **** Creating calendar table: *****
CREATE TABLE calendar (
	date		DATE PRIMARY KEY,
	dow			INTEGER,
	day			INTEGER,
	month		INTEGER,
	year		INTEGER,
	doy			INTEGER,
	is_weekend	INTEGER,
	is_holiday	INTEGER
);

COPY calendar (	date,
				dow,
				day,
				month,
				year,
				doy,
				is_weekend,
				is_holiday)
	FROM '/data/london_calendar.csv'
	WITH (FORMAT CSV, HEADER);


 			-- **** Creating table of averaged kWh per day energy consumption: *****--
CREATE TABLE avg_consumption (
	date			DATE PRIMARY KEY,
	consumption 	NUMERIC(7,4)
);

INSERT INTO avg_consumption
	SELECT date, AVG(kwhr)::NUMERIC(7, 4) AS consumption
			FROM energy
			GROUP BY date;


 			-- **** Creating view for DuckDB Feature creation: *****--
CREATE VIEW v_daily_model_input AS
	SELECT
		a.date,
		a.consumption,
		w.cloud_cover, w.sunshine, w.global_radiation,
		w.max_temp, w.mean_temp, w.min_temp,
		w.precipitation, w.pressure, w.snow_depth,
		c.dow, c.day, c.month, c.year, c.doy, c.is_weekend, c.is_holiday
	FROM avg_consumption a
		JOIN weather w USING (date)
		JOIN calendar c USING (date);

-- Lee Cao
-- Monthly avg Temperture and Energy Consumption
SELECT 
    c.year,
    c.month,
    ROUND(AVG(a.consumption)::NUMERIC, 4) AS avg_consumption,
    ROUND(AVG(w.mean_temp)::NUMERIC, 2) AS avg_temp
FROM avg_consumption a
INNER JOIN calendar c ON a.date = c.date
INNER JOIN weather w ON a.date = w.date
GROUP BY c.year, c.month
ORDER BY c.year, c.month;

-- Highest vs Lowest Energy Consumption 
WITH extremes AS (
    SELECT 'Highest' AS type, date, consumption
    FROM avg_consumption
    WHERE consumption = (SELECT MAX(consumption) FROM avg_consumption)
    UNION ALL
    SELECT 'Lowest' AS type, date, consumption
    FROM avg_consumption
    WHERE consumption = (SELECT MIN(consumption) FROM avg_consumption)
)
SELECT 
    e.type,
    e.date,
    e.consumption AS avg_kwh,
    w.mean_temp,
    c.is_weekend,
    c.is_holiday
FROM extremes e
INNER JOIN weather w ON e.date = w.date
INNER JOIN calendar c ON e.date = c.date
ORDER BY e.consumption DESC;

-- Composite index on calendar for monthly trend queries
-- Accelerates Query 2's GROUP BY and ORDER BY on year, month
CREATE INDEX idx_calendar_year_month ON calendar(year, month);

-- Index on consumption values for finding extremes
-- Accelerates Query 1's MAX/MIN subqueries
CREATE INDEX idx_consumption_value ON avg_consumption(consumption);


 			-- **** REQUIRES FEATURES TABLE CREATED WITH DUCKDB: *****--

-- ALTER TABLE features_daily ADD PRIMARY KEY (date);
-- CREATE INDEX IF NOT EXISTS idx_features_daily_date ON features_daily(date);

-- select * from features_daily ORDER BY date;
