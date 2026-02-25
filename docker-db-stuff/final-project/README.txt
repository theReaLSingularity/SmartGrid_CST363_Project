SmartGrid_CST363_Project

-- Steps to Run the Project --

- Clone project from GitHub: https://github.com/theReaLSingularity/SmartGrid_CST363_Project

- Configure project with Python 3.10 virtual environment

- Ensure all dependencies are installed (pip install -r requirements.txt)

- Ensure Docker is running before starting the process.

Follow the steps below in order:

1. Run weather_date_formatter.py and copy generated files to sql data directory
(OPTIONAL — only required for full data pipeline recreation.)

2. Run london_calendar_creator.py and copy generated files to sql data directory
(OPTIONAL — only required for full data pipeline recreation.)

3. Run date_trimmer.py
(OPTIONAL — only required for full data pipeline recreation.)

4. Start the Docker services in 'final-project' directory: docker compose up -d
   and allow 2-3 minutes for data to copy and tables to populate.

5. Run build_features_duckdb.py
(This creates the features table required for model training.)

6. Optionally, open smart_grid_CST363.sql and uncomment the section marked:

"-- **** REQUIRES FEATURES TABLE CREATED WITH DUCKDB: *****--"

and run uncommented queries.

7. Run model_train_test.py
(This will train the model and generate predictions.)

Output:
After successful execution, the model predictions plot will be generated at:
SmartGrid_CST363_Project/plots/Prediction_vs_Actual.png