# PySpark Customer Flight Activity Analysis

**Author:** Mochamad Reza Rahadi

---

## Introduction

This script performs data processing and customer activity analysis for an airline using PySpark on Google Colab. It reads multiple CSV files, cleans and transforms data, and saves the analysis results as CSV files.

---

## Required Files

To run this script, ensure the following files are in the same directory as the script:

1. `customer_flight_activity.csv`: Contains information on customer flight activities.
2. `customer_loyalty_history.csv`: Contains customer loyalty data.
3. `calendar.csv`: Provides calendar data for time-based queries.

---

## Analysis Objectives

The script performs six key analyses to provide insights into customer activities and visualizations for:

1. Calculating the average salary based on loyalty card type.
2. Comparing points redeemed between 2017 and 2018.
3. Identifying the province with the highest number of flights in 2018.
4. Ranking cities with the highest flights within the top province in 2018.
5. Comparing the number of flights per quarter in 2018.
6. Ranking provinces by the highest number of flights in the third quarter of 2018.

---

## Code Workflow

### 1. Initialize Spark Session

- Creates a Spark session using PySpark, stored in the variable `spark_session`.

### 2. Read Data

- Reads the three CSV tables using `spark_session.read.csv`.

### 3. Clean and Transform Data

- Defines schemas to set appropriate column types, as CSV data is initially read as strings.
- Reads data using the defined schemas.
- Drops the `_c0` column created by default when reading CSV files.
- Fills null values (e.g., salary in `customer_loyalty_history`) with 0 to avoid aggregation issues.

### 4. SQL Analysis and Visualization

- Creates temporary tables for SQL analysis from the three DataFrames.
- Performs the following analyses:
  1.  **Average Salary by Loyalty Card**: No joins are required, as data is available within a single table.
  2.  **Points Redeemed Comparison (2017 vs. 2018)**: Compares total points to see if 2018 had an increase in flights.
  3.  **Top Province by Flight Count (2018)**: Joins `customer_loyalty_history` and `customer_flight_activity` tables.
  4.  **Top Cities in Ontario (2018)**: Ontario is chosen as it has the highest flight count. Joins `customer_loyalty_history` and `customer_flight_activity`.
  5.  **Quarterly Flight Comparison (2018)**: Joins `calendar` and `customer_flight_activity` using year and month functions to categorize flights by quarter in 2018.
  6.  **Top Province in Q3 (2018)**: Based on Q3, the highest quarter, identifies provinces with the most flights.

### 5. Save Analysis Results to CSV

- Uses `write.csv` to export each analysis result to CSV files in the `graphic and data` folder, producing six CSV files and six corresponding visualizations.

---

## Running the Code

This script requires the following libraries:

```shell
pip install pyspark
pip install plotly
pip install -U kaleido
```

## Analysis Breakdown

Here’s a detailed look at each analysis performed by the script:

1. **Average Salary by Loyalty Card**

   - Calculates the average salary of customers based on their loyalty card type, offering insight into the income levels across different loyalty tiers.

2. **Points Redeemed Comparison (2017 vs. 2018)**

   - Compares total points redeemed between 2017 and 2018 to assess any increase in customer engagement or activity in 2018. Higher points redeemed suggest higher flight activity.

3. **Top Province by Flight Count in 2018**

   - Identifies the province with the highest and lowest flight counts in 2018. This provides geographical insight into where the airline has the most active customer base.

4. **Top Cities in Ontario by Flight Count (2018)**

   - Drills down into Ontario (the province with the highest flights) to rank cities by flight count, highlighting the cities with the most frequent travelers.

5. **Quarterly Flight Comparison for 2018**

   - Compares the number of flights across each quarter in 2018, showing trends in seasonal travel patterns and helping identify the busiest times of the year.

6. **Top Province in Q3 2018**
   - Since the third quarter has the highest flight count, this analysis identifies which provinces had the most flights during this period, giving insight into travel hotspots.

---

## Output

Each CSV file corresponds to one of the six analyses performed, and visualizations are also saved in the `graphic and data` folder to aid in data interpretation.

---

## Notes

- **Dependencies**: Ensure PySpark, Plotly, and Kaleido are installed before running the script.
- **Environment**: Designed for execution in Google Colab, but can be adapted to other environments that support PySpark.

---

## Contact

For questions or further information, please contact Mochamad Reza Rahadi.

---
