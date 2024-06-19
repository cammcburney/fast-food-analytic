# Fast Food Data Analysis

## Contributors

<p align="center">
 <a href="https://github.com/cammcburney">Cameron McBurney</a> | <a href="https://github.com/lUKEdOWNEY">Luke Downey</a> |
 </p>

 ## Project Overview

 This project involves transforming OLTP data into an OLAP format to optimise it for querying. Excel was used to clean data before inserting the rows into a PSQL database, the data was then extracted and transformed and uploaded to another PSQL database optimised for OLAP. The data was queried and visualised with both Excel and Tableau at the end of the process to provide insights and analysis of the contents.

- Data Cleaning and Initial Visualization:
    - Begin with uncleaned Excel files.
    - Perform necessary cleaning and preliminary visualizations directly in Excel.
- Data Transformation and Loading:
    - Convert cleaned Excel files to CSV format.
    - Utilize Python for loading CSV data into an OLTP PostgreSQL database.
- Data Structuring and Warehousing:
    - Transform the data into a star schema format.
    - Load the structured data into a dedicated data warehouse (OLAP).
- Data Distribution and Visualization:
    - Export warehouse data back into Excel spreadsheets.
    - Conduct detailed data analysis using SQL queries.
    - Create interactive dashboards and visualizations using Tableau for comprehensive data exploration.
- Reporting and Distribution:
    - Share finalized dashboards and reports with relevant stakeholders within the company or client organization.

This structured approach not only highlights our technical capabilities but also mirrors the practical challenges and outcomes expected in professional data management projects.


## Set-up

To setup this project you will need Postgres, Maketools, SQL and Python installed. The project requires a .env file in the following format to run sucessfully with a valid PSQL username and password:
```
db_user = <username>
db_password = <password>
db_host = localhost
db_port = 5432

db_name = 'oltpdatabase'
db_wname = 'olapwarehouse'

test_db_name = 'testoltpdatabase'
test_wh_name = 'testolapwarehouse'
```

1. Run the following command to set up your virtual environment and install required dependencies along with the databases required for querying and testing:

```
make requirements
```

2. Run this command next to set up security and coverage modules:

```
make dev-setup
```

3. Set up your `PYTHONPATH`:

```
export PYTHONPATH=$(pwd)
```

4. Run checks for unit-tests, pep8 compliancy, coverage and security vulnerabilities:

```
make run-checks
```

