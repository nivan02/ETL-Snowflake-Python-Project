# ETL-Snowflake-Python-Project

This project showcases the design of a complete ETL/ELT pipeline using **Snowflake** as the data warehouse and **Python** for automation. The goal was to integrate data from multiple sources—purchase orders, supplier invoices, supplier details, and weather information—into a unified analytical framework. The pipeline emphasizes scalable data loading, SQL-based transformations, and automation through Python.

The process begins with creating a Snowflake warehouse, database, schema, and stage, and then connecting Snowflake to Python with the **snowflake-connector-python** library. Purchase order data, which was split across forty-one monthly CSV files, was staged and loaded into a single Snowflake table. While loading, the pipeline cleaned the data, applied correct datatypes, and added a calculated field to capture purchase order totals.

Supplier invoices, provided in XML format, were parsed and stored as a structured table in Snowflake. These invoices were then joined with the purchase order table to calculate the difference between quoted and invoiced amounts, which was saved as a materialized view called **purchase_orders_and_invoices**. Supplier details, originally in a Postgres database, were exported with **psycopg2**, staged as a CSV, and loaded into Snowflake for integration.

Weather data was incorporated from the **NOAA Weather & Environment dataset** available in the Snowflake Marketplace. Since supplier information was organized by zip code, a mapping file of zip codes to latitude/longitude was used to identify the nearest weather station for each supplier. This data was transformed into a materialized view called **supplier_zip_code_weather**, which tracks daily high temperatures for each supplier zip code.

The final step brought everything together. Purchase orders, invoices, supplier details, and weather data were joined on zip code and transaction date to create a consolidated dataset. This integration allows analysis of invoice discrepancies in relation to supplier details and external conditions such as weather.

All of the work is contained in the Jupyter Notebook **ETL_GroupProject.ipynb**. The notebook includes both the Python code for orchestration and the SQL queries executed in Snowflake. While Python handles automation, all heavy transformations are pushed down into Snowflake for efficiency and scalability. The result is a reproducible pipeline that demonstrates real-world ETL/ELT best practices.
