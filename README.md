# Data Warehouse on AWS
Hello, this is a project focus on implement a Data Warehouse in AWS using Redshift, S3 buckets and IAM to authorize Redshift to readOnly S3 buckets to load into the Data Warehouse.
Here I'm a Data Engineer working for a music streaming startup Sparkify. My task here is to help the analytics team by building a ETL pipeline that extract data from S3 Bucket, load it into 2 stage tables and then inserting it into the Dimensional Model.

### This project has the following scripts:
* create_tables.py - Script that drop and create the necessary tables to help the analytics team.
* sql_quieries.py - Script that contains all the SQL queries to drop, create, insert and COPY the data from a S3 bucket.
* etl.py - ETL pipeline that drop, create and then insert the data into the Data Warehouse.
* Testing.ipynb - A Jupyter notebook with some SQL queries to check the data that will be inserted into the Dimensional Model.

### How to execute the scripts:
* Check your if you have python installed with ```python -V```
* First, execute the 'create_tables.py' to drop (if they exist already) and the create the staging tables as well as the Dimensional Model with ```python create_tables.py```
* Last but not least, execute the 'etl.py' to load the data from S3 Bucket into staging and then the Dimensional Model with ```python etl.py```