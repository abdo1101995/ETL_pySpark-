### The project follows the follow steps:
* Step 1: Scope the Project and Gather Data
* Step 2: Explore and Assess the Data
* Step 3: Define the Data Model
* Step 4: Run ETL to Model the Data
* Step 5: Complete Project Write Up

### Step 1: Scope the Project and Gather Data

#### Scope 
in This Data Engineering Project I will Extract more than data source and ETL by Making Data Modelling and put final table in
destination place
I used pandas to read handle small datasets and pyspark for large datasets
I used S3 storage to save all tables 


#### Describe and Gather Data 

I94 Immigration Data: This data comes from the US National Tourism and Trade Office. A data dictionary is included in the workspace. 

World Temperature Data: This dataset came from Kaggle.

U.S. City Demographic Data: This data comes from OpenSoft.

Airport Code Table: This is a simple table of airport codes and corresponding cities.

### Step 2: Explore and Assess the Data
#### Explore the Data 
I Explored data with pandas and pyspark
I used pyspark to process large data in immigration_data and handle sas data.

#### Cleaning Steps
After Exploring data I will handle missing data , duplicated data ,data format,inconsistent data

### Step 3: Define the Data Model
#### 3.1 Conceptual Data Model
we will work with star schema for Bi Teams and analytics we will extract some information about immigration and know more analyis about that

issue,I used star schema because easy for all to understand and efficent in query because denormalized model unlike snowflake schema 

that model contains 

fact table->it is about bussiness metrics like sales ,profits,number of employess.

dimension tables that surrounds fact table by keys(which link them):they are context that answer why ,how ,where ,when.

![alt text](https://github.com/abdo1101995/ETL_pySpark-/blob/main/Screenshot%202023-01-22%20004921.png)

#### 3.2 Mapping Out Data Pipelines
we do ETL Pipline Process:


Extracting data from different sources sas_data,us-cities-demographics.csv,../../data2/GlobalLandTemperaturesByCity.csv

transform process:from spliting fact table ,dimension tables ,cleaning , edit data_types  to get final data for  loading in destination source



Load Process:load final data in s3 storage

### Step 4: Run Pipelines to Model the Data 

#### 4.1 Create the data model
Build the data pipelines to create the data model.

#### 4.2 Data Quality Checks
Explain the data quality checks you'll perform to ensure the pipeline ran as expected. These could include:
 * Integrity constraints on the relational database (e.g., unique key, data type, etc.)
 * Unit tests for the scripts to ensure they are doing the right thing
 * Source/Count checks to ensure completeness
 
Run Quality Checks

#### 4.3 Data dictionary 
Create a data dictionary for your data model. For each field, provide a brief description of what the data is and where it came from. You can include the data dictionary in the notebook or in a separate file.


#### Step 5: Complete Project Write Up
### * tools and technologies for the project.

1-AWs Cloud(iam users,S3 Stoarge)

2-python(pysaprk,pandas)

### * the data should be updated and why.

 * The data was increased by 100x.
   we can used EMR in AWS that provides managed scaling, and reconfiguring of clusters to handle that updated and size of data
   you may run multiple clusters to deal with multiple users and/or applications.
 * The data populates a dashboard that must be updated on a daily basis by 7am every day.
   we can used airflow to schedule dashboard daily
 * The database needed to be accessed by 100+ people.
   we can load data and schema in data warehouse and other database server
