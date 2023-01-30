import configparser
from datetime import datetime
import pandas as pd
from pyspark.sql.functions import isnan, when, count, col ,udf
import datetime as dt
from pyspark.sql.types import StringType , IntegerType, TimestampType, DateType,DoubleType
import os
from boto.s3.connection import S3Connection
from pyspark.sql.types import StructType, StructField
from pyspark.sql import SparkSession
#spark version
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["PATH"] = "/opt/conda/bin:/opt/spark-2.4.3-bin-hadoop2.7/bin:/opt/conda/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/lib/jvm/java-8-openjdk-amd64/bin"
os.environ["SPARK_HOME"] = "/opt/spark-2.4.3-bin-hadoop2.7"
os.environ["HADOOP_HOME"] = "/opt/spark-2.4.3-bin-hadoop2.7"

#config_parser to read dl.cfg file
config = configparser.ConfigParser()
config.read('dl.cfg')

#access and secert key
AWS_ACCESS_KEY_ID = config['AWS']['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = config['AWS']['AWS_SECRET_ACCESS_KEY']
os.environ['AWS_ACCESS_KEY_ID'] =  config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
    """
    create spark_session and lauch spark in aws
    
    
    """
    spark = SparkSession.builder\
        .config("spark.jars.repositories", "https://repos.spark-packages.org/")\
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0,saurfang:spark-sas7bdat:2.0.0-s_2.11")\
        .config("spark.hadoop.fs.s3a.access.key",AWS_ACCESS_KEY_ID)\
        .config("spark.hadoop.fs.s3a.secret.key",AWS_SECRET_ACCESS_KEY)\
        .enableHiveSupport().getOrCreate()
                     
    return spark

def Process_immigration_data(spark,imm_data,output_data):
    """
    process immigration_data to extract  correct fields for fact_table 
    ,airline_dimension,visa_dimension,Flag_dimension,Person_dimension
    
    and write tables in praquet format 
    
    with three params:
    
    sparks: to connect to saprk_session
    
    imm_data: path to read data 
    
    out_data: path to save data in s3 bucket
    
    """
    #process some columns   
    immigration_data=spark.read.option("header","true").csv(imm_data)
    replace_column = udf(lambda x: 'Air' if x==str(1.0) else ('Sea' if x==str(2.0) else ('Land' if x==str(3.0) else 'Not reported')))
    replace_column1 = udf(lambda x: 'Business' if x==str(1.0) else ('Pleasure' if x==str(2.0) else  'Student'))
    replace_column2= udf(lambda x: 1 if x=="M" else 2)
    immigration_data=immigration_data.withColumn("model",replace_column(immigration_data.i94mode))\
                          .withColumn("visa_type",replace_column1(immigration_data.i94visa))\
                          .withColumn("person_id",replace_column2(immigration_data.gender)) 
    #Extract Fact Table
    fact_table=immigration_data.select(["cicid","i94yr","i94mon","i94cit","i94addr","arrdate","depdate","occup","i94visa"
                                            ,"airline","ENTDEPA","i94mode","person_id"])\
                            .withColumnRenamed("cicid","fact_id")\
                            .withColumnRenamed("i94yr","year")\
                            .withColumnRenamed("i94mon","month")\
                            .withColumnRenamed("i94cit","city_code")\
                            .withColumnRenamed("arrdate","arrival_date")\
                            .withColumnRenamed("depdate","departure_date")\
                            .withColumnRenamed("i94addr","state_code")\
                            .withColumnRenamed("arrdate","arrive_date")\
                            .withColumnRenamed("occup","Occupation")\
                            .withColumnRenamed("i94visa","Visa_id")\
                            .withColumnRenamed("airline","airline_code")\
                            .withColumnRenamed("ENTDEPA","flag_id")\
                            .withColumnRenamed("i94mode","model_id")
   
    get_date = udf(lambda x: (dt.datetime(1960, 1, 1).date() + dt.timedelta(float(x))).isoformat() if x else None)
    
    fact_table=fact_table.withColumn("arrival_date",get_date(fact_table.arrival_date))\
    .withColumn("departure_date",get_date(fact_table.departure_date))
    
    #Cleaning Fact_Table
    #I will drop Occupation and drop all missing data in rest of columns 
    fact_table=fact_table.drop("Occupation")
    
    fact_table=fact_table.dropna()\
                    .drop_duplicates()
    
    
    #Load Data
    fact_table=fact_table.write.parquet(output_data+"fact_table/", mode = "overwrite")
    
    #Extract airline_dimension
    airline_dim=immigration_data.select(["airline","admnum","fltno","visatype"])\
                    .withColumnRenamed("airline","airline_code")
    #Cleaning airline_dimension
    airline_dim=airline_dim.dropna()\
                    .drop_duplicates()
    
    #Load Data
    airline_dim=airline_dim.write.parquet(output_data+"airline_table/", mode = "overwrite")
    
    #Extract visa_dimension
    vis_dim=immigration_data.select(["i94visa","visa_type","visapost"])\
                .withColumnRenamed("i94visa","Visa_id")
    
    #Cleaning visa_dimension
    vis_dim=vis_dim.dropna()\
                    .drop_duplicates()
    #Load Data
    vis_dim=vis_dim.write.parquet(output_data+"visa_table/", mode = "overwrite")
    
    #Extract Flag_dimension
    Flag_dimm=immigration_data.select(["ENTDEPA","ENTDEPD","ENTDEPU","MATFLAG"])\
                .withColumnRenamed("ENTDEPA","flag_id")\
                .withColumnRenamed("ENTDEPD","Departure_Flag")\
                .withColumnRenamed("ENTDEPU","Update_Flag")\
                .withColumnRenamed("MATFLAG","Match_Flag")
    
    #Cleaning Flag_data
    Flag_dimm=Flag_dimm.drop("Update_Flag")
    Flag_dimm=Flag_dimm.dropna()\
                    .drop_duplicates()
                    
    #load Data
    Flag_dimm=Flag_dimm.write.parquet(output_data+"flag_table/", mode = "overwrite")
    
    
    #Extract Person_dimension
    Person_dimm=immigration_data.select(['biryear', 'gender', 'person_id'])\
                .withColumnRenamed("biryear","Birth_Year")
    
    #Cleaning Person_id
    
    Person_dimm=Person_dimm.dropna()\
                    .drop_duplicates()
    #load Data
    Person_dimm=Person_dimm.write.parquet(output_data+"person_table/", mode = "overwrite")
    
    #Extract Model_dimension
    model_dimm=immigration_data.select(["i94mode","Model"])\
                .withColumnRenamed("i94mode","model_id")
    #Cleaning Data
    model_dimm=model_dimm.dropna()\
                    .drop_duplicates()
    
    model_dimm=model_dimm.write.parquet(output_data+"model_table/", mode = "overwrite")
    
    
    
def Process_City_data(spark,city_data,output_data):
    """
    process log_data to extract  correct fields for City_data
    
    and write tables in praquet format 
    
    with three params:
    
    sparks: to connect to saprk_session
    
    city_data: path to read data 
    
    out_data: path to save data in s3 bucket
    """
    
    city_df=pd.read_csv(city_data,sep=';')
    state_dimm=city_df[["City","State","State Code","Race"]]
    state_dimm.columns=["City","State","state_code","Race"]
    
    pop_stats_dimm=city_df[['Median Age', 'Male Population', 'Female Population',
       'Total Population', 'Number of Veterans', 'Foreign-born',
       'Average Household Size', 'State Code']]
    
    pop_stats_dimm.columns=['Median_Age', 'Male_Population', 'Female_Population',
       'Total_Population', 'Number_Veterans', 'Foreign-born',
       'Average_Household', 'State_Code']
    
    pop_stats_dimm.dropna(inplace=True)
    pop_stats_dimm.drop_duplicates(inplace=True)
    
    #load Data
    state_dimm.to_csv("state_dimm")
    pop_stats_dimm.to_csv("pop_stats_dimm")
    
    state_dimm=spark.read.option("header","true").csv("state_dimm")
    pop_stats_dimm=spark.read.option("header","true").csv("pop_stats_dimm")
    
    state_dimm=state_dimm.select(["City","State","state_code","Race"])
    pop_stats_dimm=pop_stats_dimm.select(['Median_Age', 'Male_Population', 'Female_Population',
       'Total_Population', 'Number_Veterans', 'Foreign-born',
       'Average_Household', 'State_Code'])
    
    state_dimm=state_dimm.write.parquet(output_data+"state_table/", mode = "overwrite")
    pop_stats_dimm=pop_stats_dimm.write.parquet(output_data+"pop_stats_table/", mode = "overwrite")
     
def  Process_temp_data(spark,temp_data,output_data):
    """
    process log_data to extract  correct fields for Temp_dimension
    
    and write tables in praquet format 
    
    with three params:
    
    sparks: to connect to saprk_session
    
    temp_data: path to read data 
    
    out_data: path to save data in s3 bucket"""
    
    temp_df = pd.read_csv(temp_data)
    temp_dimm=temp_df[["dt","AverageTemperature","AverageTemperatureUncertainty","City","Country"]]
    temp_dimm.columns=["dt","Avg_Temp","Avg_Temp_Uncertainty","City","Country"]
    temp_dimm.dropna(inplace=True)
    
    temp_dimm.to_csv("temp_dimm")
   
   
    temp_dimm=spark.read.option("header","true").csv("temp_dimm")
    
    temp_dimm=temp_dimm.select(["dt","Avg_Temp","Avg_Temp_Uncertainty","City","Country"])
    temp_dimm=temp_dimm.write.parquet(output_data+"temp_table/", mode = "overwrite")
    
    
    
def main():
    
    """
    implement all function   here 
    
    """      
    spark = create_spark_session()
    temp_data= '../../data2/GlobalLandTemperaturesByCity.csv'
    city_data='us-cities-demographics.csv'
    imm_data='immigration_data_sample.csv'
    output_data = "s3a://awssparks195/"
    
    Process_immigration_data(spark,imm_data,output_data)
    
    Process_City_data(spark,city_data,output_data)
    
    Process_temp_data(spark,temp_data,output_data)
    
    
    
    
    
    
    
    
    
if __name__ == "__main__":
    main()

    
    