# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col
from snowflake.snowpark.types import IntegerType
from snowflake.snowpark.functions import udf

def main(session: snowpark.Session): 
    
    
    # Your code goes here, inside the "main" handler.
    tableName = "TITANIC_ETL.PUBLIC.TITANIC_RAW_DATASET"
    raw_dataframe = session.table(tableName)

    # Print a sample of the dataframe to standard output.
    raw_dataframe.show()

    removed_column_data = raw_dataframe.drop(col('Name'), col('Cabin'))
    print("delete operation")

    removed_column_data = removed_column_data.with_column_renamed(col('Ticket'), 'Ticket Number')

    # try to apply filter condition
    removed_column_data = removed_column_data.filter((col('AGE')>18)& (col('PCLASS')==1)).select(col('PASSENGERID'), col('SURVIVED'), col('PCLASS'), col('SEX'))
    
 

    removed_column_data.show()
    # Return value will appear in the Results tab.

    @udf(name="minus_one", is_permanent=True, stage_location="@raw_data_stage2", replace=True)
    def minus_one(x: int) -> int:
        return x+3

    @udf(name="map_class", is_permanent=True, stage_location="@raw_data_stage2", replace=True)
    def map_class(x: int) -> str:
        class_dic={1:'First Class', 2:'Second Class', 3:'Economy'}
        return class_dic[x]

    removed_column_data = removed_column_data.withColumn('new', map_class(col('PCLASS')))


    # saving the transformed data to a snowflake table
    removed_column_data.write.mode('overwrite').save_as_table('Titanic_Passenger_Final_Data')
    table_data = session.sql("""select * from Titanic_Passenger_Final_Data""")
    return  table_data
 
     
