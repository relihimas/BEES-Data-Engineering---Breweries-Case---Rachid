# Bronze Layer: Persist the raw data from the API in its native format or any format you find suitable.
import json
from datetime import datetime
from pyspark import SparkContext
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *


def quality_gate(body) -> bool:
    """
    Simple quality gate function to check if the fetched data meets basic quality criteria.
    In this case, we check if the body contains at least one brewery entry.
    """
    return len(body) > 0

def bronze_extraction():
    """
    Fetch data from the generated file from the raw payload extraction phase.
    Apply a quality gate to ensure data integrity before proceeding.
    Save data to the bronze layer.
    """
    try:

        spark = SparkSession.builder.getOrCreate()

        df_raw = spark.read.json("bees_listbreweries_2025-11-28.json")

        df_raw.printSchema()
        df_raw.show()

        # checar se a tabela existe
        # 

        # return f"Data successfully saved to bronze layer in {bronze_file_name}"

    except Exception as e:
        return str(e)
    
bronze_extraction()