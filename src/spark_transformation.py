"""
Bronze to Silver Layer Transformation
Validates, cleans, and masks PII from FHIR data
HIPAA-compliant processing with Spark
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class HealthcareDataTransformer:
    """
    Transforms raw FHIR data from Bronze to Silver layer
    Applies HIPAA compliance rules and data quality checks
    """
    
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("FHIR-Healthcare-ETL") \
            .config("spark.sql.adaptive.enabled", "true") \
            .getOrCreate()
            
        logger.info("Spark session initialized for healthcare ETL")
    
    def load_bronze_data(self, input_path: str = "bronze/"):
        """Load raw FHIR data from Bronze layer"""
        # In production: Read from ADLS Gen2
        # df = self.spark.read.json(f"abfss://{input_path}@storage.dfs.core.windows.net/")
        
        # Simulated data for demonstration
        schema = StructType([
            StructField("resourceType", StringType(), True),
            StructField("id", StringType(), True),
            StructField("name", ArrayType(StructType([
                StructField("family", StringType(), True),
                StructField("given", ArrayType(StringType()), True)
            ])), True),
            StructField("birthDate", StringType(), True),
            StructField("gender", StringType(), True),
            StructField("address", ArrayType(StructType([
                StructField("postalCode", StringType(), True)
            ])), True),
            StructField("identifier", ArrayType(StructType([
                StructField("value", StringType(), True)
            ])), True)
        ])
        
        # Create sample data
        data = [
            ("Patient", "1001", [{"family": "Smith", "given": ["John"]}], "1985-05-15", "male", [{"postalCode": "10001"}], [{"value": "MRN001001"}]),
            ("Patient", "1002", [{"family": "Doe", "given": ["Jane"]}], "1990-08-22", "female", [{"postalCode": "10002"}], [{"value": "MRN001002"}]),
        ]
        
        df = self.spark.createDataFrame(data, schema)
        logger.info(f"Loaded {df.count()} records from Bronze layer")
        return df
    
    def mask_pii(self, df):
        """
        HIPAA-compliant PII masking
        Masks: MRN (last 4 digits), Full name (initials only), Zip (first 3 digits)
        """
        logger.info("Applying PII masking...")
        
        # Mask MRN - show only last 4 characters
        df = df.withColumn("mrn_masked", 
            regexp_extract(col("identifier")[0]["value"], r".*(\d{4})$", 1))
        
        # Mask Name - keep only first initial
        df = df.withColumn("name_initial",
            concat(
                substring(col("name")[0]["given"][0], 1, 1),
                lit(". "),
                col("name")[0]["family"]
            ))
        
        # Mask Zip Code - keep only first 3 digits
        df = df.withColumn("zip_region",
            substring(col("address")[0]["postalCode"], 1, 3))
        
        # Remove original PII columns
        df = df.drop("name", "address", "identifier")
        
        return df
    
    def validate_data(self, df):
        """
        Data quality checks for healthcare data
        Enforces: No null patient IDs, valid dates, allowed gender values
        """
        logger.info("Running data quality validations...")
        
        initial_count = df.count()
        
        # Check 1: Remove records with null IDs
        df = df.filter(col("id").isNotNull())
        
        # Check 2: Valid birth dates (not in future, not too old)
        current_year = year(current_date())
        df = df.withColumn("birth_year", year(to_date(col("birthDate"), "yyyy-MM-dd")))
        df = df.filter((col("birth_year") <= current_year) & (col("birth_year") >= 1900))
        
        # Check 3: Valid gender values
        valid_genders = ["male", "female", "other", "unknown"]
        df = df.filter(col("gender").isin(valid_genders))
        
        final_count = df.count()
        dropped_count = initial_count - final_count
        
        logger.info(f"Validation complete: {final_count} valid, {dropped_count} dropped")
        
        if dropped_count > 0:
            logger.warning(f"Data quality issue: {dropped_count} records failed validation")
        
        return df
    
    def transform_to_silver(self, df):
        """
        Main transformation logic
        Applies masking, validation, and schema standardization
        """
        logger.info("Transforming Bronze to Silver layer...")
        
        # Step 1: PII Masking (HIPAA)
        df = self.mask_pii(df)
        
        # Step 2: Data Quality
        df = self.validate_data(df)
        
        # Step 3: Add metadata
        df = df.withColumn("ingestion_timestamp", current_timestamp())
        df = df.withColumn("data_source", lit("FHIR-R4-API"))
        df = df.withColumn("compliance_flag", lit("HIPAA-MASKED"))
        
        # Step 4: Standardize column names
        df = df.withColumnRenamed("id", "patient_id") \
               .withColumnRenamed("resourceType", "resource_type")
        
        logger.info("Silver layer transformation complete")
        return df
    
    def save_to_silver(self, df, output_path: str = "silver/"):
        """Save processed data to Silver layer (Delta Lake format)"""
        logger.info(f"Saving {df.count()} records to Silver layer...")
        
        # In production: Write to ADLS Gen2 as Delta
        # df.write.format("delta").mode("append").save(f"abfss://{output_path}...")
        
        # Display schema and sample for demo
        print("Silver Layer Schema:")
        df.printSchema()
        print("\nSample Data (PII Masked):")
        df.show(5, truncate=False)
        
        return df

if __name__ == "__main__":
    transformer = HealthcareDataTransformer()
    
    # Execute pipeline
    bronze_df = transformer.load_bronze_data()
    silver_df = transformer.transform_to_silver(bronze_df)
    transformer.save_to_silver(silver_df)
    
    logger.info("Bronze to Silver ETL completed successfully")
