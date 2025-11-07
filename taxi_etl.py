"""
NYC Taxi Analytics Pipeline - Production Grade Data Engineering
Author: Seu Nome
Description: ETL pipeline processing 1.6B+ records with Data Quality Framework
"""

# Standard imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import json
from datetime import datetime, timedelta


class DataQualityFramework:
    """Production Data Quality Framework"""
    
    @staticmethod
    def check_null_rates(df, threshold=0.1):
        """Calculate null rates and alert if above threshold"""
        null_rates = {}
        total_count = df.count()
        
        for column in df.columns:
            null_count = df.filter(col(column).isNull()).count()
            null_rate = null_count / total_count if total_count > 0 else 0
            null_rates[column] = null_rate
            
            if null_rate > threshold:
                print(f"⚠️  ALERT: Column {column} has {null_rate:.2%} null values")
        
        return null_rates
    
    @staticmethod
    def validate_ranges(df, rules):
        """Validate if values are within expected ranges"""
        violations = {}
        for column, rule in rules.items():
            if column in df.columns:
                min_val = rule.get("min")
                max_val = rule.get("max")
                
                if min_val is not None:
                    count_below = df.filter(col(column) < min_val).count()
                    if count_below > 0:
                        violations[f"{column}_below_min"] = count_below
                
                if max_val is not None:
                    count_above = df.filter(col(column) > max_val).count()
                    if count_above > 0:
                        violations[f"{column}_above_max"] = count_above
        
        return violations


class NYCTaxiPipeline:
    """Main NYC Taxi Analytics Pipeline Class"""
    
    def __init__(self):
        self.spark = self._create_spark_session()
        self.TAXI_SCHEMA = self._define_schema()
        self.DATA_QUALITY_RULES = {
            "fare_amount": {"min": 0, "max": 1000},
            "trip_distance": {"min": 0, "max": 100},
            "passenger_count": {"min": 1, "max": 9}
        }
    
    def _create_spark_session(self):
        """Create optimized Spark session"""
        return SparkSession.builder \
            .appName("gold_nyc_taxi_analytics") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.adaptive.skew.enabled", "true") \
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
            .getOrCreate()
    
    def _define_schema(self):
        """Define data schema contract"""
        return StructType([
            StructField("vendor_id", StringType(), True),
            StructField("pickup_datetime", TimestampType(), True),
            StructField("dropoff_datetime", TimestampType(), True),
            StructField("passenger_count", IntegerType(), True),
            StructField("trip_distance", DoubleType(), True),
            StructField("pickup_longitude", DoubleType(), True),
            StructField("pickup_latitude", DoubleType(), True),
            StructField("rate_code", StringType(), True),
            StructField("store_and_fwd", StringType(), True),
            StructField("dropoff_longitude", DoubleType(), True),
            StructField("dropoff_latitude", DoubleType(), True),
            StructField("payment_type", StringType(), True),
            StructField("fare_amount", DoubleType(), True),
            StructField("surcharge", DoubleType(), True),
            StructField("mta_tax", DoubleType(), True),
            StructField("tip_amount", DoubleType(), True),
            StructField("tolls_amount", DoubleType(), True),
            StructField("total_amount", DoubleType(), True)
        ])
    
    def ingest_data(self):
        """Data ingestion with quality checks"""
        print("📥 Starting data ingestion...")
        
        try:
            raw_df = self.spark.read.format("delta").load(
                "dbfs:/databricks-datasets/nyctaxi/tables/nyctaxi_yellow"
            )
            print(f"📊 Total raw records: {raw_df.count():,}")
            
            # Apply schema validation
            df_validated = raw_df.select([
                col(field.name).cast(field.dataType) 
                for field in self.TAXI_SCHEMA 
                if field.name in raw_df.columns
            ])
            
            # Data quality checks
            dq = DataQualityFramework()
            null_rates = dq.check_null_rates(df_validated)
            violations = dq.validate_ranges(df_validated, self.DATA_QUALITY_RULES)
            
            print("🔍 Data Quality Metrics:")
            for col_name, null_rate in null_rates.items():
                print(f"   - {col_name}: {null_rate:.2%} nulls")
                
            return df_validated
            
        except Exception as e:
            print(f"❌ Error during data ingestion: {str(e)}")
            raise
    
    def transform_data(self, df):
        """Apply business transformations"""
        print("🔄 Applying transformations...")
        
        # Calculate trip duration
        df_with_duration = df.withColumn(
            "trip_duration_minutes", 
            round((col("dropoff_datetime").cast("long") - col("pickup_datetime").cast("long")) / 60, 2)
        )
        
        # Calculate average speed
        df_with_speed = df_with_duration.withColumn(
            "avg_speed_mph",
            when(col("trip_duration_minutes") > 0, 
                 round(col("trip_distance") / (col("trip_duration_minutes") / 60), 2)
            ).otherwise(0)
        )
        
        # Add temporal features
        df_with_features = df_with_speed \
            .withColumn("pickup_date", to_date(col("pickup_datetime"))) \
            .withColumn("pickup_hour", hour(col("pickup_datetime"))) \
            .withColumn("day_of_week", dayofweek(col("pickup_datetime"))) \
            .withColumn("year", year(col("pickup_datetime"))) \
            .withColumn("month", month(col("pickup_datetime"))) \
            .withColumn("is_weekend", when(col("day_of_week").isin(1, 7), 1).otherwise(0))
        
        # Calculate data quality score
        quality_expr = """
            CASE 
                WHEN fare_amount > 0 AND trip_distance > 0 AND trip_duration_minutes > 0 THEN 1.0
                WHEN fare_amount > 0 AND trip_distance > 0 THEN 0.8
                WHEN fare_amount > 0 THEN 0.5
                ELSE 0.2
            END
        """
        df_with_quality = df_with_features.withColumn("data_quality_score", expr(quality_expr))
        
        # Filter invalid data
        df_final = df_with_quality.filter(
            (col("fare_amount") > 0) &
            (col("trip_distance") > 0) &
            (col("trip_duration_minutes") > 0) &
            (col("trip_duration_minutes") < 180) &
            (col("avg_speed_mph") < 100) &
            (col("data_quality_score") >= 0.5)
        )
        
        print(f"✅ Transformations applied. Records: {df_final.count():,}")
        return df_final
    
    def create_business_views(self, df):
        """Create analytical views for business intelligence"""
        print("👔 Creating business views...")
        
        # Gold table
        gold_df = df.select(
            "vendor_id", "pickup_date", "pickup_hour", "passenger_count",
            "trip_distance", "fare_amount", "total_amount", 
            "trip_duration_minutes", "avg_speed_mph", "payment_type",
            col("data_quality_score").cast("double"), "year", "month"
        )
        
        # In production, this would write to Delta tables
        print("✅ Business views logic implemented")
        return gold_df
    
    def generate_insights(self, df):
        """Generate business insights"""
        print("📈 Generating business insights...")
        
        # Sample insights calculation
        hourly_metrics = df.groupBy("pickup_hour").agg(
            count("*").alias("total_trips"),
            avg("fare_amount").alias("avg_fare"),
            sum("total_amount").alias("total_revenue")
        ).orderBy("pickup_hour")
        
        vendor_metrics = df.groupBy("vendor_id").agg(
            count("*").alias("total_trips"),
            avg("fare_amount").alias("avg_fare"),
            sum("total_amount").alias("total_revenue")
        ).orderBy(col("total_trips").desc())
        
        print("🏙️  Peak hours analysis:")
        hourly_metrics.show(10)
        
        print("🚗 Top vendors analysis:")
        vendor_metrics.show(5)
        
        return {
            "hourly_metrics": hourly_metrics,
            "vendor_metrics": vendor_metrics
        }
    
    def run_pipeline(self):
        """Execute complete pipeline"""
        print("🚀 Starting NYC Taxi Analytics Pipeline...")
        
        try:
            # Execute pipeline steps
            raw_data = self.ingest_data()
            transformed_data = self.transform_data(raw_data)
            business_views = self.create_business_views(transformed_data)
            insights = self.generate_insights(transformed_data)
            
            print("✨ PIPELINE EXECUTED SUCCESSFULLY!")
            print(f"📊 Final records: {transformed_data.count():,}")
            
            return {
                "status": "success",
                "records_processed": transformed_data.count(),
                "insights": insights
            }
            
        except Exception as e:
            print(f"❌ Pipeline failed: {str(e)}")
            return {"status": "failed", "error": str(e)}


def main():
    """Main execution function"""
    pipeline = NYCTaxiPipeline()
    results = pipeline.run_pipeline()
    print(f"Pipeline results: {results}")


if __name__ == "__main__":
    main()