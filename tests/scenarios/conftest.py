import pytest
import os
import shutil
import json
import time
from pyspark.sql import SparkSession
from spark_lineage_collector.config import LineageConfig

# Directory to store OpenLineage events
EVENTS_DIR = os.path.join(os.path.dirname(__file__), "events")

@pytest.fixture(scope="session")
def spark_session():
    """
    Creates a SparkSession with OpenLineage configured to write to a local file.
    """
    # Clean up events directory
    if os.path.exists(EVENTS_DIR):
        shutil.rmtree(EVENTS_DIR)
    os.makedirs(EVENTS_DIR)

    # Define the OpenLineage jar version
    # Using 1.18.0 as it is stable and compatible with Spark 3.x
    # Note: In a real environment, this might need to be adjusted based on Spark version
    # For PySpark 4.0.1 (which uv installed), we might need a newer OL version or stick to Spark 3.5
    # Let's try to use a recent version.
    
    # Using 1.18.0 as it is stable and compatible with Spark 3.x
    maven_coords = "io.openlineage:openlineage-spark_2.12:1.18.0"

    builder = SparkSession.builder \
        .appName("TDD_Lineage_Tests") \
        .master("local[*]") \
        .config("spark.jars.packages", maven_coords) \
        .config("spark.extraListeners", "io.openlineage.spark.agent.OpenLineageSparkListener") \
        .config("spark.openlineage.transport.type", "file") \
        .config("spark.openlineage.transport.location", os.path.join(EVENTS_DIR, "lineage.json"))
        
    # Apply our library's configuration logic (Sanitization)
    lineage_config = LineageConfig()
    lineage_config.sanitization.use_common_date_partition_pattern()
    lineage_config.sanitization.use_spark_temporary_dir_pattern()
    
    # Apply configs
    lineage_config.apply_to_spark_conf(builder)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("INFO")
    
    # Debug: Print Spark Conf
    print("Spark Conf:")
    print(spark.sparkContext.getConf().getAll())
    
    yield spark
    spark.stop()

@pytest.fixture
def lineage_events():
    """
    Fixture to read the emitted lineage events.
    """
    def get_events():
        events = []
        if not os.path.exists(EVENTS_DIR):
            return []
            
        for filename in sorted(os.listdir(EVENTS_DIR)):
            if filename.endswith(".json"):
                with open(os.path.join(EVENTS_DIR, filename), 'r') as f:
                    try:
                        # The file transport writes one JSON per line or file? 
                        # Usually file transport writes one file per event or appends.
                        # Let's assume line-delimited JSON if appended, or single JSON if separate files.
                        # OpenLineage FileTransport usually writes one line per event in a single file 
                        # or multiple files depending on config.
                        # Let's read line by line.
                        for line in f:
                            if line.strip():
                                events.append(json.loads(line))
                    except json.JSONDecodeError:
                        pass
        return events
    return get_events

@pytest.fixture
def clear_events():
    """
    Clears the events directory between tests.
    """
    if os.path.exists(EVENTS_DIR):
        # We can't easily delete the file if Spark is holding it open.
        # Instead, we might just track new events.
        # For simplicity in this TDD, we'll just read all events and filter by job name in tests.
        pass
