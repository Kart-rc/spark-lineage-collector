import sys
import os

# Add src to path for demo purposes
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from spark_lineage_collector.config import LineageConfig
from spark_lineage_collector.facets import CustomFacetHelper

def main():
    # 1. Initialize the configuration builder
    config = LineageConfig()

    # 2. Strategy 1: Sanitization
    # Remove date partitions to avoid dataset explosion
    config.sanitization.use_common_date_partition_pattern()
    # Exclude user development workspaces
    config.sanitization.add_exclude_pattern("hdfs://user/.*")

    # --- Custom Pattern Examples ---
    # Example 1: Remove ID-based partitions (e.g., /user_id=12345/...)
    # Regex: (.*)(?<remove>/user_id=\d+.*)
    # config.sanitization.set_remove_path_pattern(r"(.*)(?<remove>/user_id=\d+.*)")

    # Example 2: Remove nested partitions (e.g., /region=US/year=2023/...)
    # Regex: (.*)(?<remove>/region=[^/]+/year=\d+.*)
    # config.sanitization.set_remove_path_pattern(r"(.*)(?<remove>/region=[^/]+/year=\d+.*)")


    # 3. Strategy 2: Namespace Normalization
    # Map all kafka brokers to a single logical cluster name
    config.resolvers.add_host_list_resolver(
        name="kafka_prod",
        hosts=["broker1.internal", "broker2.internal"],
        resolved_name="production-kafka"
    )
    
    # 4. Configure Transport (for testing we use console)
    config.set_transport_type("console")
    
    # 5. Generate the Spark Configuration
    spark_conf = config.get_spark_config()
    
    print("--- Generated Spark Configuration ---")
    for k, v in spark_conf.items():
        print(f"{k} = {v}")
        
    # In a real Spark app, you would do:
    # from pyspark.sql import SparkSession
    # builder = SparkSession.builder
    # for k, v in spark_conf.items():
    #     builder.config(k, v)
    # spark = builder.getOrCreate()
    
    # 6. Strategy 3: Custom Facets (Example usage)
    # When writing a DataFrame, you can inject custom metadata
    print("\n--- Example Custom Facet Injection ---")
    custom_meta = {"owner": "data-governance-team", "classification": "PII"}
    facet_json = CustomFacetHelper.create_custom_facet("governance", custom_meta)
    
    print(f"To inject governance metadata, use:")
    print(f'df.write.option("openlineage.dataset.facets.governance", \'{facet_json}\').save(...)')

if __name__ == "__main__":
    main()
