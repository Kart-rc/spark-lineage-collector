import pytest
import time
import os

def test_basic_lineage_sanitization(spark_session, lineage_events):
    """
    Scenario: Read CSV -> Write Parquet with Partitioning.
    Verify: The output dataset name should NOT contain the partition path (e.g., /dt=...)
            because we configured 'use_common_date_partition_pattern'.
    """
    # 1. Setup Data
    data = [("2023-01-01", "A", 10), ("2023-01-02", "B", 20)]
    columns = ["dt", "category", "value"]
    df = spark_session.createDataFrame(data, columns)
    
    # 2. Write Data (Partitioned)
    # We write to a temporary directory
    output_path = os.path.abspath("tests/scenarios/output/basic_test")
    
    # Ensure clean state
    import shutil
    if os.path.exists(output_path):
        shutil.rmtree(output_path)
        
    # Action that triggers lineage
    df.write.partitionBy("dt").mode("overwrite").parquet(output_path)
    
    # Wait a moment for events to be emitted
    time.sleep(2)
    
    # 3. Verify Lineage
    events = lineage_events()
    
    # Find the COMPLETE event for this job
    # We look for an event that has outputs
    found_sanitized_dataset = False
    
    for event in events:
        if event.get("eventType") == "COMPLETE":
            outputs = event.get("outputs", [])
            for output in outputs:
                name = output.get("name")
                # The name should be the base path, NOT containing /dt=...
                # The physical path would be .../basic_test/dt=2023-01-01
                # The sanitized name should be .../basic_test
                
                # Check if it matches our expected output path
                # Note: OpenLineage usually returns file:// + absolute path
                if output_path in name:
                    print(f"Found output dataset: {name}")
                    if "dt=" not in name:
                        found_sanitized_dataset = True
                    else:
                        pytest.fail(f"Dataset name was NOT sanitized: {name}")

    assert found_sanitized_dataset, "Did not find the expected sanitized output dataset in lineage events."
