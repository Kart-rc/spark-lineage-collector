import pytest
import time
import os

def test_join_lineage(spark_session, lineage_events):
    """
    Scenario: Read Table A, Read Table B -> Join -> Write Table C.
    Verify: Lineage captures both inputs A and B and connects them to output C.
    """
    # 1. Setup Data
    data_a = [("1", "Alice"), ("2", "Bob")]
    df_a = spark_session.createDataFrame(data_a, ["id", "name"])
    
    data_b = [("1", "Engineering"), ("2", "Sales")]
    df_b = spark_session.createDataFrame(data_b, ["id", "dept"])
    
    # 2. Write Inputs (to simulate reading from files)
    path_a = os.path.abspath("tests/scenarios/input/table_a")
    path_b = os.path.abspath("tests/scenarios/input/table_b")
    path_c = os.path.abspath("tests/scenarios/output/table_c")
    
    import shutil
    for p in [path_a, path_b, path_c]:
        if os.path.exists(p):
            shutil.rmtree(p)
            
    df_a.write.parquet(path_a)
    df_b.write.parquet(path_b)
    
    # 3. Read and Join
    read_a = spark_session.read.parquet(path_a)
    read_b = spark_session.read.parquet(path_b)
    
    joined = read_a.join(read_b, "id")
    
    # 4. Write Output
    joined.write.mode("overwrite").parquet(path_c)
    
    time.sleep(2)
    
    # 5. Verify
    events = lineage_events()
    
    found_join_event = False
    
    for event in events:
        if event.get("eventType") == "COMPLETE":
            inputs = event.get("inputs", [])
            outputs = event.get("outputs", [])
            
            input_names = [i.get("name") for i in inputs]
            output_names = [o.get("name") for o in outputs]
            
            # Check if this event has our output C
            has_output_c = any(path_c in name for name in output_names)
            
            if has_output_c:
                # Check if it has both inputs A and B
                has_input_a = any(path_a in name for name in input_names)
                has_input_b = any(path_b in name for name in input_names)
                
                if has_input_a and has_input_b:
                    found_join_event = True
                    
    assert found_join_event, "Did not find a lineage event connecting Inputs A & B to Output C"
