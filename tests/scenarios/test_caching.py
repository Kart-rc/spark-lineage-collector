import pytest
import time
import os

def test_caching_lineage(spark_session, lineage_events):
    """
    Scenario: Read -> Cache -> Count -> Write.
    Verify: Lineage is preserved even when reading from cache.
    """
    # 1. Setup Data
    data = [("1", "A"), ("2", "B")]
    df = spark_session.createDataFrame(data, ["id", "val"])
    
    # 2. Cache and Action
    df = df.cache()
    count = df.count() # Trigger cache materialization
    
    # 3. Write
    out_path = os.path.abspath("tests/scenarios/output/caching_test")
    import shutil
    if os.path.exists(out_path):
        shutil.rmtree(out_path)
        
    df.write.mode("overwrite").json(out_path)
    
    time.sleep(2)
    
    # 4. Verify
    events = lineage_events()
    
    found_output = False
    
    for event in events:
        if event.get("eventType") == "COMPLETE":
            outputs = event.get("outputs", [])
            for output in outputs:
                if out_path in output.get("name"):
                    found_output = True
                    
    assert found_output, "Lineage broken by caching? Output not found."
