import pytest
import time
import os

def test_multi_destination_lineage(spark_session, lineage_events):
    """
    Scenario: Read -> Split -> Write to A and B.
    Verify: Lineage captures two distinct output datasets.
    """
    # 1. Setup Data
    data = [("1", "A"), ("2", "B")]
    df = spark_session.createDataFrame(data, ["id", "val"])
    
    # 2. Write to two locations
    out_a = os.path.abspath("tests/scenarios/output/multi_a")
    out_b = os.path.abspath("tests/scenarios/output/multi_b")
    
    # Clean
    import shutil
    for p in [out_a, out_b]:
        if os.path.exists(p):
            shutil.rmtree(p)
            
    # We perform two separate writes. 
    # Note: In Spark, these are two separate jobs usually, unless we use something like multi-write (not standard in vanilla DF API without caching/actions).
    # We'll just do two writes sequentially.
    df.filter("id = '1'").write.mode("overwrite").csv(out_a)
    df.filter("id = '2'").write.mode("overwrite").csv(out_b)
    
    time.sleep(2)
    
    # 3. Verify
    events = lineage_events()
    
    found_a = False
    found_b = False
    
    for event in events:
        if event.get("eventType") == "COMPLETE":
            outputs = event.get("outputs", [])
            for output in outputs:
                name = output.get("name")
                if out_a in name:
                    found_a = True
                if out_b in name:
                    found_b = True
                    
    assert found_a, "Did not find output A in lineage"
    assert found_b, "Did not find output B in lineage"
