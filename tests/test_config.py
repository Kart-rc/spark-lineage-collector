import unittest
import json
from spark_lineage_collector.config import LineageConfig

class TestLineageConfig(unittest.TestCase):
    
    def test_sanitization_defaults(self):
        config = LineageConfig()
        config.sanitization.use_common_date_partition_pattern()
        config.sanitization.add_exclude_pattern("hdfs://user/.*")
        
        props = config.get_spark_config()
        
        self.assertEqual(props["spark.openlineage.dataset.removePath.pattern"], r"(.*)(?<remove>/(dt|date)=.*)")
        self.assertEqual(props["spark.openlineage.dataset.exclude"], "hdfs://user/.*")

    def test_namespace_resolvers(self):
        config = LineageConfig()
        config.resolvers.add_host_list_resolver("kafka", ["b1", "b2"], "prod-kafka")
        config.resolvers.add_pattern_resolver("db", "db-.*", "prod-db")
        
        props = config.get_spark_config()
        
        self.assertEqual(props["spark.openlineage.dataset.namespaceResolvers.kafka.type"], "hostList")
        self.assertEqual(props["spark.openlineage.dataset.namespaceResolvers.kafka.resolvedName"], "prod-kafka")
        self.assertEqual(json.loads(props["spark.openlineage.dataset.namespaceResolvers.kafka.hosts"]), ["b1", "b2"])
        
        self.assertEqual(props["spark.openlineage.dataset.namespaceResolvers.db.type"], "pattern")
        self.assertEqual(props["spark.openlineage.dataset.namespaceResolvers.db.regex"], "db-.*")

    def test_full_config(self):
        config = LineageConfig()
        config.set_transport_type("console")
        config.disable_facet("spark.logicalPlan")
        
        props = config.get_spark_config()
        
        self.assertEqual(props["spark.openlineage.transport.type"], "console")
        self.assertEqual(props["spark.openlineage.facets.spark.logicalPlan.disabled"], "true")

if __name__ == '__main__':
    unittest.main()
