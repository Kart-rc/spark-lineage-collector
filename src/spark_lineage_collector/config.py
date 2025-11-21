from typing import Dict, Optional
from .sanitization import SanitizationConfig
from .resolvers import NamespaceResolverConfig

class LineageConfig:
    """
    Main entry point for configuring OpenLineage Spark integration.
    Aggregates Sanitization and Namespace Resolver configurations.
    """
    def __init__(self):
        self.sanitization = SanitizationConfig()
        self.resolvers = NamespaceResolverConfig()
        self._extra_props: Dict[str, str] = {}

    def set_transport_type(self, type: str) -> 'LineageConfig':
        """
        Sets the transport type (e.g., 'console', 'http', 'kafka').
        """
        self._extra_props["spark.openlineage.transport.type"] = type
        return self
    
    def set_transport_url(self, url: str) -> 'LineageConfig':
        """
        Sets the transport URL (for http transport).
        """
        self._extra_props["spark.openlineage.transport.url"] = url
        return self
    
    def set_api_key(self, key: str) -> 'LineageConfig':
        """
        Sets the API key (for http transport).
        """
        self._extra_props["spark.openlineage.transport.apiKey"] = key
        return self
    
    def disable_facet(self, facet_name: str) -> 'LineageConfig':
        """
        Disables a specific facet to reduce payload size.
        Example: spark.logicalPlan
        """
        self._extra_props[f"spark.openlineage.facets.{facet_name}.disabled"] = "true"
        return self

    def get_spark_config(self) -> Dict[str, str]:
        """
        Generates the full dictionary of Spark properties.
        """
        props = {}
        props.update(self.sanitization.get_spark_properties())
        props.update(self.resolvers.get_spark_properties())
        props.update(self._extra_props)
        return props

    def apply_to_spark_conf(self, conf) -> None:
        """
        Applies the configuration to a PySpark SparkConf or SparkSession.Builder object.
        """
        for k, v in self.get_spark_config().items():
            if hasattr(conf, "config"):
                conf.config(k, v)
            else:
                conf.set(k, v)
