from typing import List, Dict

class NamespaceResolverConfig:
    """
    Configuration for Strategy 2: Namespace Normalization.
    Handles hostList and pattern resolvers.
    """
    def __init__(self):
        self._resolvers: Dict[str, dict] = {}

    def add_host_list_resolver(self, name: str, hosts: List[str], resolved_name: str) -> 'NamespaceResolverConfig':
        """
        Adds a hostList resolver.
        Useful for clusters (Kafka, Cassandra) where multiple hosts map to one logical cluster.
        """
        self._resolvers[name] = {
            "type": "hostList",
            "hosts": hosts,
            "resolvedName": resolved_name
        }
        return self

    def add_pattern_resolver(self, name: str, regex: str, resolved_name: str) -> 'NamespaceResolverConfig':
        """
        Adds a pattern resolver.
        Useful for naming conventions (e.g., db-prod-01.company.com -> production-db).
        """
        self._resolvers[name] = {
            "type": "pattern",
            "regex": regex,
            "resolvedName": resolved_name
        }
        return self

    def get_spark_properties(self) -> dict:
        """
        Returns the dictionary of Spark properties for this configuration.
        """
        props = {}
        for name, config in self._resolvers.items():
            prefix = f"spark.openlineage.dataset.namespaceResolvers.{name}"
            props[f"{prefix}.type"] = config["type"]
            props[f"{prefix}.resolvedName"] = config["resolvedName"]
            
            if config["type"] == "hostList":
                # OpenLineage expects a JSON-like list string: ["h1", "h2"]
                # We need to format it carefully.
                import json
                props[f"{prefix}.hosts"] = json.dumps(config["hosts"])
            elif config["type"] == "pattern":
                props[f"{prefix}.regex"] = config["regex"]
                
        return props
