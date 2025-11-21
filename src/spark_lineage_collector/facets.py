import json
from typing import Dict, Any

class CustomFacetHelper:
    """
    Configuration for Strategy 3: Custom Facets (via properties).
    Allows injecting custom metadata into datasets.
    """
    
    @staticmethod
    def create_custom_facet(name: str, content: Dict[str, Any]) -> str:
        """
        Creates a JSON string for a custom facet.
        Usage:
        df.write.option(f"openlineage.dataset.facets.{name}", CustomFacetHelper.create_custom_facet(name, {...}))
        """
        return json.dumps(content)

    @staticmethod
    def create_symlink_facet(identifiers: list) -> str:
        """
        Creates a SymlinksDatasetFacet JSON string.
        identifiers: List of dicts with keys 'namespace', 'name', 'type'.
        """
        return json.dumps({
            "identifiers": identifiers
        })
