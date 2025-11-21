from typing import List, Optional

class SanitizationConfig:
    """
    Configuration for Strategy 1: Dataset Sanitization.
    Handles removePath.pattern and dataset exclusion/inclusion.
    """
    def __init__(self):
        self._remove_path_pattern: Optional[str] = None
        self._exclude_patterns: List[str] = []
        self._include_patterns: List[str] = []

    def set_remove_path_pattern(self, pattern: str) -> 'SanitizationConfig':
        """
        Sets the regex pattern to remove from dataset paths.
        The pattern must contain a named capturing group 'remove'.
        Example: (.*)(?<remove>/dt=.*)
        """
        if "(?<remove>" not in pattern:
             # Basic validation, though user might use different regex syntax if they know what they are doing, 
             # but for OpenLineage Java regex, this is required.
             pass 
        self._remove_path_pattern = pattern
        return self

    def add_exclude_pattern(self, pattern: str) -> 'SanitizationConfig':
        """
        Adds a regex pattern to exclude datasets.
        Example: hdfs://user/.*
        """
        self._exclude_patterns.append(pattern)
        return self

    def add_include_pattern(self, pattern: str) -> 'SanitizationConfig':
        """
        Adds a regex pattern to include datasets. 
        If specified, only datasets matching these patterns are included.
        """
        self._include_patterns.append(pattern)
        return self

    # --- Pre-defined patterns for common use cases ---

    def use_common_date_partition_pattern(self) -> 'SanitizationConfig':
        """
        Sets a pattern to remove standard date partitions like /dt=2023-01-01 or /date=...
        Regex: (.*)(?<remove>/(dt|date)=.*)
        """
        return self.set_remove_path_pattern(r"(.*)(?<remove>/(dt|date)=.*)")

    def use_common_hive_partition_pattern(self) -> 'SanitizationConfig':
        """
        Sets a pattern to remove standard Hive style partitions like /year=2023/month=10
        Regex: (.*)(?<remove>/[a-zA-Z0-9_]+=.*)
        """
        return self.set_remove_path_pattern(r"(.*)(?<remove>/[a-zA-Z0-9_]+=.*)")
    
    def use_spark_temporary_dir_pattern(self) -> 'SanitizationConfig':
        """
        Sets a pattern to remove Spark temporary directories like /_temporary/0/...
        Regex: (.*)(?<remove>/_temporary.*)
        """
        return self.set_remove_path_pattern(r"(.*)(?<remove>/_temporary.*)")

    def get_spark_properties(self) -> dict:
        """
        Returns the dictionary of Spark properties for this configuration.
        """
        props = {}
        if self._remove_path_pattern:
            props["spark.openlineage.dataset.removePath.pattern"] = self._remove_path_pattern
        
        if self._exclude_patterns:
            props["spark.openlineage.dataset.exclude"] = ",".join(self._exclude_patterns)
            
        if self._include_patterns:
            props["spark.openlineage.dataset.include"] = ",".join(self._include_patterns)
            
        return props
