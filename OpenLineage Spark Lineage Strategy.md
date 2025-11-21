

# **Architectural Strategies for High-Fidelity Lineage: Sanitization and logical Abstraction in OpenLineage Spark Integration**

## **1\. Introduction: The Signal-to-Noise Paradox in Distributed Data Processing**

The evolution of modern data architecture, transitioning from rigid, monolithic Data Warehouses to flexible, distributed Data Lakes and Lakehouses, has democratized data access but simultaneously introduced a crisis of observability. In the era of the Data Warehouse, lineage was implicit; the SQL queries executed within the confines of a single engine (e.g., Teradata, Oracle) defined the relationships between tightly coupled, pre-defined tables. The migration to Apache Spark and object storage (S3, ADLS, GCS) decoupled compute from storage, granting immense scalability but shattering the implicit lineage graph.  
In this distributed paradigm, the "truth" of data movement is no longer a logical INSERT INTO table statement, but a physical manifestation of file operations: thousands of executors writing parquet files to temporary directories, shuffling blocks across the network, and renaming partitions upon job commitment.1 For a Data Architect or Governance Officer, this shift presents a fundamental paradox: the system generates orders of magnitude more metadata (telemetry), yet the clarity of the logical data flow (lineage) degrades significantly. A lineage graph populated by physical artifacts—such as ephemeral partition paths, temporary staging directories, and dynamic execution identifiers—becomes a map of *infrastructure noise* rather than a map of *data provenance*.  
This report provides a comprehensive, expert-level analysis of the mechanisms available within the OpenLineage Spark integration to resolve this paradox. We will explore the architectural depth of the OpenLineageSparkListener, the necessity of dataset sanitization, and the advanced implementation of logical abstractions. The objective is to guide the transformation of a raw, noisy execution log into a pristine, logical lineage graph that serves the needs of data governance, compliance (GDPR, BCBS 239), and impact analysis. We will focus specifically on three pillars of high-fidelity lineage: **Sanitization** (filtering temporary artifacts), **Normalization** (customizing namespaces), and **Abstraction** (injecting logical Symlinks).

### **1.1 The Operational Context of Spark Metadata**

To understand the necessity of sanitization, one must appreciate the operational reality of a Spark job. When a simple ETL process runs—reading a year's worth of sales data and aggregating it by region—the physical execution involves complex stages. Spark breaks the logical job into stages; stages are broken into tasks. Data is read from hundreds of distinct objects in S3, shuffled across the cluster (generating intermediate disk spills), and written to a temporary staging directory before being atomically committed to the final destination.1  
If a lineage tool naively captures every "read" and "write" event reported by the file system, the resulting graph for a single job might contain thousands of nodes.

* **The Partition Explosion:** A daily job writing to s3://bucket/data/date=2023-10-27 creates a new dataset node every day. Over a year, this single logical table becomes 365 distinct nodes in the graph, rendering impact analysis impossible.3  
* **The Temporary Artifact:** Intermediate writes to \_temporary/job\_12345/ are technical necessities for HDFS/S3 consistency but are logically irrelevant. Their presence in a lineage graph confuses users who search for the "Sales" table and find "Sales\_temp\_123".4  
* **The Physical-Logical Disconnect:** Spark reads files (s3://...), but business users query tables (hive://...). Without bridging this gap, the lineage graph is fragmented; the upstream ETL (producing files) does not connect to the downstream BI dashboard (consuming tables).5

The OpenLineage Spark integration provides the raw capability to extract this metadata by instrumenting the LogicalPlan of the Spark application.6 However, the quality of the resulting lineage depends entirely on the configuration and extension of this integration to filter noise and impose logical structure.

## **2\. Architecture of Lineage Extraction in Apache Spark**

Before detailing the sanitization strategies, it is critical to establish the architectural baseline: how does OpenLineage extract metadata from Spark, and where do the opportunities for intervention exist?

### **2.1 The OpenLineageSparkListener Mechanism**

The integration operates by attaching a listener, io.openlineage.spark.agent.OpenLineageSparkListener, to the Spark Context.6 This listener creates a parallel observation plane alongside the main execution thread. It subscribes to the Spark Listener Bus, receiving asynchronous events regarding the application lifecycle.  
Key events monitored include:

* SparkListenerJobStart: Triggered when an action (like count() or save()) initiates a job. This is the primary hook for input/output extraction.  
* SparkListenerJobEnd: Marks the completion, success, or failure of the unit of work.  
* SparkListenerSQLExecutionStart / End: Provides context for SQL-based operations, essential for grouping multiple jobs under a single query execution.8

Unlike simple logging tools that might parse the text of a SQL query, the OpenLineage listener interrogates the **Catalyst Optimized Logical Plan**. The LogicalPlan is a tree structure representing the data flow operations Spark intends to perform.

### **2.2 Traversing the Logical Plan**

When a job starts, the listener walks the LogicalPlan tree. It looks for specific leaf nodes that represent interactions with the outside world—DataSourceV2Relation, HiveTableRelation, JDBCRelation, or SaveIntoDataSourceCommand.8  
This traversal is where the "raw" lineage is born.

1. **Input Extraction:** The listener identifies nodes reading data. For a file-based source, it extracts the path (e.g., s3://bucket/input).  
2. **Output Extraction:** It identifies the destination. For a write operation, it extracts the target path (e.g., s3://bucket/output).  
3. **Plan Serialization:** The listener serializes the entire logical plan into a text representation and attaches it as a facet (spark.logicalPlan) to the event.4

The "noise" problem arises because the LogicalPlan is accurate to the *physical* execution. If the plan says "Write to s3://bucket/table/partition=x", the listener faithfully reports that specific partition as the dataset. It requires explicit intervention—via configuration or code—to tell the listener: "Ignore the partition suffix; the dataset is actually s3://bucket/table."

### **2.3 The OpenLineageContext**

Throughout this extraction process, the OpenLineageContext object is passed between the extraction methods. This context holds references to the active SparkSession, the SparkContext, and the configuration provided by the user.10 This is the injection point for all customization strategies. Whether we are applying a regex filter or running a custom facet builder, we are essentially modifying how the OpenLineageContext interprets the LogicalPlan.

## **3\. Strategy 1: Dataset Sanitization via Configuration**

The first tier of defense against lineage noise is configuration-based sanitization. This involves instructing the OpenLineage client to ignore specific datasets or to transform their names into canonical forms before the event is emitted to the backend.

### **3.1 The Partition Explosion Problem and removePath.pattern**

The most pervasive issue in file-based data lakes is the inclusion of partition paths in dataset names. Lineage backends (like Marquez or DataHub) typically treat the dataset name as a unique identifier.

* **Scenario:** A job runs daily, appending data to s3://warehouse/transactions/.  
  * Day 1 output: s3://warehouse/transactions/dt=2023-01-01  
  * Day 2 output: s3://warehouse/transactions/dt=2023-01-02  
* **Consequence:** The backend registers two distinct datasets. Lineage queries for "transactions" will fail to aggregate statistics across days. The graph fractures.

#### **3.1.1 Implementing spark.openlineage.dataset.removePath.pattern**

To resolve this, OpenLineage provides the spark.openlineage.dataset.removePath.pattern configuration. This utilizes Java Regular Expressions to rewrite the dataset name on the fly.4  
The mechanism relies on a **named capturing group** with the specific name remove. The logic is precise: the regex must match the *entire* path string, and the portion matched by the (?\<remove\>...) group is excised from the name.  
**Technical Specification:**

* **Property:** spark.openlineage.dataset.removePath.pattern  
* **Engine:** Java java.util.regex.Pattern  
* **Logic:** result \= matcher.replaceAll("") applied only to the captured group.

**Configuration Examples:**

| Scenario | Physical Path (Input) | Regex Configuration | Resulting Dataset Name |
| :---- | :---- | :---- | :---- |
| **Date Partitioning** | s3://bkt/data/dt=2023-10-01 | (.\*)(?\<remove\>/dt=.\*) | s3://bkt/data |
| **Nested Partitions** | .../data/year=2023/month=10 | (.\*)(?\<remove\>/year=.\*) | .../data |
| **ID Partitioning** | .../user/id=500/profile | (.\*)(?\<remove\>/id=\\d+)(.\*) | .../user/profile |
| **Staging Dir** | .../data/\_temporary/0/task | (.\*)(?\<remove\>/\_temporary.\*) | .../data |

**Architectural Insight:** The use of (.\*) at the start of the regex is crucial. It ensures that the base path (the part we want to keep) is consumed by the first group, while the remove group anchors to the specific pattern we wish to drop.3 This transformation happens within the RemovePathPatternUtils class inside the Spark agent, ensuring that the event sent to the transport layer is already sanitized.

#### **3.1.2 Risks of Over-Sanitization**

Careless regex application can destroy lineage utility. If a user configures (.\*)(?\<remove\>/.\*), identifying the root bucket s3://my-bucket as the dataset, then *all* tables within that bucket will collapse into a single node. The graph will show every job reading from and writing to the same "Bucket Node," obscuring the table-level dependencies. The regex must be strictly targeted to the partition key naming convention (e.g., /dt=, /year=).

### **3.2 Exclusion of Temporary and Irrelevant Datasets**

In complex Spark applications, datasets are often created that serve purely transient purposes. These might be checkpoint directories for Spark Streaming, broadcast exchange locations, or temporary outputs from persist() operations.

#### **3.2.1 Namespace Filtering Strategies**

OpenLineage introduces the spark.openlineage.dataset.exclude (and include) configurations to handle this. These properties utilize regex lists to block entire classes of datasets based on their URI.4

* **Mechanism:** Upon extracting a dataset, the listener checks the dataset's namespace (e.g., s3://temp-bucket) against the exclusion list. If a match is found, the dataset is dropped from the inputs or outputs list of the RunEvent.  
* Use Case: The Developer Sandbox:  
  In many organizations, production clusters are also used for ad-hoc analysis writing to user-specific home directories (e.g., hdfs://user/jdoe/test\_output). These ad-hoc runs create noise in the corporate catalog.  
  * **Configuration:** spark.openlineage.dataset.exclude=hdfs://user/.\*,s3://.\*-dev-sandbox/.\*  
  * **Outcome:** The production jobs remain in the graph; the ad-hoc experiments are silently ignored.

#### **3.2.2 Intermediate Shuffle Exclusion**

Spark Shuffles (Exchange operations) involve writing intermediate data to local disk or network storage.1 In early implementations of lineage tools, these were sometimes misidentified as datasets.  
The current OpenLineage architecture largely filters these out by focusing on specific LogicalPlan nodes (DataSourceV2Relation) rather than generic RDD dependencies. However, "checkpointing" in Spark (truncating the lineage of an RDD by saving it to disk) effectively creates a dataset.

* **Implication:** If a job explicitly checkpoints to s3://bucket/checkpoints, this *is* a valid dataset from Spark's perspective.  
* **Strategy:** If these checkpoints are not useful for governance, they should be targeted via the exclude pattern or the removePath pattern to aggregate them into a generic "Checkpoint" node rather than thousands of unique checkpoint paths.

### **3.3 Reducing Metadata Payload with Facet Disabling**

Sanitization also applies to the size of the event. A massive Spark job might have a LogicalPlan that is megabytes in size when serialized. Transmitting this spark.logicalPlan facet can strain the transport layer (HTTP/Kafka) or the backend database.

* **Configuration:** spark.openlineage.facets.\<facet name\>.disabled.4  
* **Example:** spark.openlineage.facets.spark.logicalPlan.disabled=true  
* **Trade-off:** Disabling the logical plan reduces visibility for debugging performance issues (e.g., seeing if a broadcast join was used) but significantly lightens the event payload. For purely governance-focused deployments, disabling the plan facet is a valid optimization.

## **4\. Strategy 2: Normalization via Namespace Resolution**

In the OpenLineage model, a dataset is uniquely identified by the tuple (namespace, name). The namespace typically represents the storage service or cluster (e.g., s3://my-bucket or postgres://db-host:5432). A frequent challenge in cloud environments is the instability of these namespace identifiers.

### **4.1 The Dynamic Infrastructure Problem**

Consider a Spark job reading from a PostgreSQL database.

* **Scenario A:** The job connects to the primary replica: jdbc:postgresql://db-primary:5432/sales.  
  * Generated Namespace: postgres://db-primary:5432  
* **Scenario B:** A failover occurs. The job connects to: jdbc:postgresql://db-secondary:5432/sales.  
  * Generated Namespace: postgres://db-secondary:5432

To a lineage backend, these are two different databases. The graph splits. The sales table in namespace A is distinct from the sales table in namespace B. This fragmentation destroys the continuity of the data history.

### **4.2 Solution: Dataset Namespace Resolvers**

OpenLineage solves this via **Dataset Namespace Resolvers**. This mechanism intercepts the physical namespace string and maps it to a logical, canonical name *before* the event is constructed.13  
These resolvers are configured in spark-defaults.conf or openlineage.yml and support several strategies.

#### **4.2.1 The Host List Resolver (hostList)**

This strategy is designed for systems where a client might connect to any node in a cluster, such as Kafka brokers or Cassandra nodes.

* **Configuration Pattern:**  
  Properties  
  spark.openlineage.dataset.namespaceResolvers.my-kafka.type=hostList  
  spark.openlineage.dataset.namespaceResolvers.my-kafka.hosts=\["broker-1.internal", "broker-2.internal", "broker-3.internal"\]  
  spark.openlineage.dataset.namespaceResolvers.my-kafka.resolvedName=production-kafka-cluster

* **Mechanism:** When Spark interacts with kafka://broker-2.internal:9092, the resolver checks if broker-2.internal is in the hosts list. If yes, it replaces the namespace with production-kafka-cluster.  
* **Result:** Reads/writes to any broker in the cluster map to the same logical dataset nodes.13

#### **4.2.2 The Pattern Resolver (pattern)**

This strategy employs regular expressions to enforce naming conventions. It is ideal for cloud hostnames that follow a predictable structure.

* **Configuration Pattern:**  
  Properties  
  spark.openlineage.dataset.namespaceResolvers.prod-db.type=pattern  
  spark.openlineage.dataset.namespaceResolvers.prod-db.regex=db-prod-\[a-z0-9\]+.company.com  
  spark.openlineage.dataset.namespaceResolvers.prod-db.resolvedName=logical-production-db

* **Mechanism:** Any host matching the regex db-prod-.\* will be resolved to logical-production-db.14

#### **4.2.3 The Custom Resolver Interface (DatasetNamespaceResolver)**

For complex scenarios not covered by static lists or regexes (e.g., resolving a namespace by querying an external Service Discovery API like Consul or Eureka), OpenLineage exposes the io.openlineage.client.dataset.namespace.resolver.DatasetNamespaceResolver interface.15  
**Implementation Architecture:**

1. **Interface:** Developers implement a Java class extending DatasetNamespaceResolver.  
2. **Method:** The core method resolve(String namespace) contains the logic to return the canonical name.  
3. **Deployment:** The implementation must be packaged as a JAR and registered using the Java ServiceLoader mechanism (via a META-INF/services file).  
4. **Configuration:**  
   Properties  
   spark.openlineage.dataset.namespaceResolvers.my-custom.type=com.company.lineage.MyConsulResolver

This extensibility ensures that the lineage graph can adapt to any infrastructure orchestration pattern, maintaining the logical integrity of the data map.

## **5\. Strategy 3: Logical Abstraction via Symlinks**

The most advanced form of lineage sanitization addresses the duality of data identity. In a Data Lake, data often exists simultaneously as a physical object (File) and a logical entity (Table).

### **5.1 The Physical-Logical Duality**

* **Physical Identity:** s3://warehouse/finance/gl/ (What Spark reads/writes).  
* **Logical Identity:** hive://warehouse/finance.gl (What the user queries in SQL/BI).

If Spark reports the S3 path, and a downstream tool (like Presto/Trino or a BI tool) reports the Hive table name, the lineage graph is broken. There is no edge connecting the S3 path to the Hive table. To the graph, they are strangers.

### **5.2 The SymlinksDatasetFacet**

The solution is the SymlinksDatasetFacet.5 This facet allows a dataset to declare "aliases." It tells the lineage backend: "I am reporting s3://path, but I am also known as hive://table."  
**JSON Structure:**

JSON

"symlinks": {  
  "identifiers":  
}

When a backend like Marquez receives this, it can merge the nodes or create an equivalency edge, unifying the lineage.

### **5.3 Injecting Symlinks via CustomFacetBuilder**

While some integration points might allow passing facets via Spark properties 6, the robust, architectural way to inject Symlinks is by extending the OpenLineage Spark Agent using a CustomFacetBuilder.

#### **5.3.1 The CustomFacetBuilder Interface**

The CustomFacetBuilder is a typed interface that allows developers to define logic for specific Spark LogicalPlan nodes.

* **Signature:** CustomFacetBuilder\<T, F extends DatasetFacet\>  
* **Generics:** T is the Spark node type (e.g., HiveTableRelation), F is the facet type (SymlinksDatasetFacet).  
* **Key Methods:**  
  * isDefinedAt(Object x): A filter method to verify if the builder applies to the current node.  
  * build(T x, BiConsumer\<String, F\> consumer): The logic to construct the facet.8

#### **5.3.2 Implementation Logic: The Hive Table Symlink Builder**

To implement automatic Symlinks for all Hive tables in Spark:

1. **Project Setup:** Create a Java/Scala project with dependencies on openlineage-spark and spark-catalyst.  
2. **Builder Implementation:**  
   Java  
   public class HiveSymlinkBuilder extends CustomFacetBuilder\<HiveTableRelation, SymlinksDatasetFacet\> {  
       public HiveSymlinkBuilder() {  
           // Default constructor required for ServiceLoader  
       }

       @Override  
       public boolean isDefinedAt(Object x) {  
           return x instanceof HiveTableRelation;  
       }

       @Override  
       public void build(HiveTableRelation x, BiConsumer\<String, SymlinksDatasetFacet\> consumer) {  
           // 1\. Extract metadata from the Spark Logical Plan node  
           String db \= x.tableMeta().database();  
           String table \= x.tableMeta().table();

           // 2\. Construct the Logical Identifier  
           SymlinksDatasetFacetIdentifiers id \= new SymlinksDatasetFacetIdentifiers(  
               "hive://warehouse",   
               db \+ "." \+ table,   
               "TABLE"  
           );

           // 3\. Create the Facet  
           SymlinksDatasetFacet facet \= new SymlinksDatasetFacet(  
               Collections.singletonList(id)  
           );

           // 4\. Emit the facet  
           consumer.accept("symlinks", facet);  
       }  
   }

   This code programmatically bridges the gap. Whenever Spark interacts with a HiveTableRelation, the standard visitor extracts the physical path, and *this* builder acts as a sidecar, injecting the logical alias.10  
3. Registration via OpenLineageEventHandlerFactory:  
   The OpenLineage Spark agent uses the Java Service Provider Interface (SPI) to load extensions.  
   * Create a factory class implementing OpenLineageEventHandlerFactory.  
   * Override createDatasetFacetBuilders(OpenLineageContext context) to return an instance of HiveSymlinkBuilder.  
   * Create the file META-INF/services/io.openlineage.spark.api.OpenLineageEventHandlerFactory pointing to your factory class.8  
4. **Deployment:** Package the code as a JAR and add it to the spark.jars configuration of the cluster.

### **5.4 Injecting Custom Facets via Properties**

For simpler use cases, or where compiling custom JARs is restricted, OpenLineage supports injecting facets via Spark configuration properties. This is particularly useful for DataSourceV2 connectors that expose their options to the plan.

* **Pattern:** Properties starting with openlineage.dataset.facets. are parsed and injected.  
* **Example:**  
  Scala  
  df.write  
   .option("openlineage.dataset.facets.customMeta", "{\\"owner\\":\\"data-team\\", \\"criticality\\":\\"high\\"}")  
   .save(...)

  This allows job-level metadata injection without changing the agent code.8

## **6\. Advanced Metrics and Iceberg Integration**

Beyond identity and sanitization, high-fidelity lineage requires operational context. The OpenLineage integration automatically captures standard input/output statistics.

### **6.1 Standard Dataset Statistics**

For supported data sources, the integration attaches InputStatisticsInputDatasetFacet and OutputStatisticsOutputDatasetFacet.

* **Metrics:**  
  * rowCount: Number of rows read/written.  
  * byteSize: Volume of data in bytes.  
  * fileCount: Number of files involved.  
* **Mechanism:** These are collected from Spark's internal task metrics accumulators (TaskMetrics). By aggregating these at the job level, the lineage graph can serve as a dashboard for data volume trends.17

### **6.2 Iceberg-Specific Metrics**

For Apache Iceberg tables, the integration goes deeper, leveraging Iceberg's MetricReport API.

* **ScanReport:** Metrics collected during the scan planning phase (e.g., manifests scanned, files skipped via partition pruning).  
* **CommitReport:** Metrics regarding the write commit (e.g., added data files, added delete files).  
* **Value:** This allows users to not only see *that* a table was read but *how efficiently* it was read. A spike in "scanned files" without a spike in "rows read" might indicate poor partition pruning, a performance insight derived directly from the lineage graph.18

## **7\. Operationalization and Best Practices**

Implementing these strategies transforms the OpenLineage integration from a passive logger to an active governance engine. However, success requires a structured operational approach.

### **7.1 Testing with the Console Transport**

Before deploying regex patterns or custom JARs to production, the console transport is invaluable.

* **Configuration:** spark.openlineage.transport.type=console.4  
* **Procedure:** Run a sample Spark job. The driver logs will print the full JSON OpenLineage events.  
* **Validation:**  
  * Check the name field of datasets to verify removePath.pattern is working correctly (no partition strings).  
  * Check the namespace field to verify Resolvers are mapping hosts correctly.  
  * Check the facets map to verify symlinks or other custom facets are present.

### **7.2 Managing Version Compatibility**

The Spark ecosystem is fragmented (Spark 2.4, 3.1, 3.2, 3.3+). The OpenLineage integration relies on internal Spark classes (LogicalPlan, HiveTableRelation) which can change between versions.

* **Strategy:** When developing CustomFacetBuilder extensions, maintain separate build branches for major Spark versions if the underlying Catalyst classes differ. Use the openlineage-spark variant that matches your cluster version (e.g., openlineage-spark-3.3).19

### **7.3 Impact on the Lineage Backend (Marquez)**

The sanitization work performed in Spark directly dictates the quality of the user experience in a backend like Marquez.

* **Dataset Grouping:** By sanitizing the name and normalizing the namespace, Marquez can group thousands of job runs under a single "Dataset" node. This enables the "Versions" tab in the UI, showing the schema and facet evolution of that single dataset over time.2  
* **Graph Connectivity:** The Symlinks facet enables Marquez to merge nodes. If the backend supports it, searching for "Finance Table" will reveal the lineage even if the underlying jobs reported "S3 Path".20

## **8\. Conclusion**

The implementation of OpenLineage in Apache Spark is not a "set it and forget it" deployment. The default behavior, while technically accurate, produces a high-noise signal reflecting the physical chaos of distributed processing. To elevate this signal to the level of Enterprise Data Governance, Data Architects must intervene.  
By rigorously applying **Dataset Filtering** (via regex) to remove temporary artifacts, implementing **Namespace Resolvers** to canonicalize infrastructure identities, and deploying **Custom Facet Builders** to inject Logical Symlinks, an organization can reconstruct the logical intent of their data ecosystem. This sanitized graph becomes a powerful asset, capable of driving regulatory compliance, cost optimization, and cross-system impact analysis, turning the opaque "black box" of Spark execution into a transparent, navigable map of data provenance.

#### **Works cited**

1. Optimize shuffles \- \- AWS Documentation, accessed November 20, 2025, [https://docs.aws.amazon.com/prescriptive-guidance/latest/tuning-aws-glue-for-apache-spark/optimize-shuffles.html](https://docs.aws.amazon.com/prescriptive-guidance/latest/tuning-aws-glue-for-apache-spark/optimize-shuffles.html)  
2. Tracing Data Lineage with OpenLineage and Apache Spark, accessed November 20, 2025, [https://openlineage.io/blog/openlineage-spark/](https://openlineage.io/blog/openlineage-spark/)  
3. spark lineage plugin \- remove\_partition\_pattern config parameter has no effect \#11942, accessed November 20, 2025, [https://github.com/datahub-project/datahub/issues/11942](https://github.com/datahub-project/datahub/issues/11942)  
4. Spark Config Parameters \- OpenLineage, accessed November 20, 2025, [https://openlineage.io/docs/integrations/spark/configuration/spark\_conf/](https://openlineage.io/docs/integrations/spark/configuration/spark_conf/)  
5. Symlinks Facet \- OpenLineage, accessed November 20, 2025, [https://openlineage.io/docs/spec/facets/dataset-facets/symlinks/](https://openlineage.io/docs/spec/facets/dataset-facets/symlinks/)  
6. Using OpenLineage with Spark, accessed November 20, 2025, [https://openlineage.io/docs/guides/spark/](https://openlineage.io/docs/guides/spark/)  
7. OpenLineage for Spark Connectors, accessed November 20, 2025, [https://openlineage.io/docs/guides/spark-connector/](https://openlineage.io/docs/guides/spark-connector/)  
8. Extending | OpenLineage, accessed November 20, 2025, [https://openlineage.io/docs/integrations/spark/extending/](https://openlineage.io/docs/integrations/spark/extending/)  
9. Integrating with Spark extensions \- OpenLineage, accessed November 20, 2025, [https://openlineage.io/docs/1.34.0/development/developing/spark/built\_in\_lineage/](https://openlineage.io/docs/1.34.0/development/developing/spark/built_in_lineage/)  
10. Integrating with Spark extensions \- OpenLineage, accessed November 20, 2025, [https://openlineage.io/docs/1.23.0/development/developing/spark/built\_in\_lineage/](https://openlineage.io/docs/1.23.0/development/developing/spark/built_in_lineage/)  
11. Spark Config Parameters \- OpenLineage, accessed November 20, 2025, [https://openlineage.io/docs/1.22.0/integrations/spark/configuration/spark\_conf](https://openlineage.io/docs/1.22.0/integrations/spark/configuration/spark_conf)  
12. OpenLineage Resource Configuration \- IBM, accessed November 20, 2025, [https://www.ibm.com/docs/en/manta-data-lineage?topic=openlineage-resource-configuration](https://www.ibm.com/docs/en/manta-data-lineage?topic=openlineage-resource-configuration)  
13. Configuration \- OpenLineage, accessed November 20, 2025, [https://openlineage.io/docs/1.28.0/client/java/configuration/](https://openlineage.io/docs/1.28.0/client/java/configuration/)  
14. Configuration | OpenLineage, accessed November 20, 2025, [https://openlineage.io/docs/client/java/configuration/](https://openlineage.io/docs/client/java/configuration/)  
15. DatasetNamespaceResolver (openlineage-java 1.40.0-SNAPSHOT API), accessed November 20, 2025, [https://openlineage.io/apidocs/javadoc/io/openlineage/client/dataset/namespace/resolver/DatasetNamespaceResolver.html](https://openlineage.io/apidocs/javadoc/io/openlineage/client/dataset/namespace/resolver/DatasetNamespaceResolver.html)  
16. Python Client \- OpenLineage, accessed November 20, 2025, [https://openlineage.io/docs/next/development/developing/python/api-reference/openlineage.client/](https://openlineage.io/docs/next/development/developing/python/api-reference/openlineage.client/)  
17. Capturing dataset statistics in Apache Spark \- OpenLineage, accessed November 20, 2025, [https://openlineage.io/blog/spark-dataset-statistics/](https://openlineage.io/blog/spark-dataset-statistics/)  
18. Dataset Metrics \- OpenLineage, accessed November 20, 2025, [https://openlineage.io/docs/integrations/spark/dataset\_metrics/](https://openlineage.io/docs/integrations/spark/dataset_metrics/)  
19. 1.17.1 | OpenLineage, accessed November 20, 2025, [https://openlineage.io/docs/releases/1\_17\_1/](https://openlineage.io/docs/releases/1_17_1/)  
20. Marquez Project | Marquez Project, accessed November 20, 2025, [https://marquezproject.ai/](https://marquezproject.ai/)