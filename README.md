# RDF2X

Convert big Linked Data RDF datasets to a relational database model, CSV, JSON and ElasticSearch using [Spark](http://spark.apache.org). 

![image](https://user-images.githubusercontent.com/2894124/66893915-6d362d00-efef-11e9-8515-ccf04274d61f.png)

# Tutorials

[Visualizing ClinicalTrials.gov RDF data in Tableau using RDF2X](https://medium.com/@david.prihoda/visualizing-clinicaltrials-gov-rdf-data-in-tableau-using-rdf2x-bd5bb5c97f0a)

[Querying Wikidata RDF with SQL using RDF2X](https://medium.com/@david.prihoda/querying-wikidata-rdf-with-sql-using-rdf2x-324f18219adf)

[Distributed Conversion of RDF Data to the Relational Model (thesis)](http://davidprihoda.com/rdf2x/Distributed_Conversion_of_RDF_Data_to_the_Relational_Model.pdf)

# Get started

RDF2X can be executed [from source using Maven](#running-from-source) or 
[using a JAR file](#running-jar-using-spark-submit).

## Running from source

To launch from source using Maven:

- Install [JDK 1.8](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html)
- Install [Maven](https://maven.apache.org/download.cgi)
- Run the following commands: 

```bash
# Save to CSV
mvn exec:java -Dexec.args="convert \
--input.file /path/to/input/ \
--output.target CSV \
--output.folder /path/to/output/folder"

# Save to JSON
mvn exec:java -Dexec.args="convert \
--input.file /path/to/input/ \
--output.target JSON \
--output.folder /path/to/output/folder"

# Save to a database
mvn exec:java -Dexec.args="convert \
--input.file /path/to/input/ \
--output.target DB \
--db.url 'jdbc:postgresql://localhost:5432/database_name' \
--db.user user \
--db.password 123456 \
--db.schema public"

# More config options
mvn \
-Dspark.app.name="RDF2X My file" \
-Dspark.master=local[2] \
-Dspark.driver.memory=3g \
exec:java  \
-Dexec.args="convert \
--input.file /path/to/input/ \
--input.lineBasedFormat true \
--input.batchSize 500000 \
--output.saveMode Overwrite \
--output.target DB \
--db.url \"jdbc:postgresql://localhost:5432/database_name\" \
--db.user user \
--db.password 123456 \
--db.schema public \
--db.batchSize 1000"
```

Refer to the [Configuration](#configuration) section below for all config parameters.




## Running JAR using spark-submit

To launch locally via spark-submit:

- Download the packaged JAR from our [releases page](https://github.com/Merck/rdf2x/releases)
- Install [JDK 1.8](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html)
- Download [Spark 1.6](http://spark.apache.org/downloads.html)
- Add the Spark bin directory to your system PATH variable
- Refer to the [Configuration](#configuration) section below for all config parameters.
- Run this command from the project target directory (or anywhere you have put your packaged JAR)

```bash
spark-submit \
--name "RDF2X ClinicalTrials.gov" \
--class com.merck.rdf2x.main.Main \
--master 'local[2]' \
--driver-memory 2g \
--packages postgresql:postgresql:9.1-901-1.jdbc4,org.eclipse.rdf4j:rdf4j-runtime:2.1.4,org.apache.jena:jena-core:3.1.1,org.apache.jena:jena-elephas-io:3.1.1,org.apache.jena:jena-elephas-mapreduce:0.9.0,com.beust:jcommander:1.58,com.databricks:spark-csv_2.10:1.5.0,org.elasticsearch:elasticsearch-spark_2.10:2.4.4,org.jgrapht:jgrapht-core:1.0.1 \
rdf2x-1.0-SNAPSHOT.jar \
convert \
--input.file /path/to/clinicaltrials \
--input.lineBasedFormat true \
--cacheLevel DISK_ONLY \
--input.batchSize 1000000 \
--output.target DB \
--db.url "jdbc:postgresql://localhost:5432/database_name?tcpKeepAlive=true" \
--db.user user \
--db.password 123456 \
--db.schema public \
--db.batchSize 1000 \
--output.saveMode Overwrite
```
To run stats job via spark-submit:

```bash
spark-submit --name "RDF2X ClinicalTrials.gov" --class com.merck.rdf2x.main.Main --master 'local' \
--driver-memory 2g \
--packages postgresql:postgresql:9.1-901-1.jdbc4,org.eclipse.rdf4j:rdf4j-runtime:2.1.4,org.apache.jena:jena-core:3.1.1,org.apache.jena:jena-elephas-io:3.1.1,org.apache.jena:jena-elephas-mapreduce:0.9.0,com.beust:jcommander:1.58,com.databricks:spark-csv_2.10:1.5.0,org.elasticsearch:elasticsearch-spark_2.10:2.4.4,org.jgrapht:jgrapht-core:1.0.1 \
rdf2x-0.1.jar \
stats \
--input.file  bio2rdf-clinicaltrials.nq \
--input.batchSize 1000000 \
--stat SUBJECT_URI_COUNT
```

## Running on YARN

To launch on a cluster:

- Copy the JAR you packaged earlier to your server
- Optionally, configure driver log level by referencing custom log4j.properties. You can copy and modify the existing ones in src/main/resources/ folder.

### Run on YARN: Save to DB

```bash
spark-submit \
--name "RDF2X ClinicalTrials.gov" \
--class com.merck.rdf2x.main.Main \
--master yarn \
--deploy-mode client \
--driver-memory 4g \
--queue default \
--executor-memory 6g \
--executor-cores 1 \
--num-executors 5 \
--conf spark.yarn.executor.memoryOverhead=2048 \
--conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:///path/to/your/log4j.properties" \
--packages postgresql:postgresql:9.1-901-1.jdbc4,org.eclipse.rdf4j:rdf4j-runtime:2.1.4,org.apache.jena:jena-core:3.1.1,org.apache.jena:jena-elephas-io:3.1.1,org.apache.jena:jena-elephas-mapreduce:0.9.0,com.beust:jcommander:1.58,com.databricks:spark-csv_2.10:1.5.0,org.elasticsearch:elasticsearch-spark_2.10:2.4.4,org.jgrapht:jgrapht-core:1.0.1 \
rdf2x-1.0-SNAPSHOT.jar \
convert \
--input.file hdfs:///path/to/clinicaltrials/ \
--input.lineBasedFormat true \
--input.batchSize 1000000 \
--output.saveMode Overwrite \
--output.target DB \
--db.url "jdbc:postgresql://your.db.server.com/database_name" \
--db.user user \
--db.password 123456 \
--db.schema public \
--db.batchSize 1000
```

### Run on YARN: Save to CSV

```bash
...
--output.target CSV \
--output.folder hdfs:///path/to/clinicaltrials-csv/ 
```

### Run on YARN: Save to JSON

```bash
...
--output.target JSON \
--output.folder hdfs:///path/to/clinicaltrials-csv/ 
```

### Run on YARN: Save to ElasticSearch

Note: 
- Currently the data is saved to ElasticSearch in a relational format - entity and relation tables. 
- --output.saveMode is ignored when saving to ElasticSearch (data is always appended).
- Connection parameters and other ES config can be specified as System properties via Spark conf: `--conf spark.es.nodes=localhost --conf spark.es.port=9200`

```bash
spark-submit \
--name "RDF2X ClinicalTrials.gov" \
--class com.merck.rdf2x.main.Main \
--master yarn \
--deploy-mode client \
--driver-memory 4g \
--queue default \
--executor-memory 6g \
--executor-cores 1 \
--num-executors 5 \
--conf spark.es.nodes=localhost \
--conf spark.es.port=9200 \
--conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:///path/to/your/log4j.properties" \
--packages postgresql:postgresql:9.1-901-1.jdbc4,org.eclipse.rdf4j:rdf4j-runtime:2.1.4,org.apache.jena:jena-core:3.1.1,org.apache.jena:jena-elephas-io:3.1.1,org.apache.jena:jena-elephas-mapreduce:0.9.0,com.beust:jcommander:1.58,com.databricks:spark-csv_2.10:1.5.0,org.elasticsearch:elasticsearch-spark_2.10:2.4.4,org.jgrapht:jgrapht-core:1.0.1 \
rdf2x-1.0-SNAPSHOT.jar \
convert \
--input.file hdfs:///path/to/clinicaltrials/ \
--input.lineBasedFormat true \
--input.batchSize 1000000 \
--output.target ES \
--es.index clinicaltrials
```

Refer to the [Configuration](#configuration) section below for all config parameters.

## Data sources

Download your RDF dataset, e.g. ClinicalTrials.gov:

```bash
wget http://download.bio2rdf.org/release/4/clinicaltrials/clinicaltrials.nq.gz
```

If you plan on using a cluster, add the data to HDFS:

```bash
# Single file
hadoop fs -put clinicaltrials.nq.gz /path/to/datasets/

# Multiple files
hadoop fs -mkdir /path/to/datasets/clinicaltrials
hadoop fs -put * /path/to/datasets/clinicaltrials
```

## Building RDF2X JAR

Use Maven to get a packaged JAR file: 

```bash
# compile, run tests and create JAR
mvn package

# or without running tests
mvn package -Dmaven.test.skip=true
```


# Example

Consider this simple example from the [W3C Turtle specification](https://www.w3.org/TR/turtle/):

```
@base <http://example.org/> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix foaf: <http://xmlns.com/foaf/0.1/> .
@prefix rel: <http://www.perceive.net/schemas/relationship/> .

<#green-goblin>
        rel:enemyOf <#spiderman> ;
        a foaf:Person ;    # in the context of the Marvel universe
        foaf:name "Green Goblin" .

<#spiderman>
        rel:enemyOf <#green-goblin> ;
        a foaf:Person ;
        foaf:name "Spiderman", "Человек-паук"@ru .
```

## Database output

Converting to SQL format will result in the following tables:

### Person

| ID|          URI|name_ru_string| name_string|
|---|-------------|--------------|------------|
|  1|#green-goblin|          null|Green Goblin|
|  2|   #spiderman|  Человек-паук|   Spiderman|

### Person_Person

|person_ID_from|person_ID_to|predicate|
|--------------|------------|---------|
|             2|           1|        3|
|             1|           2|        3|

Along with the entities and relationships, metadata is persisted:

### _META_Entities:

|                             URI|  name|  label| num_rows|
|--------------------------------|------|-------|---------|
|http://xmlns.com/foaf/0.1/Person|Person|   null|        2|

### _META_Columns

|          name|predicate|  type|multivalued|language|non_null|entity_name|
|--------------|---------|------|-----------|--------|--------|-----------|
|name_ru_string|        2|STRING|      false|      ru|     0.5|     Person|
|   name_string|        2|STRING|      false|    null|       1|     Person|

### _META_Relations

|         name|from_name|to_name|
|-------------|---------|-------|
|person_person|   person| person|

### _META_Predicates

|predicate|                 URI|     name|label|
|---------|--------------------|---------|-----|
|        1|http://www.w3.org/1999/02/22-rdf-syntax-ns#type      |     type| null|
|        2|http://xmlns.com/foaf/0.1/name                       |     name| null|
|        3|http://www.perceive.net/schemas/relationship/enemyOf |  enemyof| null|



# Tested datasets

## ClinicalTrials.gov 

Property | Value
---|---
Number of quads | 159,001,344
Size gzipped | 1.8 GB gzipped
Size uncompressed | 45.1 GB uncompressed
Output entity table rows | 17,855,687 (9,859,796 rows in largest table)
Output relation table rows | 74,960,010 (19,084,633 rows in largest table)

- Cluster setup: 5 executors, 6GB RAM each.
    - Run time: 2.7 hours

# Configuration

## Convert

|Name|Default|Description|
|---|---|---|
|--input.file|**required**|Path to input file or folder|
|--flavor|null |Specify a flavor to be used. Flavors modify the behavior of RDF2X, applying default settings and providing custom methods for data source specific modifications.|
|--filter.cacheLevel|StorageLevel(false, false, false, false, 1) |Level of caching of the input dataset after filtering (None, DISK_ONLY, MEMORY_ONLY, MEMORY_AND_DISK, ...). See Spark StorageLevel class for more info.|
|--cacheLevel|StorageLevel(true, false, false, false, 1) |Level of caching of the instances before collecting schema and persisting (None, DISK_ONLY, MEMORY_ONLY, MEMORY_AND_DISK, ...). See Spark StorageLevel class for more info.|
|--instancePartitions|null |Repartition before aggregating instances into this number of partitions.|
|--help|false |Show usage page|

Currently supported flavors:

- Wikidata: Applies settings and methods for converting the Wikidata RDF dumps.
- Bio2RDF: Generates nicer names for Resource entities (with vocabulary prefix).

### Parsing

|Name|Default|Description|
|---|---|---|
|--input.lineBasedFormat|null |Whether the input files can be read line by line (e.g. true for NTriples or NQuads, false for Turtle). In default, will try to guess based on file extension. Line based formats can be parsed by multiple nodes at the same time, other formats will be read by master node and repartitioned after parsing.|
|--input.repartition|null |Repartition after parsing into this number of partitions.|
|--input.batchSize|500000 |Batch size for parsing line-based formats (number of quads per partition)|
|--input.errorHandling|Ignore |How to handle RDF parsing errors (Ignore, Throw).|
|--input.acceptedLanguage|[] |Accepted language. Literals in other languages are ignored. You can specify more languages by repeating this parameter.|

### Filtering

|Name|Default|Description|
|---|---|---|
|--filter.resource|[] |Accept resources of specified URI. More resource URIs can be specified by repeating this parameter.|
|--filter.resourceBlacklist|[] |Ignore resources of specified URI. More resource URIs can be specified by repeating this parameter.|
|--filter.relatedDepth|0 |Accept also resources related to the original set in relatedDepth directed steps. Uses an in-memory set of subject URIs, therefore can only be used for small results (e.g. less than 1 million resources selected).|
|--filter.directed|true |Whether to traverse only in the subject->object directions of relations when retrieving related resources.|
|--filter.type|[] |Accept only resources of specified type. More type URIs can be specified by repeating this parameter.|
|--filter.ignoreOtherTypes|true |Whether to ignore instance types that were not selected. If true, only the tables for the specified types are created. If false, all of the additional types and supertypes of selected instances are considered as well.|

### Output

|Name|Default|Description|
|---|---|---|
|--output.target|**required**|Where to output the result (DB, CSV, JSON, ES, Preview).|
|--output.saveMode|ErrorIfExists |How to handle existing tables (Append, Overwrite, ErrorIfExists, Ignore).|

Based on --output.saveMode, you have to specify additional parameters:

#### Output to DB

|Name|Default|Description|
|---|---|---|
|--db.url|**required** |Database JDBC string|
|--db.user|**required** |Database user|
|--db.password|null |Database password|
|--db.schema|null |Database schema name|
|--db.batchSize|5000 |Insert batch size|
|--db.bulkLoad|true |Use CSV bulk load if possible (PostgreSQL COPY)|

#### Output to JSON, CSV

|Name|Default|Description|
|---|---|---|
|--output.folder|*required* |Folder to output the files to|

#### Output to ElasticSearch (ES)

|Name|Default|Description|
|---|---|---|
|--es.index|null |ElasticSearch Index to save the output to|
|--es.createIndex|true |Whether to create index in case it does not exist, overrides es.index.auto.create property|

Connection parameters can be specified as system properties:

- Via Spark conf: `--conf spark.es.nodes=localhost --conf spark.es.port=9200`
- In standalone mode: `-Dspark.es.nodes=localhost -Dspark.es.port=9200`

### RDF Schema

|Name|Default|Description|
|---|---|---|
|--rdf.typePredicate|[] |Additional URI apart from rdf:type to treat as type predicate. You can specify more predicates by repeating this parameter.|
|--rdf.subclassPredicate|[] |Additional URI apart from rdfs:subClassOf to treat as subClassOf predicate. You can specify more predicates by repeating this parameter.|
|--rdf.collectSubclassGraph|true |Whether to collect the graph of subClass predicates.|
|--rdf.collectLabels|true |Whether to collect type and predicate labels (to be saved in meta tables and for name formatting if requested).|
|--rdf.cacheFile|null |File for saving and loading cached schema.|

### Creating instances

|Name|Default|Description|
|---|---|---|
|--instances.defaultLanguage|null |Consider all values in this language as if no language is specified. Language suffix will not be added to columns.|
|--instances.addSuperTypes|true |Automatically add all supertypes to each instance, instance will be persisted in all parent type tables.|
|--instances.repartitionByType|false |Whether to repartition instances by type. Profitable in local mode when, causes an expensive shuffle in cluster mode.|

### Writing entities and relations

|Name|Default|Description|
|---|---|---|
|--formatting.entityTablePrefix| |String to prepend to entity table names|
|--formatting.relationTablePrefix| |String to prepend to relation table names|
|--relations.storePredicate|true |Store predicate (relationship type) as a third column of entity relation tables.|

### Entities

|Name|Default|Description|
|---|---|---|
|--entities.maxNumColumns|null |Maximum number of columns for one table.|
|--entities.minColumnNonNullFraction|0.0 |Properties require at least minColumnNonNullFraction non-null values to be stored as columns. The rest is stored in the Entity-Attribute-Value table (e.g. 0.4 = properties with less than 40% values present will be stored only in the EAV table, 0 = store all as columns, 1 = store all only in EAV table).|
|--entities.redundantEAV|false |Store all properties in the EAV table, including values that are already stored in columns.|
|--entities.redundantSubclassColumns|false |Store all columns in subclass tables, even if they are also present in a superclass table. If false (default behavior), columns present in superclasses are removed, their superclass location is marked in the column meta table.|
|--entities.minNumRows|1 |Minimum number of rows required for an entity table. Tables with less rows will not be included.|
|--entities.sortColumnsAlphabetically|false |Sort columns alphabetically. Otherwise by non-null ratio, most frequent first.|
|--entities.forceTypeSuffix|false |Whether to always add a type suffix to columns, even if only one datatype is present.|

### Relations

|Name|Default|Description|
|---|---|---|
|--relations.schema|Types |How to create relation tables (SingleTable, Types, Predicates, TypePredicates, None)|
|--relations.rootTypesOnly|true |When creating relation tables between two instances of multiple types, create the relation table only for the root type pair. If false, relation tables are created for all combinations of types.|

Supported relation table strategies:

- **SingleTable** Store all relations in a single table
- **Types** Create relation tables between related pairs of entity tables (for example Person_Address)
- **Predicates** Create one relation table for each predicate (for example livesAt)
- **TypePredicates** Create one relation table for each predicate between two entity tables (for example Person_livesAt_Address)
- **None** Do not extract relations

### Formatting

|Name|Default|Description|
|---|---|---|
|--formatting.maxTableNameLength|25 |Maximum length of entity table names|
|--formatting.maxColumnNameLength|50 |Maximum length of column names|
|--formatting.uriSuffixPattern|[/:#=] |When collecting name from URI, use the segment after the last occurrence of this regex|
|--formatting.useLabels|false |Try to use rdfs:label for formatting names. Will use URIs if label is not present.|

# RDF Concepts implementation

## Instances with multiple types

An instance with multiple types is saved in each specified type's entity table. For example, this input:

```
@base <http://example.org/> .
@prefix foaf: <http://xmlns.com/foaf/0.1/> .

<#spiderman>
        a foaf:Person, foaf:Agent ;
        foaf:name "Spiderman".

<#lexcorp>
        a foaf:Organization, foaf:Agent ;
        foaf:name "LexCorp" ;
        foaf:homepage "https://www.lexcorp.io/".
```

will result in three entity tables:

### Agent

| ID|       URI|        homepage_string|name_string|
|---|----------|-----------------------|-----------|
|  1|  #lexcorp|https://www.lexcorp.io/|    LexCorp|
|  2|#spiderman|                   null|  Spiderman|

### Organization

| ID|     URI|        homepage_string|
|---|--------|-----------------------|
|  1|#lexcorp|https://www.lexcorp.io/|

### Person

| ID|       URI|
|---|----------|
|  2|#spiderman|

Whether the inherited *name* column is duplicated in subclass tables is configurable.

## Literal data types

Supported datatypes depend on [Jena](https://jena.apache.org/documentation/notes/typed-literals.html).

The following types will be stored: STRING, INTEGER, DOUBLE, FLOAT, LONG, BOOLEAN. 
Other types will be converted to STRING.

DATETIME will be stored as STRING, column type has to be converted in post-processing, e.g. with Postgres:

```sql
ALTER TABLE public.en_thing
ALTER COLUMN start_date_datetime TYPE timestamp
USING start_date_datetime::timestamp without time zone;
```

## Multi-valued properties

Multi-valued properties occur when multiple different values are specified for a single predicate, data type and language:

```
@base <http://example.org/> .
@prefix foaf: <http://xmlns.com/foaf/0.1/> .

<#spiderman>
        a foaf:Person;
        foaf:name "Spiderman", "Spider-Man", "Spider man", "Человек-паук"@ru.
```

In that case, only one of the values is saved in the column (not deterministic).

Additionally, the column is added to the EAV set, which means that all the column values (even of the other instances that have only a single value) are saved in the Entity-Attribute-Value table.

### Person

| ID|          URI| name_string|name_ru_string|
|---|-------------|------------|--------------|
|  1|#green-goblin|Green Goblin|          null|
|  2|   #spiderman|  Spider-Man|  Человек-паук|

### EAV table

| ID|PREDICATE|datatype|language|       value|
|---|---------|--------|--------|------------|
|  1|        3|  STRING|    null|Green Goblin|
|  2|        3|  STRING|    null|  Spider-Man|
|  2|        3|  STRING|    null|  Spider man|
|  2|        3|  STRING|    null|   Spiderman|

## Blank nodes

Not implemented yet. Triples with blank nodes are ignored.
