/*
 * Copyright 2017 Merck Sharp & Dohme Corp. a subsidiary of Merck & Co.,
 * Inc., Kenilworth, NJ, USA.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.merck.rdf2x.jobs.convert;

import com.merck.rdf2x.beans.IndexMap;
import com.merck.rdf2x.beans.Instance;
import com.merck.rdf2x.flavors.DefaultFlavor;
import com.merck.rdf2x.flavors.Flavor;
import com.merck.rdf2x.persistence.InstanceRelationWriter;
import com.merck.rdf2x.persistence.MetadataWriter;
import com.merck.rdf2x.persistence.output.Persistor;
import com.merck.rdf2x.persistence.output.PersistorFactory;
import com.merck.rdf2x.persistence.schema.EntitySchema;
import com.merck.rdf2x.persistence.schema.RelationSchema;
import com.merck.rdf2x.processing.aggregating.InstanceAggregator;
import com.merck.rdf2x.processing.filtering.InstanceFilter;
import com.merck.rdf2x.processing.filtering.QuadFilter;
import com.merck.rdf2x.processing.formatting.SchemaFormatter;
import com.merck.rdf2x.processing.indexing.GlobalInstanceIndexer;
import com.merck.rdf2x.processing.indexing.InstanceIndexer;
import com.merck.rdf2x.processing.partitioning.InstancePartitioner;
import com.merck.rdf2x.processing.relations.RelationExtractor;
import com.merck.rdf2x.processing.schema.EntitySchemaCollector;
import com.merck.rdf2x.processing.schema.RelationSchemaCollector;
import com.merck.rdf2x.processing.schema.RelationSchemaStrategy;
import com.merck.rdf2x.rdf.parsing.ElephasQuadParser;
import com.merck.rdf2x.rdf.parsing.QuadParser;
import com.merck.rdf2x.rdf.schema.RdfSchema;
import com.merck.rdf2x.rdf.schema.RdfSchemaCollector;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.jena.sparql.core.Quad;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.storage.StorageLevel;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;


/**
 * ConvertJob converts RDF data to the Relational model and persists the result to a database.
 * <p>
 * The instructions are passed in a {@link ConvertConfig} object.
 */
@Slf4j
public class ConvertJob implements Runnable {
    /**
     * job config with all instructions
     */
    private final ConvertConfig config;
    /**
     * Spark context to be used
     */
    private final JavaSparkContext sc;

    /**
     * Flavor containing custom conversion methods
     */
    private final Flavor flavor;

    /**
     * Create a convert job with a custom flavor
     *
     * @param config job config with all instructions
     * @param sc     Spark context to be used
     * @param flavor Flavor containing custom conversion methods
     */
    public ConvertJob(@NonNull ConvertConfig config, @NonNull JavaSparkContext sc, @NonNull Flavor flavor) {
        this.config = config;
        this.sc = sc;
        this.flavor = flavor;
    }

    /**
     * Create a convert job with default flavor
     *
     * @param config job config with all instructions
     * @param sc     Spark context to be used
     */
    public ConvertJob(ConvertConfig config, JavaSparkContext sc) {
        this(config, sc, new DefaultFlavor());
    }


    /**
     * Run the job
     */
    @Override
    public void run() {
        // parsing input file
        QuadParser parser = new ElephasQuadParser(
                config.getParserConfig(),
                sc
        );
        String inputFile = config.getInputFile();
        log.info("Preparing input file: {}", inputFile);
        JavaRDD<Quad> allQuads = parser.parseQuads(inputFile);
        log.info("Done preparing RDD of quads with {} partitions", allQuads.getNumPartitions());

        // modify quads with custom flavor function
        allQuads = flavor.modifyQuads(allQuads);

        // collecting RDF schema with class information
        RdfSchemaCollector rdfSchemaCollector = new RdfSchemaCollector(
                config.getRdfSchemaCollectorConfig()
                        .setAdditionalTypes(InstanceAggregator.getSpecialTypes())
                        .setAdditionalPredicates(InstanceAggregator.getSpecialPredicates()),
                sc
        );
        log.info("Collecting RDF schema");
        RdfSchema rdfSchema = rdfSchemaCollector.collectSchema(
                allQuads
        );
        Broadcast<RdfSchema> broadcastSchema = sc.broadcast(rdfSchema);
        IndexMap<String> localTypeIndex = rdfSchema.getTypeIndex();

        // filtering quads
        QuadFilter quadFilter = new QuadFilter(config.getQuadFilterConfig());
        StorageLevel filterCacheLevel = config.getFilterCacheLevel();
        JavaRDD<Quad> quads = quadFilter.filter(allQuads);
        log.info("Persisting filtered quads with level: {}", filterCacheLevel.description());
        quads.persist(filterCacheLevel);

        // aggregating instances
        InstanceAggregator instanceAggregator = new InstanceAggregator(
                config.getAggregatorConfig(),
                broadcastSchema
        );
        Integer instancePartitions = config.getInstancePartitions();
        if (instancePartitions != null) {
            quads = quads.repartition(instancePartitions);
        }
        JavaRDD<Instance> instances = instanceAggregator.aggregateInstances(quads);

        // filtering instances
        InstanceFilter instanceFilter = new InstanceFilter(config.getInstanceFilterConfig());
        instances = instances.filter(Instance::hasType);
        instances = instanceFilter.filter(instances, localTypeIndex);

        // adding IDs
        InstanceIndexer indexer = new GlobalInstanceIndexer();
        instances = indexer.addIDs(instances);

        // partitioning by type if requested
        InstancePartitioner partitioner = new InstancePartitioner(
                config.getInstancePartitionerConfig()
        );
        instances = partitioner.partition(instances);

        // caching dataset if requested
        StorageLevel cacheLevel = config.getCacheLevel();
        log.info("Persisting instances with level: {}", cacheLevel.description());
        instances.persist(cacheLevel);

        // Prepare reserved names
        Persistor persistor = PersistorFactory.createPersistor(config.getOutputConfig());
        Set<String> reservedNames = new HashSet<>();
        reservedNames.addAll(InstanceRelationWriter.getReservedNames());
        reservedNames.addAll(persistor.getReservedNames());
        // formatting table and column names
        SchemaFormatter formatter = new SchemaFormatter(
                config.getSchemaFormatterConfig()
                        .setFlavor(flavor)
                        .setReservedNames(reservedNames)
        );
        // collecting entity schema
        EntitySchemaCollector entitySchemaCollector = new EntitySchemaCollector(
                config.getEntitySchemaCollectorConfig(),
                formatter,
                rdfSchema
        );
        log.info("Collecting entity schema");
        EntitySchema entitySchema = entitySchemaCollector.collectSchema(instances);
        log.info("Collected entity schema of {} tables", entitySchema.getTables().size());

        // exiting if no tables were extracted
        if (entitySchema.getTables().isEmpty()) {
            log.warn("No entities to be written, exiting.");
            return;
        }

        // remove filtered out types from class graph
        Set<Integer> tableIndexes = entitySchema.getTables().stream()
                .map(table -> localTypeIndex.getIndex(table.getTypeURI()))
                .collect(Collectors.toSet());
        rdfSchema.getClassGraph().keepOnlyClasses(tableIndexes);

        // logging entity schema log.debug is enabled
        if (log.isDebugEnabled()) {
            log.debug("-- ENTITY SCHEMA ----------------------");
            entitySchema.getTables().forEach(table -> {
                log.debug("Table {} has {} columns and {} rows", table.getName(), table.getColumns().size(), table.getNumRows());
            });
        }

        boolean persistRelations = config.getRelationConfig().getSchemaStrategy() != RelationSchemaStrategy.None;
        DataFrame relations = null;
        RelationSchema relationSchema = null;
        if (persistRelations) {
            // if a prefix is supplied, we do not need to avoid entity names in relation table names
            if (!config.getWriterConfig().getRelationTablePrefix().isEmpty()) {
                config.getRelationConfig().setEntityNamesForbidden(false);
            }
            // extracting relations
            RelationExtractor relationExtractor = new RelationExtractor(
                    config.getRelationConfig(),
                    sc,
                    rdfSchema.getClassGraph()
            );
            relations = relationExtractor.extractRelations(instances);
            log.info("Persisting relations with level: {}", cacheLevel.description());
            relations.persist(cacheLevel);

            // collecting relation schema
            RelationSchemaCollector relationSchemaCollector = new RelationSchemaCollector(
                    config.getRelationConfig(),
                    formatter,
                    rdfSchema
            );
            log.info("Collecting relation schema");
            relationSchema = relationSchemaCollector.collectSchema(
                    relations,
                    entitySchema,
                    localTypeIndex
            );
            log.info("Collected relation schema of {} tables", relationSchema.getTables().size());
        } else {
            log.info("Relations are disabled, not collecting.");
        }

        // persisting
        MetadataWriter metadataWriter = new MetadataWriter(sc, persistor, rdfSchema);
        InstanceRelationWriter instanceRelationWriter = new InstanceRelationWriter(
                config.getWriterConfig(), sc, persistor, rdfSchema
        );
        log.info("-- PERSISTING --------------------");
        log.info("Writing to output: {}", config.getOutputConfig());

        // writing metadata tables
        metadataWriter.writeMetadata(entitySchema, relationSchema);

        // writing entity tables
        instanceRelationWriter.writeEntityTables(entitySchema, instances);

        // writing EAV table
        instanceRelationWriter.writeEntityAttributeValueTable(entitySchema, instances);

        // writing relation tables
        if (persistRelations) {
            instanceRelationWriter.writeRelationTables(relationSchema, relations);
        }

        log.info("Conversion done.");
    }

}
