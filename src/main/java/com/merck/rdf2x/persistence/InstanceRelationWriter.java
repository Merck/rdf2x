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

package com.merck.rdf2x.persistence;

import com.merck.rdf2x.beans.IndexMap;
import com.merck.rdf2x.beans.Instance;
import com.merck.rdf2x.beans.Predicate;
import com.merck.rdf2x.beans.RelationRow;
import com.merck.rdf2x.persistence.output.Persistor;
import com.merck.rdf2x.persistence.schema.*;
import com.merck.rdf2x.rdf.LiteralType;
import com.merck.rdf2x.rdf.schema.ClassGraph;
import com.merck.rdf2x.rdf.schema.RdfSchema;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;
import scala.Tuple4;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * InstanceRelationsWriter writes {@link Instance}s and {@link RelationRow}s in various output targets.
 */
@ToString
@Slf4j
public class InstanceRelationWriter implements Serializable {

    static final String PREDICATE_COLUMN_NAME = "predicate";
    static final String URI_COLUMN_NAME = "URI";
    static final String ID_COLUMN_NAME = "ID";
    static final String ID_SUFFIX_JOINER = "_";
    static final String ID_FROM_SUFFIX = "_from";
    static final String ID_TO_SUFFIX = "_to";
    static final String EAV_TABLE_NAME = "_attributes";
    static final String EAV_LANGUAGE_COLUMN_NAME = "language";
    static final String EAV_DATATYPE_COLUMN_NAME = "datatype";
    static final String EAV_VALUE_COLUMN_NAME = "value";
    /**
     * database connection and output format config
     */
    private final InstanceRelationWriterConfig config;
    /**
     * SQLContext used to create Spark DataFrames
     */
    transient private final SQLContext sql;
    /**
     * Output persistor that persists DataFrames
     */
    transient private final Persistor persistor;
    /**
     * stats to be printed
     */
    transient private final List<String> stats;
    /**
     * RDF schema containing subclass information and URI index
     */
    private final RdfSchema rdfSchema;

    /**
     * @param config    database connection and output format config
     * @param sc        spark context to be used
     * @param persistor output persistor
     * @param rdfSchema RDF schema containing subclass information and URI index
     */
    public InstanceRelationWriter(InstanceRelationWriterConfig config, JavaSparkContext sc, Persistor persistor, RdfSchema rdfSchema) {
        this.config = config;
        this.sql = new SQLContext(sc);
        this.persistor = persistor;
        this.rdfSchema = rdfSchema;
        this.stats = new ArrayList<>();
    }

    /**
     * Persist entity tables.
     *
     * @param entitySchema entity schema
     * @param instances    RDD of {@link Instance}s
     */
    public void writeEntityTables(EntitySchema entitySchema, JavaRDD<Instance> instances) {
        List<Tuple2<String, String>> primaryKeys = new ArrayList<>();
        List<Tuple4<String, String, String, String>> foreignKeys = new ArrayList<>();
        List<Tuple2<String, String>> indexes = new ArrayList<>();
        IndexMap<String> typeIndex = rdfSchema.getTypeIndex();
        int done = 0;

        List<EntityTable> sortedTables = getSortedTables(entitySchema.getTables());

        // get set of all type indexes, used to filter out superclasses that are not persisted
        Set<Integer> usedTypes = entitySchema.getTables().stream()
                .map(table -> typeIndex.getIndex(table.getTypeURI()))
                .collect(Collectors.toSet());

        Map<Integer, Set<Integer>> partitionTypes = instances.mapPartitionsWithIndex(
                (index, iterator) -> {
                    Iterable<Instance> iterable = () -> iterator;
                    Set<Integer> types = StreamSupport.stream(iterable.spliterator(), true)
                            .flatMap(instance -> instance.getTypes().stream())
                            .collect(Collectors.toSet());
                    return Collections.singleton(new Tuple2<>(index, types)).iterator();
                },
                false)
                .mapToPair(tuple -> tuple)
                .collectAsMap();

        for (EntityTable table : sortedTables) {
            done++;
            Integer tableIndex = typeIndex.getIndex(table.getTypeURI());
            String name = config.getEntityTablePrefix() + table.getName();

            // create the schema
            List<StructField> fields = new ArrayList<>();
            List<Predicate> predicates = new ArrayList<>();
            fields.add(DataTypes.createStructField(ID_COLUMN_NAME, DataTypes.LongType, false));
            fields.add(DataTypes.createStructField(URI_COLUMN_NAME, DataTypes.StringType, false));
            for (EntityColumn column : table.getColumns()) {
                // skip column if already present in superclass
                if (column.getStoredInSuperclassColumn() != null) {
                    continue;
                }
                Predicate predicate = column.getProperty().getPredicate();
                StructField field = DataTypes.createStructField(column.getName(), getDataType(predicate.getLiteralType()), true);
                fields.add(field);
                predicates.add(predicate);
            }
            StructType schema = DataTypes.createStructType(fields);

            // add indexes to list
            primaryKeys.add(new Tuple2<>(name, ID_COLUMN_NAME));
            indexes.add(new Tuple2<>(name, URI_COLUMN_NAME));
            // add foreign key to add superclasses on ID
            ClassGraph classGraph = rdfSchema.getClassGraph();
            foreignKeys.addAll(
                    classGraph.getSuperClasses(tableIndex).stream()
                            .filter(usedTypes::contains)
                            .map(ancestor -> new Tuple4<>(
                                    name,
                                    ID_COLUMN_NAME,
                                    config.getEntityTablePrefix() + entitySchema.getTableNames().get(typeIndex.getValue(ancestor)),
                                    ID_COLUMN_NAME
                            ))
                            .collect(Collectors.toList())
            );

            // filter out partitions that do not contain the type
            JavaRDD<Instance> selectedInstances = instances.mapPartitionsWithIndex(
                    (pi, iterator) -> partitionTypes.get(pi).contains(tableIndex) ? iterator : Collections.emptyIterator(),
                    true
            );

            // filter instances by type
            selectedInstances = selectedInstances.filter(i -> i.getTypes().contains(tableIndex));

            // create table rows
            int numPredicates = predicates.size();
            JavaRDD<Row> rowRDD = selectedInstances.map(instance -> {
                Object[] valueArray = new Object[numPredicates + 2];
                valueArray[0] = instance.getId();
                valueArray[1] = instance.getUri();
                for (int i = 0; i < numPredicates; i++) {
                    Object value = instance.getLiteralValue(predicates.get(i));
                    valueArray[i + 2] = value instanceof Set ? ((Set) value).iterator().next() : value;
                }
                return RowFactory.create(valueArray);
            });

            // create and write the dataframe
            log.info("Writing entity {} of {} rows and {} columns ({}/{})", table.getName(), table.getNumRows(), table.getColumns().size(), done, entitySchema.getTables().size());
            DataFrame df = sql.createDataFrame(rowRDD, schema);
            persistor.writeDataFrame(name, df);
            df.unpersist();
        }

        // create indexes
        log.info("Creating indexes for entity tables");
        persistor.createPrimaryKeys(primaryKeys);
        persistor.createForeignKeys(foreignKeys);
        persistor.createIndexes(indexes);
    }

    /**
     * Sort tables in topological order guaranteeing that every subclass precedes its superclasses.
     * This is done to avoid errors when overwriting tables with existing foreign key constraints.
     *
     * @param tables list of tables to sort
     * @return tables sorted in topological order guaranteeing that every subclass precedes its superclasses
     */
    private List<EntityTable> getSortedTables(List<EntityTable> tables) {

        // Get order index for all classes in topological order guaranteeing that every subclass precedes its superclass
        Map<String, Integer> typeOrder = new HashMap<>();
        int order = 0;
        for (Integer typeIndex : rdfSchema.getClassGraph().inSuperClassFirstOrder()) {
            typeOrder.put(rdfSchema.getTypeIndex().getValue(typeIndex), order--);
        }
        return tables.stream()
                .sorted(Comparator.comparingInt(table -> typeOrder.getOrDefault(table.getTypeURI(), 0)))
                .collect(Collectors.toList());
    }

    /**
     * Persist the Entity Attribute Value table
     *
     * @param entitySchema entity schema
     * @param instances    RDD of {@link Instance}s
     */
    public void writeEntityAttributeValueTable(EntitySchema entitySchema, JavaRDD<Instance> instances) {

        IndexMap<String> typeIndex = rdfSchema.getTypeIndex();
        // create the schema
        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField(ID_COLUMN_NAME, DataTypes.LongType, false));
        fields.add(DataTypes.createStructField(PREDICATE_COLUMN_NAME, DataTypes.IntegerType, false));
        fields.add(DataTypes.createStructField(EAV_DATATYPE_COLUMN_NAME, DataTypes.StringType, true));
        fields.add(DataTypes.createStructField(EAV_LANGUAGE_COLUMN_NAME, DataTypes.StringType, true));
        fields.add(DataTypes.createStructField(EAV_VALUE_COLUMN_NAME, DataTypes.StringType, false));
        StructType schema = DataTypes.createStructType(fields);

        List<Tuple2<String, String>> indexes = new ArrayList<>();
        indexes.add(new Tuple2<>(EAV_TABLE_NAME, ID_COLUMN_NAME));
        indexes.add(new Tuple2<>(EAV_TABLE_NAME, PREDICATE_COLUMN_NAME));
        indexes.add(new Tuple2<>(EAV_TABLE_NAME, EAV_DATATYPE_COLUMN_NAME));
        indexes.add(new Tuple2<>(EAV_TABLE_NAME, EAV_LANGUAGE_COLUMN_NAME));

        // get map of type index -> set of attributes
        Map<Integer, Set<Predicate>> typeEavPredicates = entitySchema.getTables().stream()
                .collect(Collectors.toMap(
                        table -> typeIndex.getIndex(table.getTypeURI()),
                        table -> table.getAttributes().stream()
                                .map(EntityProperty::getPredicate)
                                .collect(Collectors.toSet())
                ));

        // get all entity attribute values
        JavaRDD<Row> rowRDD = instances.flatMap(instance ->
                instance.getLiteralPredicates().stream()
                        // filter predicates that are in the EAV set of at least one of the instance types
                        .filter(predicate -> instance.getTypes().stream().anyMatch(type ->
                                typeEavPredicates.containsKey(type) && // type could have been removed (not enough rows, ...)
                                        typeEavPredicates.get(type).contains(predicate)
                        ))
                        // map to row of values
                        .flatMap(predicate -> {
                                    Object value = instance.getLiteralValue(predicate);
                                    if (value instanceof Set) {
                                        // return a row for each single value
                                        return ((Set<Object>) value).stream().map(val -> getAttributeRow(instance, predicate, val));
                                    }
                                    return Stream.of(getAttributeRow(instance, predicate, value));//getAttributeRow(instance, predicate, value)
                                }
                        )
                        .collect(Collectors.toList())
        );

        int predicateCount = typeEavPredicates.values().stream().collect(Collectors.summingInt(Set::size));

        // create and write the dataframe
        log.info("Writing EAV table of {} predicates", predicateCount);
        DataFrame df = sql.createDataFrame(rowRDD, schema);
        persistor.writeDataFrame(EAV_TABLE_NAME, df);
        log.info("Creating indexes for EAV table");
        persistor.createIndexes(indexes);
        df.unpersist();
    }

    private static Row getAttributeRow(Instance instance, Predicate predicate, Object value) {
        return RowFactory.create(
                instance.getId(),
                predicate.getPredicateIndex(),
                LiteralType.toString(predicate.getLiteralType()),
                predicate.getLanguage(),
                value.toString()
        );
    }

    /**
     * Persist relation tables.
     *
     * @param relationSchema relation schema
     * @param relations      relation rows (predicateIndex, fromTypeIndex, instanceID, toTypeIndex, relatedID)
     */
    public void writeRelationTables(RelationSchema relationSchema, DataFrame relations) {
        IndexMap<String> predicateIndex = rdfSchema.getPredicateIndex();
        List<Tuple2<String, String>> indexes = new ArrayList<>();
        List<Tuple4<String, String, String, String>> foreignKeys = new ArrayList<>();

        int done = 0;
        for (RelationTable table : relationSchema.getTables()) {
            done++;
            // no prefix is added if all relations are stored in a single table
            String name = table.isSingleTable() ? table.getName() : config.getRelationTablePrefix() + table.getName();
            // in default, column names are 'ID_from' and 'ID_to'
            String fromIdName = ID_COLUMN_NAME + ID_FROM_SUFFIX;
            String toIdName = ID_COLUMN_NAME + ID_TO_SUFFIX;
            // whether predicate column is stored
            boolean isPredicateStored = config.isStorePredicate();
            // store all relations in default
            DataFrame selectedRelations = relations;

            RelationPredicateFilter predicateFilter = table.getPredicateFilter();
            RelationEntityFilter entityFilter = table.getEntityFilter();

            // if table is bound to a specific predicate
            if (predicateFilter != null) {
                // filter relation rows by predicate index
                selectedRelations = selectedRelations
                        .where(
                                selectedRelations.col("predicateIndex")
                                        .equalTo(predicateIndex.getIndex(predicateFilter.getPredicateURI()))
                        );
                isPredicateStored = false;
            }
            // if table is bound to specific source and target types
            if (entityFilter != null) {
                // name columns 'entity1_ID_from' and 'entity2_ID_to'
                fromIdName = entityFilter.getFromTypeName() + ID_SUFFIX_JOINER + ID_COLUMN_NAME;
                toIdName = entityFilter.getToTypeName() + ID_SUFFIX_JOINER + ID_COLUMN_NAME;
                // if source and target is the same, set name to 'entity_ID_from' and 'entity_ID_to'
                boolean isSameType = entityFilter.getFromTypeIndex().equals(entityFilter.getToTypeIndex());
                if (isSameType) {
                    fromIdName += ID_FROM_SUFFIX;
                    toIdName += ID_TO_SUFFIX;
                }

                // filter relation rows by from type & to type
                selectedRelations = selectedRelations
                        .where(selectedRelations.col("fromTypeIndex").equalTo(entityFilter.getFromTypeIndex()))
                        .where(selectedRelations.col("toTypeIndex").equalTo(entityFilter.getToTypeIndex()));

                // add foreign key referencing source table
                foreignKeys.add(new Tuple4<>(
                        name,
                        fromIdName,
                        config.getEntityTablePrefix() + entityFilter.getFromTypeName(),
                        ID_COLUMN_NAME
                ));
                // add foreign key referencing target table
                foreignKeys.add(new Tuple4<>(
                        name,
                        toIdName,
                        config.getEntityTablePrefix() + entityFilter.getToTypeName(),
                        ID_COLUMN_NAME
                ));
            }

            selectedRelations = selectedRelations
                    .select("fromID", "toID", "predicateIndex")
                    .withColumnRenamed("fromID", fromIdName)
                    .withColumnRenamed("toID", toIdName)
                    .withColumnRenamed("predicateIndex", PREDICATE_COLUMN_NAME);

            if (!isPredicateStored) {
                selectedRelations = selectedRelations.drop(PREDICATE_COLUMN_NAME);
            }

            // add index for each field
            indexes.addAll(Stream.of(selectedRelations.schema().fields())
                    .map(field -> new Tuple2<>(name, field.name()))
                    .collect(Collectors.toList()));

            // create and write the dataframe
            log.info("Writing relation {} ({}/{})", table.getName(), done, relationSchema.getTables().size());
            persistor.writeDataFrame(name, selectedRelations);
        }
        log.info("Creating indexes for relation tables");
        persistor.createIndexes(indexes);
        persistor.createForeignKeys(foreignKeys);
    }

    private DataType getDataType(int type) {
        switch (type) {
            case LiteralType.BOOLEAN:
                return DataTypes.BooleanType;
            case LiteralType.STRING:
                return DataTypes.StringType;
            case LiteralType.FLOAT:
                return DataTypes.FloatType;
            case LiteralType.DOUBLE:
                return DataTypes.DoubleType;
            case LiteralType.INTEGER:
                return DataTypes.IntegerType;
            case LiteralType.LONG:
                return DataTypes.LongType;
            case LiteralType.DATETIME:
                // datetime not supported due to timezone issues with java.sql.Timestamp
                // check the InstanceAggregator for more info
                return DataTypes.StringType;
        }
        throw new NotImplementedException("Not able to write literal type " + type);
    }


    public static Set<String> getReservedNames() {
        return new HashSet<>(Arrays.asList(ID_COLUMN_NAME, URI_COLUMN_NAME));
    }
}
