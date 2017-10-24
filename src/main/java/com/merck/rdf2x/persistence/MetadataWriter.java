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
import com.merck.rdf2x.beans.Predicate;
import com.merck.rdf2x.persistence.output.Persistor;
import com.merck.rdf2x.persistence.schema.*;
import com.merck.rdf2x.rdf.LiteralType;
import com.merck.rdf2x.rdf.schema.RdfSchema;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;
import scala.Tuple4;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * MetadataWriter writes metadata tables describing the relational schema.
 */
@ToString
@Slf4j
public class MetadataWriter {
    /**
     * Meta table name prefix
     */
    static final String META_TABLE_PREFIX = "_META_";
    /**
     * Name of meta properties table
     */
    public static final String PROPERTIES_TABLE_NAME = META_TABLE_PREFIX + "Properties";
    /**
     * Name of meta entities table
     */
    public static final String ENTITIES_TABLE_NAME = META_TABLE_PREFIX + "Entities";
    /**
     * Name of meta relations table
     */
    public static final String RELATIONS_TABLE_NAME = META_TABLE_PREFIX + "Relations";
    /**
     * Name of meta properties table
     */
    public static final String PREDICATES_TABLE_NAME = META_TABLE_PREFIX + "Predicates";

    static final String PROPERTIES_COLUMN_NAME = "column_name";
    static final String PROPERTIES_IS_COLUMN = "is_column";
    static final String PROPERTIES_IS_ATTRIBUTE = "is_attribute";
    static final String PROPERTIES_PREDICATE_ID = "predicate";
    static final String PROPERTIES_TYPE = "datatype";
    static final String PROPERTIES_IS_MULTIVALUED = "is_multivalued";
    static final String PROPERTIES_STORED_IN_SUPERCLASS = "in_superclass";
    static final String PROPERTIES_LANGUAGE = "language";
    static final String PROPERTIES_ENTITY_NAME = "entity_name";
    static final String PROPERTIES_NON_NULL_RATIO = "non_null";

    static final String ENTITIES_URI = "entity_URI";
    static final String ENTITIES_NAME = "entity_name";
    static final String ENTITIES_LABEL = "entity_label";
    static final String ENTITIES_NUM_ROWS = "num_rows";

    static final String RELATIONS_NAME = "name";
    static final String RELATIONS_FROM_NAME = "from_name";
    static final String RELATIONS_TO_NAME = "to_name";
    static final String RELATIONS_PREDICATE_ID = "predicate";

    static final String PREDICATE_ID = "predicate";
    static final String PREDICATE_URI = "predicate_URI";
    static final String PREDICATE_LABEL = "predicate_label";

    /**
     * SQLContext used to create Spark DataFrames
     */
    transient private final SQLContext sql;
    /**
     * stats to be printed
     */
    private final List<String> stats;
    /**
     * schema storing information about classes and properties
     */
    private final RdfSchema rdfSchema;

    /**
     * Output persistor that persists DataFrames
     */
    private final Persistor persistor;

    /**
     * @param sc        spark context to be used
     * @param persistor output persistor
     * @param rdfSchema schema storing information about classes and properties
     */
    public MetadataWriter(JavaSparkContext sc, Persistor persistor, RdfSchema rdfSchema) {
        this.sql = new SQLContext(sc);
        this.persistor = persistor;
        this.rdfSchema = rdfSchema;
        this.stats = new ArrayList<>();
    }

    /**
     * Write all metadata tables
     *
     * @param entitySchema   the entity schema
     * @param relationSchema the relation schema
     */
    public void writeMetadata(EntitySchema entitySchema, RelationSchema relationSchema) {
        // write tables
        writePropertyMetadata(entitySchema);
        if (relationSchema != null) {
            writeRelationMetadata(relationSchema);
        }
        writePredicateMetadata();
        writeEntityMetadata(entitySchema);

        // write foreign keys
        writePropertyForeignKeys();
        if (relationSchema != null) {
            writeRelationForeignKeys();
        }
    }

    public void writeRelationForeignKeys() {
        List<Tuple4<String, String, String, String>> foreignKeys = new ArrayList<>();
        foreignKeys.add(new Tuple4<>(RELATIONS_TABLE_NAME, RELATIONS_PREDICATE_ID, PREDICATES_TABLE_NAME, PREDICATE_ID));
        foreignKeys.add(new Tuple4<>(RELATIONS_TABLE_NAME, RELATIONS_FROM_NAME, ENTITIES_TABLE_NAME, ENTITIES_NAME));
        foreignKeys.add(new Tuple4<>(RELATIONS_TABLE_NAME, RELATIONS_TO_NAME, ENTITIES_TABLE_NAME, ENTITIES_NAME));
        persistor.createForeignKeys(foreignKeys);
    }

    public void writePropertyForeignKeys() {
        List<Tuple4<String, String, String, String>> foreignKeys = new ArrayList<>();
        foreignKeys.add(new Tuple4<>(PROPERTIES_TABLE_NAME, PROPERTIES_PREDICATE_ID, PREDICATES_TABLE_NAME, PREDICATE_ID));
        foreignKeys.add(new Tuple4<>(PROPERTIES_TABLE_NAME, PROPERTIES_ENTITY_NAME, ENTITIES_TABLE_NAME, ENTITIES_NAME));
        persistor.createForeignKeys(foreignKeys);
    }

    /**
     * Persist predicate metadata table storing all predicates.
     */
    public void writePredicateMetadata() {

        // create the schema
        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField(PREDICATE_ID, DataTypes.IntegerType, false));
        fields.add(DataTypes.createStructField(PREDICATE_URI, DataTypes.StringType, false));
        fields.add(DataTypes.createStructField(PREDICATE_LABEL, DataTypes.StringType, true));
        StructType schema = DataTypes.createStructType(fields);

        List<Tuple2<String, String>> indexes = new ArrayList<>();
        indexes.add(new Tuple2<>(PREDICATES_TABLE_NAME, PREDICATE_URI));

        List<Tuple2<String, String>> primaryKeys = new ArrayList<>();
        primaryKeys.add(new Tuple2<>(PREDICATES_TABLE_NAME, PREDICATE_ID));


        final IndexMap<String> predicateIndex = rdfSchema.getPredicateIndex();
        final Map<String, String> uriLabels = rdfSchema.getUriLabels();
        // create table rows
        List<Row> rows = predicateIndex.getValues().stream()
                .map(uri -> {
                    Object[] valueArray = new Object[]{
                            predicateIndex.getIndex(uri),
                            uri,
                            uriLabels.get(uri)
                    };
                    return RowFactory.create(valueArray);
                }).collect(Collectors.toList());

        // create and write the META_Predicates dataframe
        DataFrame df = sql.createDataFrame(rows, schema);
        persistor.writeDataFrame(PREDICATES_TABLE_NAME, df);
        persistor.createPrimaryKeys(primaryKeys);
        persistor.createIndexes(indexes);
        df.unpersist();
    }

    /**
     * Write metadata describing relation tables
     *
     * @param relationSchema the relation schema
     */
    public void writeRelationMetadata(RelationSchema relationSchema) {
        // create the schema
        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField(RELATIONS_NAME, DataTypes.StringType, false));
        fields.add(DataTypes.createStructField(RELATIONS_FROM_NAME, DataTypes.StringType, true));
        fields.add(DataTypes.createStructField(RELATIONS_TO_NAME, DataTypes.StringType, true));
        fields.add(DataTypes.createStructField(RELATIONS_PREDICATE_ID, DataTypes.IntegerType, true));

        // create table rows
        List<Row> rows = relationSchema.getTables().stream()
                .map(table -> {
                    RelationPredicateFilter predicateFilter = table.getPredicateFilter();
                    RelationEntityFilter entityFilter = table.getEntityFilter();
                    Object[] valueArray = new Object[]{
                            table.getName(),
                            entityFilter == null ? null : entityFilter.getFromTypeName(),
                            entityFilter == null ? null : entityFilter.getToTypeName(),
                            predicateFilter == null ? null : rdfSchema.getPredicateIndex().getIndex(predicateFilter.getPredicateURI())
                    };
                    return RowFactory.create(valueArray);
                }).collect(Collectors.toList());

        StructType schema = DataTypes.createStructType(fields);

        // add index for each field
        List<Tuple2<String, String>> indexes = fields.stream()
                .map(field -> new Tuple2<>(RELATIONS_TABLE_NAME, field.name()))
                .collect(Collectors.toList());

        // create and write the META_Relations dataframe
        DataFrame df = sql.createDataFrame(rows, schema);
        persistor.writeDataFrame(RELATIONS_TABLE_NAME, df);
        persistor.createIndexes(indexes);
        df.unpersist();
    }

    /**
     * Write metadata describing literal entity properties
     *
     * @param entitySchema the entity schema
     */
    public void writePropertyMetadata(EntitySchema entitySchema) {

        // create the schema
        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField(PROPERTIES_ENTITY_NAME, DataTypes.StringType, false));
        fields.add(DataTypes.createStructField(PROPERTIES_PREDICATE_ID, DataTypes.IntegerType, false));
        fields.add(DataTypes.createStructField(PROPERTIES_COLUMN_NAME, DataTypes.StringType, true));
        fields.add(DataTypes.createStructField(PROPERTIES_IS_COLUMN, DataTypes.BooleanType, false));
        fields.add(DataTypes.createStructField(PROPERTIES_IS_ATTRIBUTE, DataTypes.BooleanType, false));
        fields.add(DataTypes.createStructField(PROPERTIES_IS_MULTIVALUED, DataTypes.BooleanType, false));
        fields.add(DataTypes.createStructField(PROPERTIES_TYPE, DataTypes.StringType, false));
        fields.add(DataTypes.createStructField(PROPERTIES_LANGUAGE, DataTypes.StringType, true));
        fields.add(DataTypes.createStructField(PROPERTIES_NON_NULL_RATIO, DataTypes.DoubleType, true));
        fields.add(DataTypes.createStructField(PROPERTIES_STORED_IN_SUPERCLASS, DataTypes.StringType, true));
        StructType schema = DataTypes.createStructType(fields);

        List<Tuple2<String, String>> indexes = new ArrayList<>();
        indexes.add(new Tuple2<>(PROPERTIES_TABLE_NAME, PROPERTIES_COLUMN_NAME));
        indexes.add(new Tuple2<>(PROPERTIES_TABLE_NAME, PROPERTIES_PREDICATE_ID));

        // create table rows
        List<Row> rows = entitySchema.getTables().stream()
                .flatMap(table -> {

                    Set<EntityProperty> properties = table.getColumns().stream()
                            .map(EntityColumn::getProperty)
                            .collect(Collectors.toSet());
                    properties.addAll(table.getAttributes());

                    Map<EntityProperty, EntityColumn> propertyColumns = table.getColumns().stream()
                            .collect(Collectors.toMap(
                                    EntityColumn::getProperty,
                                    column -> column
                            ));

                    return properties.stream()
                            .map(property -> {
                                EntityColumn column = Objects.requireNonNull(propertyColumns.get(property));
                                Predicate predicate = property.getPredicate();
                                Object[] valueArray = new Object[]{
                                        table.getName(), // entity name
                                        predicate.getPredicateIndex(), // predicate index
                                        column == null ? null : column.getName(), // column name
                                        column != null, // is column
                                        true, // is attribute
                                        property.isMultivalued(), // is multivalued
                                        LiteralType.toString(predicate.getLiteralType()), // datatype
                                        predicate.getLanguage(), // language
                                        property.getNonNullFraction(), // non-null ratio
                                        column == null ? null : column.getStoredInSuperclassColumn() // stored in superclass
                                };
                                return RowFactory.create(valueArray);
                            });
                })
                .collect(Collectors.toList());


        // create and write the META_Columns dataframe
        DataFrame df = sql.createDataFrame(rows, schema);
        persistor.writeDataFrame(PROPERTIES_TABLE_NAME, df);
        persistor.createIndexes(indexes);
        df.unpersist();
    }

    /**
     * Write metadata describing entity tables
     *
     * @param entitySchema the entity schema
     */
    public void writeEntityMetadata(EntitySchema entitySchema) {

        // create the schema
        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField(ENTITIES_NAME, DataTypes.StringType, false));
        fields.add(DataTypes.createStructField(ENTITIES_URI, DataTypes.StringType, false));
        fields.add(DataTypes.createStructField(ENTITIES_LABEL, DataTypes.StringType, true));
        fields.add(DataTypes.createStructField(ENTITIES_NUM_ROWS, DataTypes.LongType, false));
        StructType schema = DataTypes.createStructType(fields);

        List<Tuple2<String, String>> indexes = new ArrayList<>();
        indexes.add(new Tuple2<>(ENTITIES_TABLE_NAME, ENTITIES_URI));

        List<Tuple2<String, String>> primaryKeys = new ArrayList<>();
        indexes.add(new Tuple2<>(ENTITIES_TABLE_NAME, ENTITIES_NAME));

        final Map<String, String> uriLabels = rdfSchema.getUriLabels();
        // create table rows
        List<Row> rows = entitySchema.getTables().stream()
                .map(table -> {
                    Object[] valueArray = new Object[]{
                            table.getName(),
                            table.getTypeURI(),
                            uriLabels.get(table.getTypeURI()),
                            table.getNumRows()
                    };
                    return RowFactory.create(valueArray);
                }).collect(Collectors.toList());

        // create and write the META_Entities dataframe
        DataFrame df = sql.createDataFrame(rows, schema);
        persistor.writeDataFrame(ENTITIES_TABLE_NAME, df);
        persistor.createPrimaryKeys(primaryKeys);
        persistor.createIndexes(indexes);
        df.unpersist();
    }
}
