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

package com.merck.rdf2x.processing.relations;

import com.merck.rdf2x.beans.Instance;
import com.merck.rdf2x.beans.RelationRow;
import com.merck.rdf2x.processing.schema.RelationConfig;
import com.merck.rdf2x.processing.schema.RelationSchemaStrategy;
import com.merck.rdf2x.rdf.schema.ClassGraph;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.merck.rdf2x.processing.schema.RelationSchemaStrategy.Predicates;
import static com.merck.rdf2x.processing.schema.RelationSchemaStrategy.SingleTable;

/**
 * RelationExtractor extracts {@link RelationRow}s from a RDD of {@link Instance}s.
 */
public class RelationExtractor implements Serializable {

    private final RelationConfig config;
    /**
     * SQLContext used to create Spark DataFrames
     */
    transient private final SQLContext sql;
    /**
     * classGraph stores subclass information
     */
    private final ClassGraph classGraph;

    public RelationExtractor(RelationConfig config, JavaSparkContext sc, ClassGraph classGraph) {
        this.config = config;
        this.sql = new SQLContext(sc);
        this.classGraph = classGraph;
    }

    /**
     * Extract a DataFrame of relation rows from {@link Instance}s
     *
     * @param instances RDD of {@link Instance}s
     * @return DataFrame of relations (predicateIndex, fromTypeIndex, instanceID, toTypeIndex, relatedID)
     */
    public DataFrame extractRelations(JavaRDD<Instance> instances) {

        // each Instance knows only the URIs of the related resources, not the IDs and types
        // create row of relations described by related instance URI
        DataFrame relationSources = sql.createDataFrame(
                instances.flatMap(this::getRelatedTypeIDs),
                new StructType()
                        .add("toURI", DataTypes.StringType, false)
                        .add("predicateIndex", DataTypes.IntegerType, false)
                        .add("fromTypeIndex", DataTypes.IntegerType, false)
                        .add("fromID", DataTypes.LongType, false)
        ).coalesce(instances.getNumPartitions() / 2 + 1);

        // create dataframe to map instance URIs to IDs
        DataFrame instanceURIToID = sql.createDataFrame(
                instances.flatMap(this::getInstanceTypeIDs),
                new StructType()
                        .add("toURI", DataTypes.StringType, false)
                        .add("toTypeIndex", DataTypes.IntegerType, false)
                        .add("toID", DataTypes.LongType, false)
        ).coalesce(instances.getNumPartitions() / 2 + 1);


        // (predicateIndex, fromTypeIndex, instanceID, toTypeIndex, relatedID)
        return relationSources
                .join(instanceURIToID, "toURI")
                .drop("toURI");
    }

    /**
     * Get stream of entity types to based on given strategy, relation row will be created for each one
     *
     * @param instance instance to get types from
     * @return stream of entity types based on given strategy, relation row will be created for each one
     */
    private Stream<Integer> getRelationEntityTypes(Instance instance) {
        RelationSchemaStrategy strategy = config.getSchemaStrategy();
        // in default, return all types
        Stream<Integer> types = instance.getTypes().stream();
        if (strategy == Predicates || strategy == SingleTable) {
            // create row for one type only
            types = types.limit(1);
        } else if (config.isRootTypesOnly()) {
            // filter out types that have a superclass to ensure relations are only created between root classes
            if (instance.getTypes().size() > 1) {
                types = types.filter(type -> !classGraph.hasSuperClasses(type));
            }
        }
        return types;
    }

    /**
     * Map all types of an {@link Instance} to a row of (instance URI, instance type index, instance ID)
     *
     * @param instance the requested {@link Instance}
     * @return a row of (instance URI, instance type index, instance ID)
     */
    private Iterable<Row> getInstanceTypeIDs(Instance instance) {
        String instanceURI = instance.getUri();
        Long instanceID = instance.getId();

        return getRelationEntityTypes(instance)
                .map(typeIndex -> RowFactory.create(instanceURI, typeIndex, instanceID))
                .collect(Collectors.toList());
    }

    /**
     * Map a {@link Instance} into an Iterator of all of its relations
     * represented as rows of (related URI, predicate index, type index, instance ID)
     *
     * @param instance the requested {@link Instance}
     * @return an Iterator of all of its relations represented as rows of (related URI, predicate index, type index, instance ID)
     */
    private Iterable<Row> getRelatedTypeIDs(Instance instance) {
        // typeIDs representing references to the instance in each table (or a single one, if instance has a single type)
        final Long id = instance.getId();

        final List<Tuple2<Integer, Long>> instanceTypeIDs = getRelationEntityTypes(instance)
                .map(typeIndex -> new Tuple2<>(typeIndex, id))
                .collect(Collectors.toList());

        return instance.getRelations().stream()
                .flatMap(relation ->
                        instanceTypeIDs.stream()
                                .map(instanceTypeID -> RowFactory.create(
                                        relation.getObjectURI(),
                                        relation.getPredicateIndex(),
                                        instanceTypeID._1(),
                                        instanceTypeID._2()
                                ))
                ).collect(Collectors.toList());
    }
}
