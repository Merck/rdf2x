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

package com.merck.rdf2x.processing.schema;

import com.merck.rdf2x.beans.IndexMap;
import com.merck.rdf2x.beans.RelationRow;
import com.merck.rdf2x.persistence.schema.*;
import com.merck.rdf2x.processing.formatting.FormatUtil;
import com.merck.rdf2x.processing.formatting.SchemaFormatter;
import com.merck.rdf2x.rdf.schema.RdfSchema;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import scala.Tuple3;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * RelationSchemaCollector collects a {@link RelationSchema} using an {@link EntitySchema} and a RDD of {@link RelationRow}s.
 * <p>
 * Uses a {@link SchemaFormatter} to generate names of tables and columns.
 */
@Slf4j
@RequiredArgsConstructor
public class RelationSchemaCollector {

    /**
     * Name of single relation table
     */
    static final String SINGLE_RELATION_TABLE_NAME = "_relations";
    /**
     * config with all the instructions
     */
    private final RelationConfig config;
    /**
     * schema formatter formatting table and column names
     */
    private final SchemaFormatter formatter;
    /**
     * schema storing information about classes and properties
     */
    private final RdfSchema rdfSchema;

    /**
     * Collect a {@link RelationSchema}
     *
     * @param relations    RDD of relation rows to collect from (predicateIndex, fromTypeIndex, instanceID, toTypeIndex, relatedID)
     * @param entitySchema schema storing entity tables with names and other information
     * @param typeIndex    index of types mapping type URIs to integers
     * @return the collected {@link RelationSchema}
     */
    public RelationSchema collectSchema(DataFrame relations, EntitySchema entitySchema, IndexMap<String> typeIndex) {

        List<RelationTable> tables;
        // return the corresponding schema based on strategy
        switch (config.getSchemaStrategy()) {
            case SingleTable:
                return new RelationSchema(SINGLE_RELATION_TABLE_NAME);

            case Predicates:
                // get map of all relation predicates URIs -> formatted predicate name
                Map<String, String> relationNames1 = getPredicateNames(relations, entitySchema);
                // map pairs of related types to corresponding RelationTables
                tables = relationNames1.keySet().stream().map(predicateURI -> {
                    String name = relationNames1.get(predicateURI);
                    return new RelationTable(name, config.getSchemaStrategy())
                            .setPredicateFilter(new RelationPredicateFilter(predicateURI));
                }).collect(Collectors.toList());
                break;

            case TypePredicates:
                // get map of all relation predicates URIs -> formatted predicate name
                Map<String, String> relationNames2 = getPredicateNames(relations, entitySchema);
                // get all occurring triples of related types and predicates
                Set<Tuple3<String, Integer, Integer>> getUniquePredicateTypeTriples = getUniquePredicateTypeTriples(relations);

                // map pairs of related types to corresponding RelationTables
                tables = getUniquePredicateTypeTriples.stream().map(triple -> {
                    String predicateURI = triple._1();
                    Integer fromTypeIndex = triple._2();
                    Integer toTypeIndex = triple._3();
                    String predicateName = relationNames2.get(predicateURI);
                    String fromTypeName = entitySchema.getTableNames().get(typeIndex.getValue(fromTypeIndex));
                    String toTypeName = entitySchema.getTableNames().get(typeIndex.getValue(toTypeIndex));

                    String name = FormatUtil.getRelationTableName(fromTypeName, toTypeName, predicateName);
                    return new RelationTable(name, config.getSchemaStrategy())
                            .setEntityFilter(new RelationEntityFilter(fromTypeIndex, fromTypeName, toTypeIndex, toTypeName))
                            .setPredicateFilter(new RelationPredicateFilter(predicateURI));
                }).collect(Collectors.toList());
                break;

            default:
                // get all occurring pairs of related types
                List<Row> typeURIPairs = getUniqueTypePairs(relations);

                // map pairs of related types to corresponding RelationTables
                tables = typeURIPairs.stream().map(uriPair -> {
                    Integer fromTypeIndex = uriPair.getInt(0);
                    Integer toTypeIndex = uriPair.getInt(1);
                    String fromTypeName = entitySchema.getTableNames().get(typeIndex.getValue(fromTypeIndex));
                    String toTypeName = entitySchema.getTableNames().get(typeIndex.getValue(toTypeIndex));

                    String name = FormatUtil.getRelationTableName(fromTypeName, toTypeName);
                    return new RelationTable(name, config.getSchemaStrategy())
                            .setEntityFilter(new RelationEntityFilter(fromTypeIndex, fromTypeName, toTypeIndex, toTypeName));
                }).collect(Collectors.toList());
        }

        return new RelationSchema(tables);
    }


    /**
     * Get names of relation predicates
     *
     * @param relations    DataFrame of relations (predicateIndex, fromTypeIndex, instanceID, toTypeIndex, relatedID)
     * @param entitySchema schema storing entity tables with names and other information
     * @return map of predicate URI - predicate name
     */
    private Map<String, String> getPredicateNames(DataFrame relations, EntitySchema entitySchema) {
        // get all occurring pairs of related types
        Set<String> predicateURIs = getUniquePredicateURIs(relations);
        // format relation names
        Set<String> tableNames = new HashSet<>();
        if (config.isEntityNamesForbidden()) {
            tableNames.addAll(entitySchema.getTableNames().values());
        }
        return formatter.getRelationNames(predicateURIs, rdfSchema.getUriLabels(), tableNames);
    }

    /**
     * Get all unique pairs of type indexes of related entities.
     *
     * @param relations DataFrame of relations (predicateIndex, fromTypeIndex, instanceID, toTypeIndex, relatedID)
     * @return all unique pairs of type URIs of related entities (fromTypeIndex, toTypeIndex)
     */
    private List<Row> getUniqueTypePairs(DataFrame relations) {
        return relations
                .select("fromTypeIndex", "toTypeIndex")
                .distinct()
                .collectAsList();
    }

    /**
     * Get all unique triples of (predicate URI, source type index, target type index) of related entities.
     *
     * @param relations DataFrame of relations (predicateIndex, fromTypeIndex, instanceID, toTypeIndex, relatedID)
     * @return all unique triples of (predicate URI, source type index, target type index) of related entities
     */
    private Set<Tuple3<String, Integer, Integer>> getUniquePredicateTypeTriples(DataFrame relations) {
        return relations
                .select("predicateIndex", "fromTypeIndex", "toTypeIndex")
                .distinct()
                .collectAsList().stream()
                .map(row -> new Tuple3<>(rdfSchema.getPredicateIndex().getValue(row.getInt(0)), row.getInt(1), row.getInt(2)))
                .collect(Collectors.toSet());
    }

    /**
     * Get all unique predicate URIs used in relations.
     *
     * @param relations DataFrame of relations (predicateIndex, fromTypeIndex, instanceID, toTypeIndex, relatedID)
     * @return all unique predicate URIs used in relations
     */
    private Set<String> getUniquePredicateURIs(DataFrame relations) {
        return relations
                .select("predicateIndex")
                .distinct()
                .collectAsList().stream()
                .map(predicateIndex -> rdfSchema.getPredicateIndex().getValue(predicateIndex.getInt(0)))
                .collect(Collectors.toSet());
    }
}
