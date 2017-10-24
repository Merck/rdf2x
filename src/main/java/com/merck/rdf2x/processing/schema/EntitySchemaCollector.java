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
import com.merck.rdf2x.beans.Instance;
import com.merck.rdf2x.beans.Predicate;
import com.merck.rdf2x.persistence.schema.EntityColumn;
import com.merck.rdf2x.persistence.schema.EntityProperty;
import com.merck.rdf2x.persistence.schema.EntitySchema;
import com.merck.rdf2x.persistence.schema.EntityTable;
import com.merck.rdf2x.processing.formatting.FormatUtil;
import com.merck.rdf2x.processing.formatting.SchemaFormatter;
import com.merck.rdf2x.rdf.schema.ClassGraph;
import com.merck.rdf2x.rdf.schema.RdfSchema;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;
import scala.Tuple3;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * EntitySchemaCollector collects an {@link EntitySchema} from a RDD of {@link Instance}s.
 * <p>
 * Uses a {@link SchemaFormatter} to generate names of tables and columns.
 */
@Slf4j
@RequiredArgsConstructor
public class EntitySchemaCollector implements Serializable {
    /**
     * config with all the collection instructions
     */
    private final EntitySchemaCollectorConfig config;
    /**
     * schema formatter formatting table and column names
     */
    private final SchemaFormatter formatter;
    /**
     * schema storing information about classes and properties
     */
    private final RdfSchema rdfSchema;

    /**
     * Collect schema
     *
     * @param instances RDD of instances to extract the schema from
     * @return the collected {@link EntitySchema}
     */
    public EntitySchema collectSchema(JavaRDD<Instance> instances) {

        final IndexMap<String> typeIndex = rdfSchema.getTypeIndex();
        final IndexMap<String> predicateIndex = rdfSchema.getPredicateIndex();

        log.info("Counting instances by type");
        Map<Integer, Long> typeCounts = countInstancesByType(instances);

        log.info("Getting used predicates of types");
        // get all distinct predicates of each type and their properties (number of occurrences, is multiple)
        Map<Integer, List<EntityProperty>> typeProperties = getDistinctEntityProperties(instances, typeCounts);

        log.info("Getting all used predicate URIs");
        // get predicate URIs used in properties for name formatting
        Set<String> predicateURIs = typeProperties.values().stream()
                .flatMap(list -> list.stream()
                        .map(triple -> predicateIndex.getValue(triple.getPredicate().getPredicateIndex()))
                )
                .collect(Collectors.toSet());
        log.info("Got {} used predicate URIs", predicateURIs.size());

        log.info("Getting all used type URIs");
        // get used type URIs for name formatting (type index is not used since some types might have been filtered out)
        Set<String> typeURIs = typeCounts.keySet().stream().map(typeIndex::getValue).collect(Collectors.toSet());
        log.info("Got {} used type URIs", typeURIs.size());

        Map<String, String> typeNames = formatter.getTypeNames(typeURIs, rdfSchema.getUriLabels());
        Map<String, String> propertyNames = formatter.getPropertyNames(predicateURIs, rdfSchema.getUriLabels());

        log.info("Creating table objects");
        // map the predicate groups into EntityTables
        List<EntityTable> tables = typeCounts.keySet().stream()
                .map(tableIndex -> {
                    List<EntityProperty> properties = typeProperties.getOrDefault(tableIndex, new ArrayList<>());
                    Long instanceCount = typeCounts.getOrDefault(tableIndex, 0L);
                    String tableURI = typeIndex.getValue(tableIndex);
                    return createTableWithProperties(typeNames.get(tableURI), tableURI, instanceCount, propertyNames, properties);
                })
                .filter(table -> table.getNumRows() >= config.getMinNumRows())
                .sorted((a, b) -> a.getName().compareTo(b.getName()))
                .collect(Collectors.toList());

        if (!config.isRedundantSubclassColumns()) {
            markRedundantSubclassColumns(tables);
        }

        return new EntitySchema(tables, typeNames, propertyNames);
    }

    /**
     * Mark columns that are already stored in a superclass table using {@link EntityColumn} storedInSuperclassColumn field.
     *
     * @param tables tables containing columns to be marked
     */
    private void markRedundantSubclassColumns(List<EntityTable> tables) {
        ClassGraph graph = rdfSchema.getClassGraph();
        IndexMap<String> typeIndex = rdfSchema.getTypeIndex();

        Map<Integer, EntityTable> indexToTableMap = tables.stream()
                .collect(Collectors.toMap(
                        table -> typeIndex.getIndex(table.getTypeURI()),
                        table -> table
                ));

        for (Integer tableIndex : graph.inSuperClassFirstOrder()) {
            EntityTable table = indexToTableMap.get(tableIndex);
            // skip if table is not present in schema (type was filtered out)
            if (table == null) {
                continue;
            }
            for (EntityColumn column : table.getColumns()) {
                for (Integer ancestorIndex : graph.getSuperClasses(tableIndex)) {
                    EntityTable ancestorTable = indexToTableMap.get(ancestorIndex);
                    // skip if table is not present in schema (type was filtered out)
                    if (ancestorTable == null) {
                        continue;
                    }
                    // look for a duplicate column in ancestor table by comparing Predicates
                    Optional<EntityColumn> duplicateColumn = ancestorTable.getColumns().stream()
                            .filter(ancestorColumn -> ancestorColumn.getProperty().getPredicate().equals(column.getProperty().getPredicate()))
                            .findFirst();
                    // if a duplicate is found in the superclass
                    if (duplicateColumn.isPresent()) {
                        // check if the ancestor already marked this column as stored in its own superclass
                        String superclassColumn = duplicateColumn.get().getStoredInSuperclassColumn();
                        // if this superclass is the first one to store this column, mark its storage here
                        if (superclassColumn == null) {
                            superclassColumn = table.getName() + "." + duplicateColumn.get().getName();
                        }
                        column.setStoredInSuperclassColumn(superclassColumn);
                        break;
                    }
                }
            }
        }
    }

    private Map<Integer, Long> countInstancesByType(JavaRDD<Instance> instances) {
        return instances.flatMap(Instance::getTypes).countByValue();
    }

    /**
     * Reduce a RDD of {@link Instance}s into a map of [type index -&gt; list of its {@link Predicate}s and their properties (occurrences, is multiple)]
     *
     * @param instances  a RDD of {@link Instance}s
     * @param typeCounts map of type indexes to counts of their instances
     * @return map of [type index -&gt; list of its {@link Predicate}s and their properties (occurrences, is multiple)]
     */
    private Map<Integer, List<EntityProperty>> getDistinctEntityProperties(JavaRDD<Instance> instances, Map<Integer, Long> typeCounts) {

        // all triples of (instance type, instance predicate, is multiple valued predicate)
        JavaRDD<Tuple3<Integer, Predicate, Boolean>> typePredicates = instances.flatMap(instance -> {
            Set<Predicate> predicates = instance.getLiteralPredicates();
            return instance.getTypes().stream()
                    .flatMap(typeInt -> predicates.stream()
                            .map(predicate -> new Tuple3<>(
                                    typeInt, // type index
                                    predicate, // predicate
                                    instance.getLiteralValue(predicate) instanceof Set // is multiple valued
                            ))
                    ).collect(Collectors.toList());
        });

        return typePredicates
                .mapToPair(typePredicate -> new Tuple2<>(
                                new Tuple2<>(typePredicate._1(), typePredicate._2()), // predicate in type
                                new Tuple2<>(1L, typePredicate._3()) // count, is multiple valued
                        )
                )
                // get properties of each predicate in a specific type (will become a column)
                .reduceByKey((a, b) -> new Tuple2<>(
                        a._1() + b._1(), // sum counts
                        a._2() || b._2() // is multiple if it is multiple in any instance
                ))
                // collect to Java list
                .collect().stream()
                // group by type -> list of predicates and their properties
                .collect(Collectors.groupingBy(
                        typePredicate -> typePredicate._1()._1(),
                        Collectors.mapping(
                                typePredicate -> new EntityProperty(
                                        typePredicate._1()._2(), // predicate index
                                        typePredicate._2()._2(), // is multiple
                                        typePredicate._2()._1() / ((double) typeCounts.get(typePredicate._1()._1())) // non-null ratio
                                ),
                                Collectors.toList())
                ));

    }

    /**
     * Create an {@link EntityTable} from a table name, type URI and a group of its predicates
     *
     * @param tableName     the table name
     * @param tableURI      the URI of the source type
     * @param instanceCount number of rows in the table
     * @param propertyNames map of predicate URIs to unique predicate names
     * @param properties    list of entity properties to be stored in columns or EAV table
     * @return an {@link EntityTable} with columns based on the predicates
     */
    private EntityTable createTableWithProperties(String tableName,
                                                  String tableURI,
                                                  Long instanceCount,
                                                  Map<String, String> propertyNames,
                                                  List<EntityProperty> properties) {

        final IndexMap<String> predicateIndex = rdfSchema.getPredicateIndex();

        Double minNonNullFraction = config.getMinColumnNonNullFraction();

        // whether the EAV table stores also values already included in columns
        boolean redundantEAV = config.isRedundantEAV();
        // EAV predicates are those rarer than eavNonNullRatio + multiple valued (or all if redundant EAV table is enabled)
        Set<EntityProperty> attributes = properties.stream()
                .filter(property -> redundantEAV || property.getNonNullFraction() < minNonNullFraction || property.isMultivalued())
                .collect(Collectors.toSet());

        // column predicates are those more frequent than eavNonNullRatio
        List<EntityProperty> columnCandidateProperties = properties.stream()
                .filter(property -> property.getNonNullFraction() >= minNonNullFraction)
                .sorted((a, b) -> b.getNonNullFraction().compareTo(a.getNonNullFraction())) // sort by most common columns first
                .collect(Collectors.toList());

        Stream<EntityProperty> columnProperties = columnCandidateProperties.stream();

        // limit maximum number of columns, add rest to EAV
        Integer limit = config.getMaxNumColumns();
        if (limit != null && columnCandidateProperties.size() > limit) {
            columnProperties = columnProperties.limit(limit);
            attributes.addAll(columnCandidateProperties.stream()
                    .skip(limit)
                    .collect(Collectors.toList())
            );
        }

        List<EntityColumn> columns = new ArrayList<>();
        Set<String> columnNames = new HashSet<>();
        boolean forceTypeSuffix = config.isForceTypeSuffix();
        for (EntityProperty property : columnProperties.collect(Collectors.toList())) {
            Predicate predicate = property.getPredicate();
            String predicateUri = predicateIndex.getValue(predicate.getPredicateIndex());
            String name = FormatUtil.addLanguageSuffix(propertyNames.get(predicateUri), predicate.getLanguage());
            // add type suffix only if requested or if the column without suffix is already present
            if (forceTypeSuffix || columnNames.contains(name)) {
                name = FormatUtil.addTypeSuffix(name, predicate.getLiteralType());
            }
            EntityColumn column = new EntityColumn(name, property);
            columns.add(column);
            columnNames.add(name);
        }

        if (config.isSortColumnsAlphabetically()) {
            Collections.sort(columns, Comparator.comparing(EntityColumn::getName));
        }
        return new EntityTable(tableName, tableURI, instanceCount, columns, attributes);
    }


}
