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

package com.merck.rdf2x.processing.formatting;


import com.merck.rdf2x.flavors.Flavor;
import lombok.extern.slf4j.Slf4j;
import scala.Tuple2;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * SchemaFormatter formats entity table and column names.
 */
@Slf4j
public class SchemaFormatter {

    /**
     * Config with the specified formatting properties
     */
    private final SchemaFormatterConfig config;

    public SchemaFormatter(SchemaFormatterConfig config) {
        this.config = config;
    }

    /**
     * Get map of type names for specified URIs, formatted from their String labels
     *
     * @param typeUris   list of type URIs to format
     * @param typeLabels map of type URI -&gt; label
     * @return map of URI -&gt; formatted name
     */
    public Map<String, String> getTypeNames(Collection<String> typeUris, Map<String, String> typeLabels) {
        Integer maxLength = config.getMaxTableNameLength();
        return formatNames(typeUris, maxLength, config.isUseLabels() ? typeLabels : null, config.getReservedNames());
    }

    /**
     * Get map of property names for specified URIs, formatted from String labels
     *
     * @param predicateUris   list of predicate URIs to format
     * @param predicateLabels map of predicate URI -&gt; label
     * @return map of URI -&gt; formatted name
     */
    public Map<String, String> getPropertyNames(Collection<String> predicateUris, Map<String, String> predicateLabels) {
        Integer maxLength = config.getMaxColumnNameLength();
        return formatNames(predicateUris, maxLength, config.isUseLabels() ? predicateLabels : null, config.getReservedNames());
    }

    /**
     * Get map of relation names for specified URIs, formatted from String labels
     *
     * @param predicateUris           list of predicate URIs to format
     * @param predicateLabels         map of predicate URI -&gt; label
     * @param additionalReservedNames set of names to avoid
     * @return map of URI -&gt; formatted name
     */
    public Map<String, String> getRelationNames(Collection<String> predicateUris, Map<String, String> predicateLabels, Set<String> additionalReservedNames) {
        Integer maxLength = config.getMaxTableNameLength();
        Set<String> reservedNames = new HashSet<>(config.getReservedNames());
        reservedNames.addAll(additionalReservedNames);
        return formatNames(predicateUris, maxLength, config.isUseLabels() ? predicateLabels : null, reservedNames);
    }

    /**
     * Get map of unique names for specified URIs, formatted from String labels or URIs.
     * Name uniqueness is ensured by adding numeric suffixes (in alphabetical order of URIs).
     *
     * @param uris          collection of URI Strings to find labels for
     * @param maxLength     maximum length of formatted names
     * @param labels        map of URI -&gt; label or null if labels should not be used
     * @param reservedNames set of names to avoid
     * @return map of URI -&gt; formatted name
     */
    private Map<String, String> formatNames(Collection<String> uris, Integer maxLength, Map<String, String> labels, Set<String> reservedNames) {
        final String uriSuffixPattern = config.getUriSuffixPattern();
        final Flavor flavor = config.getFlavor();
        // group URIs by equal suffixes
        // each sublist is sorted alphabetically by URI
        // e.g. [[(first/uri/name,name), (second/uri/name,name)], [(another/column, column)]]
        List<List<Tuple2<String, String>>> groupedNames = uris.stream()
                .map(uri -> {
                    String name = null;
                    // format using custom flavor function
                    if (flavor != null) {
                        name = flavor.formatName(uri, labels == null ? null : labels.get(uri), maxLength);
                    }
                    // format using label
                    if (name == null && (labels != null && labels.containsKey(uri))) {
                        name = FormatUtil.getCleanName(labels.get(uri), maxLength);
                    }
                    // if label was not available, format using URI suffix
                    if (name == null || name.isEmpty()) {
                        name = FormatUtil.getCleanURISuffix(uri, uriSuffixPattern, maxLength);
                    }
                    return new Tuple2<>(uri, name);
                })
                .collect(Collectors.groupingBy(
                        Tuple2::_2,
                        Collectors.mapping(
                                Function.identity(),
                                Collectors.toList()
                        )
                )).values().stream()
                // sort each sublist by URI to ensure suffixes are added on names in alphabetical URI order
                .map(list -> list.stream().sorted((t1, t2) -> t1._1().compareTo(t2._1())).collect(Collectors.toList()))
                .collect(Collectors.toList());

        return FormatUtil.formatUniqueNames(groupedNames, reservedNames, maxLength);
    }

}
