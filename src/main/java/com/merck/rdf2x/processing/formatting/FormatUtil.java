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

import com.merck.rdf2x.rdf.LiteralType;
import org.apache.commons.lang3.StringUtils;
import scala.Tuple2;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * FormatUtil provides methods for name formatting
 */
public class FormatUtil implements Serializable {

    /**
     * String to use for joining entity table names into relation table names
     */
    static final String RELATION_TABLE_JOINER = "_";

    /**
     * String to prepend to names starting with numbers
     */
    static final String NUMBER_PREFIX = "N";

    /**
     * Regex of characters not allowed in column names
     */
    static final String CHARS_NOT_ALLOWED = "[^A-Za-z0-9_]";

    /**
     * String to use between parts of a name
     */
    static final String NAME_JOINER = "_";

    /**
     * Remove all unwanted characters from name and replace them with underscores
     *
     * @param name the string to clean, for example '-a--name-'
     * @return name with groups of unwanted characters replaced with underscores, for example 'a_name'
     */
    public static String removeSpecialChars(String name) {
        name = StringUtils.strip(name.replaceAll(CHARS_NOT_ALLOWED + "+", "_"), "_").toLowerCase();

        // prepend letter if name starts with a number
        if (name.matches("^[0-9].*")) {
            name = NUMBER_PREFIX + name;
        }
        return name;
    }

    /**
     * Add language suffix to a column name
     *
     * @param name     column name without suffix, for example 'weight'
     * @param language language of the value, null if not specified
     * @return the suffixed column name, for example 'weight_en'
     */
    public static String addLanguageSuffix(String name, String language) {
        if (language != null && !language.isEmpty()) {
            name = name + NAME_JOINER + language;
        }
        return name;
    }

    /**
     * Add type suffix to a column name
     *
     * @param name        column name without suffix, for example 'weight'
     * @param literalType column type, for example LiteralType.DOUBLE
     * @return the suffixed column name, for example 'weight_double'
     */
    public static String addTypeSuffix(String name, int literalType) {
        return name + NAME_JOINER + LiteralType.toString(literalType).toLowerCase();
    }

    /**
     * Add a suffix based on retryIndex. If retrySuffix is <= 1, no suffix is added.
     *
     * @param name       the name to suffix
     * @param maxLength  maximum length of the resulting name
     * @param retryIndex the requested retry index to append
     * @return the name with added suffix
     */
    private static String addRetrySuffix(String name, Integer maxLength, Integer retryIndex) {
        String suffix = retryIndex <= 1 ? "" : NAME_JOINER + retryIndex;
        if (maxLength != null && name.length() + suffix.length() > maxLength) {
            name = name.substring(0, maxLength - suffix.length());
        }
        return name + suffix;
    }

    /**
     * Format groups of identical names into groups of distinct names
     * Name uniqueness is ensured by adding numeric suffixes (in alphabetical order of URIs).
     * First non-unique name has no suffix, second has _2 suffix, third has _3 suffix, etc.
     *
     * @param groupedNames  list of groups of identical names, e.g. [[(first/uri/name,name), (second/uri/name,name)], [(another/column, column)]]
     * @param reservedNames set of additional names to avoid, case insensitive
     * @param maxLength     maximum result length of each name
     * @return map of uri -&gt; unique name, e.g. [first/uri/name -> name, second/uri/name -> name_2, another/column -> column]
     */
    static Map<String, String> formatUniqueNames(Collection<List<Tuple2<String, String>>> groupedNames, Set<String> reservedNames, final Integer maxLength) {
        final Set<String> lowerCaseReservedNames = reservedNames.stream().map(String::toLowerCase).collect(Collectors.toSet());
        return groupedNames.stream()
                .flatMap(equalNames -> {
                            final String name = equalNames.get(0)._2();
                            int suffixOffset = lowerCaseReservedNames.contains(name.toLowerCase()) ? 2 : 1;
                            return IntStream
                                    .range(0, equalNames.size())
                                    .mapToObj(i -> new Tuple2<>(
                                            equalNames.get(i)._1(), // uri
                                            FormatUtil.addRetrySuffix(name, maxLength, i + suffixOffset))
                                    );
                        }
                ).collect(Collectors.toMap(
                        Tuple2::_1,
                        Tuple2::_2,
                        (a, b) -> a
                ));
    }

    /**
     * Format name of relation table using names of the two entity tables
     *
     * @param fromName name of instance entity table, for example 'Fruit'
     * @param toName   name of object entity table, for example 'Flavor'
     * @return the formatted relation table name, for example 'Fruit_Flavor'
     */
    public static String getRelationTableName(String fromName, String toName) {
        return fromName + RELATION_TABLE_JOINER + toName;
    }

    /**
     * Format name of relation table using names of the two entity tables and a relation predicate
     *
     * @param fromName      name of instance entity table, for example 'Fruit'
     * @param toName        name of object entity table, for example 'Flavor'
     * @param predicateName name of relation predicate, for example 'HasFlavor'
     * @return the formatted relation table name, for example 'Fruit_HasFlavor_Flavor'
     */
    public static String getRelationTableName(String fromName, String toName, String predicateName) {
        return fromName + RELATION_TABLE_JOINER + predicateName + RELATION_TABLE_JOINER + toName;
    }

    /**
     * Get shortened and cleaned name from an arbitrary string
     *
     * @param str       string clean and shorten
     * @param maxLength maximum length of the resulting name
     * @return shortened and cleaned name
     */
    public static String getCleanName(String str, Integer maxLength) {
        return StringUtils.substring(FormatUtil.removeSpecialChars(str), 0, maxLength);
    }

    /**
     * Get the last non-empty segment of a URI with allowed characters only. If no allowed chars are present, return the UNKNOWN_NAME field value.
     *
     * @param uri              the used URI
     * @param uriSuffixPattern use the segment after the last occurrence of this regex
     * @param maxLength        maximum length of resulting name
     * @return the last non-empty segment of the URI with allowed characters only
     */
    public static String getCleanURISuffix(String uri, String uriSuffixPattern, Integer maxLength) {
        // try to urldecode the URI
        try {
            uri = URLDecoder.decode(uri, "utf-8");
        } catch (UnsupportedEncodingException | IllegalArgumentException ignored) {
        }

        // trim all unwanted characters from the end of the URI
        String name = uri.replaceFirst("(" + uriSuffixPattern + "|" + FormatUtil.CHARS_NOT_ALLOWED + ")+$", "");

        if (name.isEmpty()) {
            return "undefined";
        }

        // find last occurrence of uriSuffixPattern
        Matcher matcher = Pattern.compile(uriSuffixPattern).matcher(name);
        int lastIndex = 0;
        while (matcher.find()) {
            lastIndex = matcher.end();
        }

        // get last suffix of uri
        name = StringUtils.substring(name, lastIndex);

        return getCleanName(name, maxLength);
    }

}
