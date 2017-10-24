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

import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.util.*;
import java.util.function.BiFunction;

import static org.junit.Assert.*;

@Slf4j
@SuppressWarnings("ConstantConditions")
public class SchemaFormatterTest {

    /**
     * Test formatting individual table names
     */
    @Test
    public void testFormatEntityNames() {
        SchemaFormatterConfig config = new SchemaFormatterConfig()
                .setMaxTableNameLength(20)
                .setReservedNames(new HashSet<>(Collections.singletonList("reserved_name")));

        SchemaFormatter formatter = new SchemaFormatter(config);

        testFormatNames(formatter::getTypeNames, config.getMaxTableNameLength());
    }


    /**
     * Test formatting individual table names
     */
    @Test
    public void testFormatColumnNames() {
        SchemaFormatterConfig config = new SchemaFormatterConfig()
                .setMaxColumnNameLength(25)
                .setReservedNames(new HashSet<>(Collections.singletonList("reserved_name")));

        SchemaFormatter formatter = new SchemaFormatter(config);

        testFormatNames(formatter::getPropertyNames, config.getMaxColumnNameLength());
    }

    /**
     * Test formatting individual table names
     */
    @Test
    public void testFormatRelationNames() {
        SchemaFormatterConfig config = new SchemaFormatterConfig()
                .setMaxTableNameLength(20)
                .setReservedNames(new HashSet<>(Collections.singletonList("reserved_name")));

        SchemaFormatter formatter = new SchemaFormatter(config);

        testFormatNames((uris, labels) -> formatter.getRelationNames(uris, labels, new HashSet<>()), config.getMaxTableNameLength());
    }

    public void testFormatNames(BiFunction<Collection<String>, Map<String, String>, Map<String, String>> format, int maxLength) {

        List<String> nameList = Arrays.asList(
                "http://example.com/d/folder#name",
                "http://example.com/c/name#",
                "http://example.com/b/--name--",
                "http://example.com/a/name/+*,-%@",
                "name",
                "http://example.com/second"
        );


        Map<String, String> names = format.apply(nameList, new HashMap<>());
        assertTrue("All names are unique", names.values().size() == new HashSet<>(names.values()).size());
        assertTrue("Alphabetically first URI has no suffix", names.get("http://example.com/a/name/+*,-%@").equals("name"));
        assertTrue("Alphabetically second URI has _2 suffix", names.get("http://example.com/b/--name--").equals("name_2"));
        assertTrue("Alphabetically third URI has _3 suffix", names.get("http://example.com/c/name#").equals("name_3"));
        assertTrue("Alphabetically fourth URI has _4 suffix", names.get("http://example.com/d/folder#name").equals("name_4"));
        assertTrue("No suffix is added for already unique name", names.get("http://example.com/second").equals("second"));

        assertNotEquals("Formatted name of empty URI is not empty", "", formatSingleName(format, ""));

        assertNotEquals("Reserved name is not used.", "reserved_name", formatSingleName(format, "http://example.com/reserved_name"));

        assertEquals("two_words", formatSingleName(format, "http://example.com/page#two-words"));
        assertEquals("two_words", formatSingleName(format, "http://example.com/page#two---words"));
        assertEquals("two_words", formatSingleName(format, "http://example.com/page#--two---words--"));

        assertEquals("an_encoded_name", formatSingleName(format, "http://example.com/table?name=an%20encoded%20name"));

        assertEquals("Long name has max length", maxLength, formatSingleName(format, "http://example.com/a_very_very_long_uri_suffix_name").length());

    }

    private String formatSingleName(BiFunction<Collection<String>, Map<String, String>, Map<String, String>> format, String uri) {
        return format.apply(Collections.singletonList(uri), new HashMap<>()).get(uri);
    }

}










