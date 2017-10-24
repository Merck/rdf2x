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

package com.merck.rdf2x.beans;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * IndexMap defines classes that represent values of any type as integers, starting from 0.
 *
 * @param <V> The value to be represented as integer
 */
public class IndexMap<V> implements Serializable {
    /**
     * value array - mapping indexes to values, starting from 0
     */
    private final ArrayList<V> values;
    /**
     * value map - mapping values to indexes, starting from 0
     */
    private Map<V, Integer> indexMap;

    /**
     * @param values list of values to represent, will be used directly
     */
    public IndexMap(ArrayList<V> values) {
        this.values = values;
        this.indexMap = createValueMap(values);
    }

    /**
     * @param values list of values to represent, deep copy will be created
     */
    public IndexMap(List<V> values) {
        this(new ArrayList<>(values));
    }

    private Map<V, Integer> createValueMap(Collection<V> values) {
        AtomicInteger index = new AtomicInteger();
        return values.stream()
                .collect(Collectors.toMap(Function.identity(), v -> index.getAndIncrement()));
    }


    /**
     * Get integer representation of a value
     *
     * @param value the value to be represented
     * @return integer representation (index) of the value
     */
    public Integer getIndex(V value) {
        if (!indexMap.containsKey(value)) {
            // alert when element is not present right away to avoid null pointer errors
            throw new NullPointerException("Map does not contain value, were all values properly initialized?");
        }
        return indexMap.get(value);
    }

    /**
     * Get value for a specific index
     *
     * @param index value index (the integer representation of the value)
     * @return value for the specified index
     */
    public V getValue(int index) {
        return values.get(index);
    }

    /**
     * Get size of the map
     *
     * @return size of the map
     */
    public int size() {
        return values.size();
    }

    /**
     * Get list of all represented values
     *
     * @return set of all represented values
     */
    public ArrayList<V> getValues() {
        return values;
    }

    /**
     * Get set of all represented values
     *
     * @return set of all represented values
     */
    public Set<V> getValueSet() {
        return indexMap.keySet();
    }

    /**
     * Get collection of all used indexes
     *
     * @return collection of all used indexes
     */
    public Collection<Integer> getIndex() {
        return indexMap.values();
    }
}
