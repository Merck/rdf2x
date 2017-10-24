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

import lombok.Data;
import org.apache.jena.sparql.core.Quad;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

/**
 * Instance stores all properties of a RDF resource. Or in other words, a {@link Instance} represents content from all {@link Quad}s with the same instance URI.
 * <p>
 * A Instance is defined by:
 * <ul>
 * <li>URI of the resource</li>
 * <li>ID (optional)</li>
 * <li>set of type URIs</li>
 * <li>set of {@link RelationPredicate} defining the Instance's relations</li>
 * <li>map of literal values ({@link Predicate} -&gt; valueObject). The valueObject can be either a single value (String, Integer, ...) or a set of these</li>
 * </ul>
 */
@Data
public class Instance implements Serializable {

    /**
     * URI of the resource. If instance is a blank node, its identifier is used instead.
     */
    private String uri;
    /**
     * ID (optional)
     */
    private Long id;
    /**
     * set of type URIs
     */
    private final Set<Integer> types;
    /**
     * set of {@link RelationPredicate} defining the Instance's relations
     */
    private final Set<RelationPredicate> relations;
    /**
     * map of literal values ({@link Predicate} -&gt; Object)
     */
    private final HashMap<Predicate, Object> literalValues;

    /**
     * Default constructor. Start with empty types, relations and literal values.
     */
    public Instance() {
        literalValues = new HashMap<>();
        types = new HashSet<>();
        relations = new HashSet<>();
    }

    public void setType(Integer type) {
        this.types.clear();
        addType(type);
    }

    public void addType(Integer type) {
        this.types.add(type);
    }

    public void addTypes(Collection<Integer> types) {
        this.types.addAll(types);
    }

    public Set<RelationPredicate> getRelations() {
        return relations;
    }

    public void setRelation(RelationPredicate relation) {
        this.relations.clear();
        addRelation(relation);
    }

    public void addRelation(RelationPredicate relation) {
        this.relations.add(relation);
    }

    public void addRelations(Collection<RelationPredicate> relations) {
        this.relations.addAll(relations);
    }

    public void putLiteralValue(Predicate predicate, Object value) {
        literalValues.put(predicate, value);
    }

    public Set<Predicate> getLiteralPredicates() {
        return literalValues.keySet();
    }

    public Object getLiteralValue(Predicate key) {
        return literalValues.get(key);
    }

    public boolean hasType() {
        return !types.isEmpty();
    }
}
