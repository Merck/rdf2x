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

package com.merck.rdf2x.rdf.schema;

import org.jgrapht.experimental.dag.DirectedAcyclicGraph;
import org.jgrapht.graph.DefaultEdge;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * ClassGraph wraps a JGraphT graph to provide nicer methods
 */
public class ClassGraph implements Serializable {

    /**
     * graph of edges from superclass to subclass
     */
    private final DirectedAcyclicGraph<Integer, DefaultEdge> graph;

    /**
     * Create new empty graph
     */
    public ClassGraph() {
        this.graph = new DirectedAcyclicGraph<>(DefaultEdge.class);
    }

    /**
     * @param graph graph of edges from superclass to subclass
     */
    public ClassGraph(DirectedAcyclicGraph<Integer, DefaultEdge> graph) {
        this.graph = graph;
    }

    /**
     * Get all types in topological order - every superclass precedes a subclass.
     *
     * @return types in topological order - every superclass precedes a subclass
     */
    public Iterable<Integer> inSuperClassFirstOrder() {
        // return empty list if no vertices are added to avoid JGraphT exception
        return graph.vertexSet().isEmpty() ? Collections.emptyList() : graph;
    }

    /**
     * Get set of edges (from superclass to subclass)
     * @return set of edges (from superclass to subclass)
     */
    public Set<DefaultEdge> edgeSet() {
        return graph.edgeSet();
    }

    /**
     * Get superclasses (type indexes) of given class (type index)
     * @param typeIndex type index to find superclasses for
     * @return superclasses (type indexes) of given class (type index)
     */
    public Set<Integer> getSuperClasses(Integer typeIndex) {
        return graph.vertexSet().isEmpty() ? Collections.emptySet() : graph.getAncestors(graph, typeIndex);
    }

    /**
     * Return whether the class (type index) has any superclasses
     * @param typeIndex type index to find superclasses for
     * @return true if the class (type index) has any superclasses, false otherwise
     */
    public boolean hasSuperClasses(Integer typeIndex) {
        return !graph.edgeSet().isEmpty() && !graph.incomingEdgesOf(typeIndex).isEmpty();
    }

    /**
     * Return whether the class (type index) has any subclasses
     * @param typeIndex type index to find superclasses for
     * @return true if the class (type index) has any subclasses, false otherwise
     */
    public boolean hasSubClasses(Integer typeIndex) {
        return !graph.edgeSet().isEmpty() && !graph.outgoingEdgesOf(typeIndex).isEmpty();
    }

    /**
     * Get superclass from edge (object of subClassOf statement)
     * @param edge edge to get superclass from
     * @return superclass of the given edge (object of subClassOf statement)
     */
    public Integer getEdgeSuperclass(DefaultEdge edge) {
        return graph.getEdgeSource(edge);
    }

    /**
     * Get subclass from edge (subject of subClassOf statement)
     * @param edge edge to get subclass from
     * @return subclass of the given edge (subject of subClassOf statement)
     */
    public Integer getEdgeSubclass(DefaultEdge edge) {
        return graph.getEdgeTarget(edge);
    }

    /**
     * Get all classes (type indexes)
     * @return set of all classes (type indexes)
     */
    public Set<Integer> getClasses() {
        return graph.vertexSet();
    }

    /**
     * Remove all vertices that are not in typeIndexes set
     *
     * @param typeIndexes set of vertices (type indexes) to preserve
     */
    public void keepOnlyClasses(Set<Integer> typeIndexes) {
        Set<Integer> toRemove = new HashSet<>(graph.vertexSet());
        toRemove.removeAll(typeIndexes);
        graph.removeAllVertices(toRemove);
    }
}
