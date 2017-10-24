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

package com.merck.rdf2x.processing.aggregating;

import com.merck.rdf2x.beans.IndexMap;
import com.merck.rdf2x.beans.Instance;
import com.merck.rdf2x.beans.Predicate;
import com.merck.rdf2x.beans.RelationPredicate;
import com.merck.rdf2x.rdf.LiteralType;
import com.merck.rdf2x.rdf.schema.ClassGraph;
import com.merck.rdf2x.rdf.schema.RdfSchema;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.jena.datatypes.DatatypeFormatException;
import org.apache.jena.datatypes.xsd.XSDDateTime;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.vocabulary.RDFS;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.broadcast.Broadcast;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import scala.Tuple2;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.*;

/**
 * InstanceAggregator aggregates a RDD of {@link Quad}s to a RDD of {@link Instance}s. Quads with errors are stored as Instances of type ERROR.
 */
@Slf4j
@RequiredArgsConstructor
public class InstanceAggregator implements Serializable {

    public static final String IRI_BASE = "#rdf2x:";
    public static final String IRI_TYPE_DEFAULT = RDFS.Resource.toString();
    public static final String IRI_TYPE_ERROR = IRI_BASE + "error";
    public static final String IRI_PROPERTY_ERROR_SUBJECT = IRI_BASE + "subject";
    public static final String IRI_PROPERTY_ERROR_QUAD = IRI_BASE + "quad";
    public static final String IRI_PROPERTY_ERROR_MESSAGE = IRI_BASE + "message";

    /**
     * config storing aggregation parameters
     */
    private final InstanceAggregatorConfig config;
    /**
     * schema storing information about classes and properties
     */
    private final Broadcast<RdfSchema> schema;

    /**
     * Aggregate RDD of quads into Instances (maps of {@link Predicate} -&gt; value). Each {@link Instance} represents all {@link Quad}s with the same instance URI.
     *
     * @param quads RDD of {@link Quad}s
     * @return RDD of aggregated {@link Instance}s
     */
    public JavaRDD<Instance> aggregateInstances(JavaRDD<Quad> quads) {
        // aggregate quads into Instances which represent all quads with the same instance URI
        JavaRDD<Instance> instances = quads
                .flatMapToPair(this::mapQuadToInstance)
                .filter(instance -> instance != null)
                .reduceByKey(this::mergeInstances).values();

        instances = addDefaultInstanceTypes(instances);

        return instances;
    }

    /**
     * Get IRIs of special types used in aggregation (e.g. type of error message instances)
     *
     * @return list of special type IRIs
     */
    public static List<String> getSpecialTypes() {
        return Arrays.asList(IRI_TYPE_ERROR, IRI_TYPE_DEFAULT);
    }

    /**
     * Get IRIs of special predicates used in aggregation (e.g. type of error message columns)
     *
     * @return list of special predicate IRIs
     */
    public static List<String> getSpecialPredicates() {
        return Arrays.asList(IRI_PROPERTY_ERROR_SUBJECT, IRI_PROPERTY_ERROR_QUAD, IRI_PROPERTY_ERROR_MESSAGE);
    }

    /**
     * Add a default type to instances with no type
     *
     * @param instances instances to add type to
     * @return instances with default type added where no type was specified
     */
    private JavaRDD<Instance> addDefaultInstanceTypes(JavaRDD<Instance> instances) {
        final int defaultType = schema.value().getTypeIndex().getIndex(IRI_TYPE_DEFAULT);
        return instances.map(instance -> {
            if (!instance.hasType()) {
                instance.addType(defaultType);
            }
            return instance;
        });
    }


    /**
     * Map a quad to a Instance URI and a new Instance containing the {@link Quad}'s content (type, relation or literal value).
     *
     * @param quad a RDF quad
     * @return a pair of Instance URI and a new Instance containing the {@link Quad}'s content (type, relation or literal value)
     */
    private Iterable<Tuple2<String, Instance>> mapQuadToInstance(Quad quad) {
        Instance instance = new Instance();
        Node object = quad.getObject();
        final boolean addSuperTypes = config.isAddSuperTypes();
        final RdfSchema localSchema = schema.value();
        final List<String> typePredicateURIs = localSchema.getConfig().getTypePredicates();
        final IndexMap<String> typeIndexMap = localSchema.getTypeIndex();
        final IndexMap<String> predicateIndexMap = localSchema.getPredicateIndex();
        ClassGraph classGraph = localSchema.getClassGraph();
        try {
            if (!quad.getSubject().isURI()) {
                throw new DatatypeFormatException("Subject has to be a URI, encountered: " + quad.getSubject());
            }
            String predicateURI = quad.getPredicate().getURI();
            String subjectURI = quad.getSubject().getURI();
            instance.setUri(subjectURI);
            // save quad object into instance as a type, property or relation
            if (predicateURI.equals(RDF.TYPE.toString()) || typePredicateURIs.contains(predicateURI)) {
                // quad represents a resource's type
                if (!object.isURI()) {
                    throw new DatatypeFormatException("Object of RDF Type predicate has to be a URI, encountered: " + object.getClass().getName());
                }
                Integer typeIndex = typeIndexMap.getIndex(object.getURI());
                instance.addType(typeIndex);
                if (addSuperTypes) {
                    Set<Integer> ancestors = classGraph.getSuperClasses(typeIndex);
                    instance.addTypes(ancestors);
                }
            } else if (object.isURI()) {

                // create blank related instance
                String relatedURI = object.getURI();
                Instance relatedInstance = new Instance();
                relatedInstance.setUri(relatedURI);

                // quad represents a relation to another resource
                Integer predicateIndex = predicateIndexMap.getIndex(predicateURI);
                instance.setRelation(new RelationPredicate(predicateIndex, relatedURI));

                return Arrays.asList(new Tuple2<>(subjectURI, instance), new Tuple2<>(relatedURI, relatedInstance));
            } else if (object.isBlank()) {
                // quad represents a relation to a blank node
                // TODO blank nodes
                // throw exception which will be caught and stored in Error table
                throw new DatatypeFormatException("Blank nodes not supported yet");
            } else if (object.isLiteral()) {
                // quad represents a literal value
                // get literal value and its type
                Object objectValue = sanitizeValue(object.getLiteralValue());
                String objectLanguage = object.getLiteralLanguage();

                if (objectLanguage == null || objectLanguage.isEmpty() || objectLanguage.equals(config.getDefaultLanguage())) {
                    // set empty string languages to null
                    objectLanguage = null;
                }
                int objectType = getLiteralTypeFromValue(objectValue);
                objectValue = convertToSupportedValue(objectValue);
                // literals with unrecognized types are handled as Strings
                if (objectType == LiteralType.UNKNOWN) {
                    objectValue = objectValue.toString();
                    objectType = LiteralType.STRING;
                }

                // add literal value to Instance
                instance.putLiteralValue(new Predicate(predicateIndexMap.getIndex(predicateURI), objectType, objectLanguage), objectValue);
            }
            return Collections.singleton(new Tuple2<>(subjectURI, instance));
        } catch (DatatypeFormatException e) {
            log.warn("Error parsing quad {}: {}", quad, e.getMessage());

            String quadString = quad.toString();
            String subjectURI = quad.getSubject().isURI() ? quad.getSubject().getURI() : quad.getSubject().toString();
            String errorURI = getErrorURI(quadString);

            instance.setUri(errorURI);
            instance.setType(typeIndexMap.getIndex(IRI_TYPE_ERROR));
            instance.putLiteralValue(new Predicate(predicateIndexMap.getIndex(IRI_PROPERTY_ERROR_SUBJECT), LiteralType.STRING), subjectURI);
            instance.putLiteralValue(new Predicate(predicateIndexMap.getIndex(IRI_PROPERTY_ERROR_QUAD), LiteralType.STRING), quadString);
            instance.putLiteralValue(new Predicate(predicateIndexMap.getIndex(IRI_PROPERTY_ERROR_MESSAGE), LiteralType.STRING), e.getMessage());
            return Collections.singleton(new Tuple2<>(errorURI, instance));
        }
    }

    private static String getErrorURI(String quadString) {
        try {
            return IRI_TYPE_ERROR + ":" + URLEncoder.encode(quadString, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            return IRI_TYPE_ERROR + ":unsupported";
        }
    }

    private Object sanitizeValue(Object objectValue) {
        if (objectValue instanceof String) {
            return ((String) objectValue).replaceAll("\0", "");
        }

        return objectValue;
    }

    /**
     * Merge two {@link Instance}s into one that contains all of their properties.
     * Conflicts (two identical {@link Predicate}s of the same URI and literal type) are merged into a set of both values.
     *
     * @param a the first {@link Instance}
     * @param b the second {@link Instance}
     * @return the merged {@link Instance} containing properties of both, multiple literal values merged into sets
     */
    private Instance mergeInstances(Instance a, Instance b) {
        Set<Predicate> predicatesOfA = a.getLiteralPredicates();
        for (Predicate key : b.getLiteralPredicates()) {
            Object value = b.getLiteralValue(key);
            if (predicatesOfA.contains(key)) {
                a.putLiteralValue(key, unionValues(a.getLiteralValue(key), value));
            } else {
                a.putLiteralValue(key, value);
            }
        }
        a.addTypes(b.getTypes());
        a.addRelations(b.getRelations());
        if (a.getUri() == null) {
            a.setUri(b.getUri());
        }
        return a;
    }

    /**
     * Union two values into a single set. If two single equal values are merged, one value is returned.
     *
     * @param a a single value (String, Integer, ...) or a set of these. If a Set is provided, add all values from b and return it.
     * @param b a single value (String, Integer, ...) or a set of these
     * @return the set containing union of all values from a and b
     */
    private Object unionValues(Object a, Object b) {
        Set result;
        if (a instanceof Set && b instanceof Set) {
            ((Set) a).addAll((Set) b);
            result = (Set) a;
        } else if (a instanceof Set) {
            ((Set) a).add(b);
            result = (Set) a;
        } else if (b instanceof Set) {
            ((Set) b).add(a);
            result = (Set) b;
        } else {
            if (a.equals(b)) {
                return a;
            }
            result = new HashSet(2);
            result.add(a);
            result.add(b);
        }
        return result;
    }


    private Object convertToSupportedValue(Object value) {
        if (value instanceof XSDDateTime) {
            // spark only accepts java.sql.Timestamp and assumes it uses the current JVM timezone
            // therefore this is not correct
            // return new Timestamp(((XSDDateTime)value).asCalendar().getTimeInMillis());
            // store the datetime as string instead
            return value.toString();
        }
        return value;
    }

    /**
     * Get literal type based on class of the value instance.
     *
     * @param value the requested value
     * @return the value's literal type, null if not recognized
     */
    private int getLiteralTypeFromValue(Object value) {
        if (value instanceof Integer) {
            return LiteralType.INTEGER;
        } else if (value instanceof Double) {
            return LiteralType.DOUBLE;
        } else if (value instanceof Float) {
            return LiteralType.FLOAT;
        } else if (value instanceof String) {
            return LiteralType.STRING;
        } else if (value instanceof Long) {
            return LiteralType.LONG;
        } else if (value instanceof Boolean) {
            return LiteralType.BOOLEAN;
        } else if (value instanceof XSDDateTime) {
            return LiteralType.DATETIME;
        }
        return 0;
    }
}
