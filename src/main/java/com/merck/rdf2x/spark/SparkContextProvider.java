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

package com.merck.rdf2x.spark;

import com.merck.rdf2x.beans.*;
import com.merck.rdf2x.rdf.LiteralType;
import lombok.extern.slf4j.Slf4j;
import org.apache.jena.sparql.core.Quad;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;

import java.util.HashMap;
import java.util.HashSet;

/**
 * SparkContextProvider provides a {@link JavaSparkContext} based on default settings.
 */
@Slf4j
public class SparkContextProvider {
    /**
     * Provide a {@link JavaSparkContext} based on default settings
     *
     * @return a {@link JavaSparkContext} based on default settings
     */
    public static JavaSparkContext provide() {
        SparkConf config = new SparkConf()
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .registerKryoClasses(getSerializableClasses());

        if (!config.contains("spark.app.name")) {
            config.setAppName("RDF2X");
        }
        if (!config.contains("spark.master")) {
            config.setMaster("local");
        }

        // set serialization registration required if you want to make sure you registered all your classes
        // some spark internal classes will need to be registered as well
        // config.set("spark.kryo.registrationRequired", "true");


        log.info("Getting Spark Context for config: \n{}", config.toDebugString());
        return new JavaSparkContext(config);
    }

    public static Class[] getSerializableClasses() {
        return new Class[]{
                Instance.class, Predicate.class, RelationPredicate.class, RelationRow.class,
                TypeID.class, HashMap.class, HashSet.class, LiteralType.class, Object[].class,
                InternalRow[].class, GenericInternalRow.class, IndexMap.class, Quad.class
        };
    }

}
