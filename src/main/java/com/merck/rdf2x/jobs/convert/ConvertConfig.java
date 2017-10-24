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

package com.merck.rdf2x.jobs.convert;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import com.merck.rdf2x.config.StorageLevelConverter;
import com.merck.rdf2x.persistence.InstanceRelationWriterConfig;
import com.merck.rdf2x.persistence.config.OutputConfig;
import com.merck.rdf2x.processing.aggregating.InstanceAggregatorConfig;
import com.merck.rdf2x.processing.filtering.InstanceFilterConfig;
import com.merck.rdf2x.processing.filtering.QuadFilterConfig;
import com.merck.rdf2x.processing.formatting.SchemaFormatterConfig;
import com.merck.rdf2x.processing.partitioning.InstancePartitionerConfig;
import com.merck.rdf2x.processing.schema.EntitySchemaCollectorConfig;
import com.merck.rdf2x.processing.schema.RelationConfig;
import com.merck.rdf2x.rdf.parsing.QuadParserConfig;
import com.merck.rdf2x.rdf.schema.RdfSchemaCollectorConfig;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;
import org.apache.spark.storage.StorageLevel;

import javax.naming.ConfigurationException;

/**
 * ConvertConfig stores all parameters used for a {@link ConvertJob}.
 * It is annotated by jCommander Parameters used to load values from the command line.
 * <p>
 */
@Data
@Accessors(chain = true)
@NoArgsConstructor
@AllArgsConstructor
@Parameters(commandDescription = "Convert RDF to the Entity-Relationship format and write it in a file or database")
public class ConvertConfig {

    @Parameter(names = "--input.file", description = "Path to input file or folder", required = true)
    private String inputFile;

    @Parameter(names = "--flavor", description = "Specify a flavor to be used. Flavors modify the behavior of RDF2X, applying default settings and providing custom methods for data source specific modifications.", help = true)
    private String flavor = null;

    @Parameter(names = "--filter.cacheLevel", converter = StorageLevelConverter.class, description = "Level of caching of the input dataset after filtering (None, DISK_ONLY, MEMORY_ONLY, MEMORY_AND_DISK, ...). See Spark StorageLevel class for more info.")
    private StorageLevel filterCacheLevel = StorageLevel.NONE();

    @Parameter(names = "--cacheLevel", converter = StorageLevelConverter.class, description = "Level of caching of the instances before collecting schema and persisting (None, DISK_ONLY, MEMORY_ONLY, MEMORY_AND_DISK, ...). See Spark StorageLevel class for more info.")
    private StorageLevel cacheLevel = StorageLevel.DISK_ONLY();

    @Parameter(names = "--instancePartitions", description = "Repartition before aggregating instances into this number of partitions.")
    private Integer instancePartitions = null;

    @Parameter(names = "--help", description = "Show usage page", help = true)
    private boolean help = false;

    @ParametersDelegate()
    private QuadParserConfig parserConfig = new QuadParserConfig();

    @ParametersDelegate()
    private QuadFilterConfig quadFilterConfig = new QuadFilterConfig();

    @ParametersDelegate()
    private RdfSchemaCollectorConfig rdfSchemaCollectorConfig = new RdfSchemaCollectorConfig();

    @ParametersDelegate()
    private InstanceFilterConfig instanceFilterConfig = new InstanceFilterConfig();

    @ParametersDelegate()
    private InstancePartitionerConfig instancePartitionerConfig = new InstancePartitionerConfig();

    @ParametersDelegate()
    private OutputConfig outputConfig = new OutputConfig();

    @ParametersDelegate()
    private InstanceAggregatorConfig aggregatorConfig = new InstanceAggregatorConfig();

    @ParametersDelegate()
    private InstanceRelationWriterConfig writerConfig = new InstanceRelationWriterConfig();

    @ParametersDelegate()
    private EntitySchemaCollectorConfig entitySchemaCollectorConfig = new EntitySchemaCollectorConfig();

    @ParametersDelegate()
    private RelationConfig relationConfig = new RelationConfig();

    @ParametersDelegate()
    private SchemaFormatterConfig schemaFormatterConfig = new SchemaFormatterConfig();


    /**
     * Validate the config, throw {@link ConfigurationException} on error
     *
     * @throws ConfigurationException found error
     */
    public void validate() throws ConfigurationException {
        outputConfig.validate();
        quadFilterConfig.validate();
    }

}
