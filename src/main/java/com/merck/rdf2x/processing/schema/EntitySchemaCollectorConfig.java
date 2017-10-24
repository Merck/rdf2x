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

import com.beust.jcommander.Parameter;
import lombok.Data;
import lombok.experimental.Accessors;

import java.io.Serializable;

/**
 * EntitySchemaCollectorConfig stores instructions for the {@link EntitySchemaCollector}.
 */
@Data
@Accessors(chain = true)
public class EntitySchemaCollectorConfig implements Serializable {

    @Parameter(names = "--entities.maxNumColumns", description = "Maximum number of columns for one table.")
    private Integer maxNumColumns = null;

    @Parameter(names = "--entities.minColumnNonNullFraction", description = "Properties require at least minColumnNonNullFraction non-null values to be stored as columns. The rest is stored in the Entity-Attribute-Value table (e.g. 0.4 = properties with less than 40% values present will be stored only in the EAV table, 0 = store all as columns, 1 = store all only in EAV table).")
    private Double minColumnNonNullFraction = 0.0;

    @Parameter(names = "--entities.redundantEAV", arity = 1, description = "Store all properties in the EAV table, including values that are already stored in columns.")
    private boolean redundantEAV = false;

    @Parameter(names = "--entities.redundantSubclassColumns", arity = 1, description = "Store all columns in subclass tables, even if they are also present in a superclass table. If false (default behavior), columns present in superclasses are removed, their superclass location is marked in the column meta table.")
    private boolean redundantSubclassColumns = false;

    @Parameter(names = "--entities.minNumRows", description = "Minimum number of rows required for an entity table. Tables with less rows will not be included.")
    private Integer minNumRows = 1;

    @Parameter(names = "--entities.sortColumnsAlphabetically", arity = 1, description = "Sort columns alphabetically. Otherwise by non-null ratio, most frequent first.")
    private boolean sortColumnsAlphabetically = false;

    @Parameter(names = "--entities.forceTypeSuffix", arity = 1, description = "Whether to always add a type suffix to columns, even if only one datatype is present.")
    private boolean forceTypeSuffix = false;

}
