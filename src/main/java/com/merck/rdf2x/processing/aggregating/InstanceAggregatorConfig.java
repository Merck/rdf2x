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

import com.beust.jcommander.Parameter;
import com.merck.rdf2x.persistence.InstanceRelationWriter;
import lombok.Data;
import lombok.experimental.Accessors;

import java.io.Serializable;

/**
 * InstanceRelationWriterConfig stores parameters for the {@link InstanceRelationWriter}.
 */
@Data
@Accessors(chain = true)
public class InstanceAggregatorConfig implements Serializable {

    @Parameter(names = "--instances.defaultLanguage", description = "Consider all values in this language as if no language is specified. Language suffix will not be added to columns.")
    private String defaultLanguage = null;

    @Parameter(names = "--instances.addSuperTypes", arity = 1, description = "Automatically add all supertypes to each instance, instance will be persisted in all parent type tables.")
    private boolean addSuperTypes = true;

}
