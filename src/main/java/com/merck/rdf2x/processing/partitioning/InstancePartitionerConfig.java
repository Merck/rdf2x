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

package com.merck.rdf2x.processing.partitioning;

import com.beust.jcommander.Parameter;
import com.merck.rdf2x.processing.schema.EntitySchemaCollector;
import lombok.Data;
import lombok.experimental.Accessors;

import java.io.Serializable;

/**
 * EntitySchemaCollectorConfig stores instructions for the {@link EntitySchemaCollector}.
 */
@Data
@Accessors(chain = true)
public class InstancePartitionerConfig implements Serializable {

    @Parameter(names = "--instances.repartitionByType", arity = 1, description = "Whether to repartition instances by type. Profitable in local mode when, causes an expensive shuffle in cluster mode.", hidden = true)
    private boolean repartitionByType = false;
}
