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
package com.merck.rdf2x.processing.filtering;

import com.beust.jcommander.Parameter;
import lombok.Data;
import lombok.experimental.Accessors;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * InstanceFilterConfig stores all information for the {@link InstanceFilter}.
 */
@Data
@Accessors(chain = true)
public class InstanceFilterConfig implements Serializable {

    @Parameter(names = "--filter.type", description = "Accept only resources of specified type. More type URIs can be specified by repeating this parameter.")
    private List<String> types = new ArrayList<>();

    @Parameter(names = "--filter.ignoreOtherTypes", description = "Whether to ignore instance types that were not selected. If true, only the tables for the specified types are created. If false, all of the additional types and supertypes of selected instances are considered as well.")
    private boolean ignoreOtherTypes = true;

}
