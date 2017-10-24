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

import java.io.Serializable;

/**
 * TypeID defines a reference to a single {@link Instance} in an entity table.
 * <p>
 * It is defined by the Instance's type URI and its ID.
 */
@Data
public class TypeID implements Serializable {
    /**
     * index of the Instance's type
     */
    private final Integer typeIndex;
    /**
     * ID of the Instance
     */
    private final Long id;

    @Override
    public String toString() {
        return "Type" + typeIndex + "(ID" + id + ")";
    }

}
