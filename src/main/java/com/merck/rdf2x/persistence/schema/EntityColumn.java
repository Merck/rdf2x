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

package com.merck.rdf2x.persistence.schema;

import com.merck.rdf2x.beans.Predicate;
import lombok.Data;
import lombok.NonNull;

import java.io.Serializable;

/**
 * EntityColumn defines structure of a column in an {@link EntityTable}.
 * <p>
 * It is defined by a {@link Predicate} and a column name.
 */
@Data
public class EntityColumn implements Serializable {

    /**
     * the full name of this column
     */
    private final String name;
    /**
     * name of superclass column this column is substituted by, in the form of 'table.column'.
     * Null if column is stored directly in its table.
     */
    private String storedInSuperclassColumn = null;
    /**
     * the property stored by this column
     */
    private final EntityProperty property;

    /**
     * @param name     the full name of this column
     * @param property the property stored by this column
     */
    public EntityColumn(@NonNull String name, EntityProperty property) {
        this.name = name;
        this.property = property;
    }


    @Override
    public String toString() {
        return name + "(" +
                property +
                (storedInSuperclassColumn == null ? "" : ", Stored in " + storedInSuperclassColumn) +
                ")";
    }

}
