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

import com.merck.rdf2x.processing.schema.RelationSchemaStrategy;
import lombok.Data;

import java.io.Serializable;
import java.util.Collection;
import java.util.TreeSet;

/**
 * RelationSchema defines a sorted set of {@link RelationTable}s to be persisted in a file or database.
 */
@Data
public class RelationSchema implements Serializable {

    /**
     * Set of tables in this schema, sorted by table name.
     */
    private final TreeSet<RelationTable> tables;

    /**
     * @param tables Set of tables in this schema, will be copied and sorted by table name
     */
    public RelationSchema(Collection<RelationTable> tables) {
        this.tables = new TreeSet<>(tables);
    }

    /**
     * Create single table schema
     *
     * @param singleRelationTableName name of the single table
     */
    public RelationSchema(String singleRelationTableName) {
        this.tables = new TreeSet<>();
        this.tables.add(new RelationTable(singleRelationTableName, RelationSchemaStrategy.SingleTable));
    }

}
