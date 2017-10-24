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

package com.merck.rdf2x.persistence.output;

import com.merck.rdf2x.persistence.config.DbConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;

@Slf4j
public class DbPersistorSQLServer extends DbPersistor {

    private static final String TMP_SUFFIX = "__RDF2X_TMP_SUFFIX";

    public DbPersistorSQLServer(DbConfig config, SaveMode saveMode) {
        super(config, saveMode);
    }

    @Override
    public void writeDataFrame(String name, DataFrame df) {
        for (StructField field : df.schema().fields()) {
            String column = field.name();
            // convert booleans to integers to avoid error in Spark 1.6.2
            // "Cannot specify a column width on data type bit."
            if (field.dataType() == DataTypes.BooleanType) {
                df = df.withColumn(column + TMP_SUFFIX, df.col(column).cast(DataTypes.IntegerType))
                        .drop(column)
                        .withColumnRenamed(column + TMP_SUFFIX, column);
            }
        }
        super.writeDataFrame(name, df);
    }

}
