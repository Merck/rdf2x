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

package com.merck.rdf2x.persistence;

import com.merck.rdf2x.persistence.config.FileConfig;
import com.merck.rdf2x.persistence.output.CSVPersistor;
import com.merck.rdf2x.persistence.output.JSONPersistor;
import com.merck.rdf2x.persistence.output.Persistor;
import com.merck.rdf2x.test.TestSparkContextProvider;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;

import static org.junit.Assert.*;

public class PersistorTest extends TestSparkContextProvider {
    @Rule
    public TemporaryFolder testFolder = new TemporaryFolder();

    private SQLContext sql;

    @Before
    public void setUp() {
        sql = new SQLContext(jsc());
    }

    @Test
    public void testWriteCSV() throws IOException {
        File folder = testFolder.newFolder("output");
        Persistor persistor = new CSVPersistor(new FileConfig(folder.getAbsolutePath()), SaveMode.Overwrite);
        testWrite(folder, persistor, this::readDfCSV);
    }

    @Test
    public void testWriteJSON() throws IOException {
        File folder = testFolder.newFolder("output");
        Persistor persistor = new JSONPersistor(new FileConfig(folder.getAbsolutePath()), SaveMode.Overwrite);
        testWrite(folder, persistor, this::readDfJSON);
    }

    /*
        @Test
        public void testWriteDB() throws IOException {
            DbConfig dbConfig = new DbConfig()
                    .setUrl("jdbc:postgresql://localhost:5432/test")
                    .setUser("test").setPassword("test");
            Persistor persistor = new DbPersitorPostgres(dbConfig, SaveMode.Overwrite);

            DataFrame testDataFrame = sql.createDataFrame(getTestRDD(), getTestSchema());
            persistor.writeDataFrame("test", testDataFrame);
        }
    */
    private void testWrite(File folder, Persistor persistor, BiFunction<StructType, String, DataFrame> readDf) {

        String tableName = "test";

        JavaRDD<Row> expectedRDD = getTestRDD();
        StructType expectedSchema = getTestSchema();

        DataFrame testDataFrame = sql.createDataFrame(expectedRDD, expectedSchema);
        persistor.writeDataFrame(tableName, testDataFrame);

        String[] folders = folder.list();

        assertNotNull("Subfolders are accessible", folders);
        assertTrue("A subfolder was created", folders.length > 0);
        assertEquals("Expected subfolders were created", folders[0], tableName);

        DataFrame actualDataFrame = readDf.apply(expectedSchema, Paths.get(folder.getAbsolutePath(), tableName).toString());
        JavaRDD<Row> actualRDD = actualDataFrame.toJavaRDD();

        assertRDDEquals("Expected rows stored for dataframe", expectedRDD, actualRDD);

    }

    private JavaRDD<Row> getTestRDD() {
        List<Row> rows = new ArrayList<>();
        rows.add(RowFactory.create(3, 3d, "newline '\n'", true, 3f));
        rows.add(RowFactory.create(3, 3d, "backslash '\\'", true, 3f));
        rows.add(RowFactory.create(3, 3d, "delimiter '|'", true, 3f));
        rows.add(RowFactory.create(1, 1d, "a \"quoted\" string", true, 1f));
        rows.add(RowFactory.create(2, 2d, "a 'single quoted' string", false, 2f));
        rows.add(RowFactory.create(2, 2d, "unicode chars 'ěščřžýáíé'", false, 2f));
        rows.add(RowFactory.create(3, 3d, "chinese '中英字典' characters", true, 3f));
        return jsc().parallelize(rows);
    }

    private DataFrame readDfJSON(StructType schema, String path) {
        return sql.read().schema(schema).json(path);
    }

    private DataFrame readDfCSV(StructType schema, String path) {
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return sql.read()
                .schema(schema)
                .format("com.databricks.spark.csv")
                .option("header", "true")
                .option("parserLib", "UNIVOCITY") // to handle newlines in values properly
                .load(path);
    }

    private StructType getTestSchema() {
        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("a", DataTypes.IntegerType, false));
        fields.add(DataTypes.createStructField("b", DataTypes.DoubleType, false));
        fields.add(DataTypes.createStructField("c", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("d", DataTypes.BooleanType, false));
        fields.add(DataTypes.createStructField("e", DataTypes.FloatType, false));
        return DataTypes.createStructType(fields);
    }
}
