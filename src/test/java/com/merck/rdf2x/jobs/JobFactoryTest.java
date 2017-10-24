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

package com.merck.rdf2x.jobs;

import com.merck.rdf2x.jobs.convert.ConvertJob;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkContext;
import org.junit.Test;

import javax.naming.ConfigurationException;

import static org.junit.Assert.*;

/**
 * Test of {@link JobFactory}
 */
@Slf4j
public class JobFactoryTest {

    @Test
    public void testNullJobs() throws ConfigurationException {
        assertNull(JobFactory.getJob(new String[]{""}));
        assertNull(JobFactory.getJob(new String[]{"convert"}));
        assertNull(JobFactory.getJob(new String[]{"convert", "--help"}));
    }

    @Test
    public void testGetPersistJob() throws ConfigurationException {
        Runnable job = JobFactory.getJob(new String[]{"convert", "--input.file", "test.nq", "--output.target", "Preview"});
        // stop the created Spark Context to avoid conflicts in other tests
        SparkContext.getOrCreate().stop();
        assertNotNull("Non-null write job returned from factory", job);
        assertEquals("Correct job returned from factory", ConvertJob.class, job.getClass());
    }

}
