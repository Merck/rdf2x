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

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.merck.rdf2x.flavors.Flavor;
import com.merck.rdf2x.flavors.FlavorFactory;
import com.merck.rdf2x.jobs.convert.ConvertConfig;
import com.merck.rdf2x.jobs.convert.ConvertJob;
import com.merck.rdf2x.jobs.stats.StatsConfig;
import com.merck.rdf2x.jobs.stats.StatsJob;
import com.merck.rdf2x.spark.SparkContextProvider;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaSparkContext;

import javax.naming.ConfigurationException;

/**
 * JobFactory creates {@link Runnable} jobs from command-line arguments.
 * <p>
 * Uses jCommander to load config property values.
 */
@Slf4j
public class JobFactory {
    /**
     * create a {@link Runnable} job from command-line arguments, print usage and return null on errors.
     *
     * @param args command-line arguments
     * @return a new {@link Runnable} job, null if errors occurred
     * @throws ConfigurationException thrown in case config is not valid
     */
    public static Runnable getJob(String args[]) throws ConfigurationException {
        return getJob(args, null);
    }

    /**
     * create a {@link Runnable} job from command-line arguments, print usage and return null on errors.
     *
     * @param args   command-line arguments
     * @param flavor flavor providing default values for config properties
     * @return a new {@link Runnable} job, null if errors occurred
     * @throws ConfigurationException thrown in case config is not valid
     */
    public static Runnable getJob(String args[], Flavor flavor) throws ConfigurationException {

        // create a jCommander instance
        CommandConfig config = new CommandConfig();
        JCommander jc = new JCommander(config);
        jc.setProgramName("rdf2x");

        // register the 'convert' command
        ConvertConfig convertConfig = new ConvertConfig();
        jc.addCommand("convert", convertConfig);
        // register the 'stats' command
        StatsConfig statsConfig = new StatsConfig();
        jc.addCommand("stats", statsConfig);

        if (flavor != null) {
            flavor.setDefaultValues(convertConfig);
            flavor.setDefaultValues(statsConfig);
        }

        try {
            jc.parse(args);
        } catch (ParameterException e) {
            jc.usage();
            System.err.println(e.getMessage());
            return null;
        }

        String command = jc.getParsedCommand();
        // print usage if no command is provided
        if (config.isHelp() || command == null) {
            jc.usage();
            return null;
        }

        try {
            // return the corresponding convert job
            switch (command) {
                case "convert":
                    // if flavor is null, create a correct one and run again
                    // necessary because default values have to be specified before parsing
                    if (flavor == null) {
                        flavor = FlavorFactory.fromString(convertConfig.getFlavor());
                        return getJob(args, flavor);
                    }
                    if (convertConfig.isHelp()) {
                        jc.usage();
                        return null;
                    }
                    convertConfig.validate();
                    JavaSparkContext scConvert = SparkContextProvider.provide();
                    return new ConvertJob(convertConfig, scConvert, flavor);
                case "stats":
                    if (statsConfig.isHelp()) {
                        jc.usage();
                        return null;
                    }
                    JavaSparkContext scStats = SparkContextProvider.provide();
                    return new StatsJob(statsConfig, scStats);
            }
        } catch (ConfigurationException e) {
            jc.usage();
            throw e;
        }
        return null;
    }

}
