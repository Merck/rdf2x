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

package com.merck.rdf2x.main;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import com.merck.rdf2x.jobs.convert.ConvertConfig;

import java.lang.reflect.Field;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * ReadmeUtils are used to generate README.md file sections, such as the config tables.
 */
public class ReadmeUtils {

    public static void main(String args[]) throws InstantiationException, IllegalAccessException {
        printConfig(ConvertConfig.class);
    }

    private static void printConfig(Class configClass) throws IllegalAccessException, InstantiationException {
        Field[] fields = configClass.getDeclaredFields();
        System.out.println();
        System.out.println("### " + configClass.getSimpleName());
        System.out.println();
        Object defaultConfig = configClass.newInstance();

        System.out.println("|Name|Default|Description|");
        System.out.println("|---|---|---|");
        try {
            for (Field field : fields) {
                field.setAccessible(true);
                StringBuilder sb = new StringBuilder();
                sb.append("|");
                Parameter param = field.getDeclaredAnnotation(Parameter.class);

                if (param != null) {
                    String names = Stream.of(param.names())
                            .collect(Collectors.joining(", "));
                    // name
                    sb.append(names).append("|");

                    // default
                    sb.append(param.required() ? "**required**" : field.get(defaultConfig) + " ").append("|");

                    // description
                    sb.append(param.description()).append("|");

                    System.out.println(sb.toString());
                }

                ParametersDelegate delegate = field.getDeclaredAnnotation(ParametersDelegate.class);

                if (delegate != null) {
                    printConfig(field.getType());
                }
            }
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
    }
}
