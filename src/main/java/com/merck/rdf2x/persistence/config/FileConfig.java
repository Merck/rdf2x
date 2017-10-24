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

package com.merck.rdf2x.persistence.config;

import com.beust.jcommander.Parameter;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import javax.naming.ConfigurationException;
import java.io.File;

/**
 * FileConfig stores output path for outputting files to disk
 */
@Data
@Accessors(chain = true)
@NoArgsConstructor
@AllArgsConstructor
public class FileConfig {
    @Parameter(names = "--output.folder", description = "Folder to output the files to", required = false)
    private String outputFolder;

    /**
     * Validate the config, throw {@link ConfigurationException} on error
     *
     * @throws ConfigurationException found error
     */
    public void validate() throws ConfigurationException {
        if (outputFolder == null) {
            throw new ConfigurationException("Specify the output folder with --output.folder");
        }
        File f = new File(outputFolder);
        if (f.exists() && !f.isDirectory()) {
            throw new ConfigurationException("The output path is not a folder: " + outputFolder);
        }
    }
}
