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

package com.merck.rdf2x.flavors;

/**
 * FlavorFactory creates new instances of flavors from their String names.
 */
public class FlavorFactory {

    /**
     * creates new Flavor instance from its String name or {@link DefaultFlavor} if input was null
     *
     * @param flavor name of the flavor without the "Flavor" suffix, e.g. 'Wikidata'
     * @return new Flavor instance or {@link DefaultFlavor} if input was null
     */
    public static Flavor fromString(String flavor) {
        if (flavor == null) {
            return new DefaultFlavor();
        }

        try {
            return (Flavor) Class.forName("com.merck.rdf2x.flavors." + flavor + "Flavor").newInstance();
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            throw new IllegalArgumentException("Unrecognized flavor (case-sensitive): " + flavor, e);
        }
    }
}
