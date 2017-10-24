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

package com.merck.rdf2x.processing.partitioning;

import com.merck.rdf2x.beans.Instance;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.HashPartitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Map;

/**
 * InstancePartitioner partitions instances by a specified partitioning (e.g. by type)
 */
@Slf4j
@RequiredArgsConstructor
public class InstancePartitioner implements Serializable {
    /**
     * Config storing partitioning type
     */
    private final InstancePartitionerConfig config;

    /**
     * Partition instances by the specified partitioning (e.g. by instance type)
     *
     * @param instances RDD of instances to partition
     * @return partitioned RDD if requested, original RDD if no partitioning is specified
     */
    public JavaRDD<Instance> partition(JavaRDD<Instance> instances) {
        if (!config.isRepartitionByType()) {
            return instances;
        }
        log.info("Getting counts by type hash");
        Map<Integer, Long> typeCounts = getApproximateTypeHashCounts(instances);
        int numPartitions = instances.getNumPartitions();
        long totalInstances = instances.count();
        long instancesPerPartition = totalInstances / numPartitions + 1;

        JavaPairRDD<Integer, Instance> instanceWithPartitions = instances.mapToPair(instance -> {
            int typeHash = getTypeHash(instance);
            int splitIncrement = getSplitIncrement(instance.getId(), typeCounts.get(typeHash), instancesPerPartition);
            return new Tuple2<>(typeHash + splitIncrement, instance);
        });

        log.info("Partitioning instances by type");
        return instanceWithPartitions
                .partitionBy(new HashPartitioner(numPartitions))
                .values();
    }

    private Map<Integer, Long> getApproximateTypeHashCounts(JavaRDD<Instance> instances) {
        return instances.map(this::getTypeHash).countByValue();
    }

    private int getTypeHash(Instance instance) {
        return instance.getTypes().hashCode() * 31;
    }

    private int getSplitIncrement(Long seed, long numInstances, long instancesPerPartition) {
        int numSplits = (int) (numInstances / instancesPerPartition) + 1;
        return (int) (seed % numSplits);
    }

}
