package org.epf.hadoop.colfil2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class UserPairPartitioner extends Partitioner<UserPair, Text> {
    @Override
    public int getPartition(UserPair key, Text value, int numPartitions) {
        return (key.getFirstUser().hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
}
