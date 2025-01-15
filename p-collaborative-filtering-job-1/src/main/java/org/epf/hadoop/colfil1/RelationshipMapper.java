package org.epf.hadoop.colfil1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class RelationshipMapper extends Mapper<LongWritable, Relationship, Text, Text> {
    private Text user = new Text();
    private Text friend = new Text();

    @Override
    protected void map(LongWritable key, Relationship value, Context context) throws IOException, InterruptedException {
        if (value != null) {
            user.set(value.getId1());
            friend.set(value.getId2());
            context.write(user, friend);

            user.set(value.getId2());
            friend.set(value.getId1());
            context.write(user, friend);
        }
    }
}
