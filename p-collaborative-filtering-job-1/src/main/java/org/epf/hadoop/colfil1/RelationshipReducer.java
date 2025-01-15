package org.epf.hadoop.colfil1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;

public class RelationshipReducer extends Reducer<Text, Text, Text, Text> {
    private Text friendsList = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        HashSet<String> uniqueFriends = new HashSet<>();

        for (Text value : values) {
            uniqueFriends.add(value.toString());
        }

        String joinedFriends = String.join(", ", uniqueFriends);
        friendsList.set(joinedFriends);
        context.write(key, friendsList);
    }
}
