package org.epf.hadoop.colfil1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;

public class RelationshipReducer extends Reducer<Text, Text, Text, Text> {
    private Text friendsList = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // Utilise un HashSet pour éviter les doublons
        HashSet<String> uniqueFriends = new HashSet<>();

        for (Text value : values) {
            uniqueFriends.add(value.toString());
        }

        // Construit la liste d'amis séparée par des virgules
        String joinedFriends = String.join(", ", uniqueFriends);
        friendsList.set(joinedFriends);

        // Écrit le résultat
        context.write(key, friendsList);
    }
}
