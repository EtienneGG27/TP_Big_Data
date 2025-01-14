package org.epf.hadoop.colfil1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;

public class RelationshipReducer extends Reducer<Text, Text, Text, Text> {

    private Text friendsList = new Text();  // Liste d'amis

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // Utilise un HashSet pour éviter les doublons
        HashSet<String> uniqueFriends = new HashSet<>();

        // Ajoute chaque ami à la liste unique
        for (Text value : values) {
            uniqueFriends.add(value.toString());
        }

        // Construit la liste finale séparée par des virgules
        String joinedFriends = String.join(",", uniqueFriends);
        friendsList.set(joinedFriends);

        // Écrit la clé (utilisateur) et la liste d'amis
        context.write(key, friendsList);
    }
}
