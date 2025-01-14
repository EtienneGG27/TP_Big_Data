package org.epf.hadoop.colfil3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class RecommendationMapper extends Mapper<Object, Text, Text, Text> {
    private Text user = new Text();
    private Text recommendation = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] parts = line.split("\t");

        if (parts.length != 2) {
            return; // Ignore les lignes invalides
        }

        String[] users = parts[0].split(",");
        if (users.length != 2) {
            return; // Ignore les paires invalides
        }

        String userA = users[0];
        String userB = users[1];
        String count = parts[1];

        // Générer les recommandations bidirectionnelles
        user.set(userA);
        recommendation.set(userB + ":" + count);
        context.write(user, recommendation);

        user.set(userB);
        recommendation.set(userA + ":" + count);
        context.write(user, recommendation);
    }
}
