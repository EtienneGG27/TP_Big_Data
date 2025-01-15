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
            return;
        }

        String[] users = parts[0].split(",");
        if (users.length != 2) {
            return;
        }

        String userA = users[0];
        String userB = users[1];
        String[] details = parts[1].split(",");
        int commonRelations = Integer.parseInt(details[0]);
        int isDirect = Integer.parseInt(details[1]);

        if (isDirect == 0) {
            user.set(userA);
            recommendation.set(userB + ":" + commonRelations);
            context.write(user, recommendation);

            user.set(userB);
            recommendation.set(userA + ":" + commonRelations);
            context.write(user, recommendation);
        }

        if (isDirect == 1) {
            user.set(userA);
            recommendation.set("direct:" + userB);
            context.write(user, recommendation);

            user.set(userB);
            recommendation.set("direct:" + userA);
            context.write(user, recommendation);
        }
    }
}
