package org.epf.hadoop.colfil3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.PriorityQueue;
import java.util.Set;

public class RecommendationReducer extends Reducer<Text, Text, Text, Text> {
    private Text result = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Set<String> directFriends = new HashSet<>();
        PriorityQueue<String> recommendations = new PriorityQueue<>((a, b) -> {
            int countA = Integer.parseInt(a.split(":")[1]);
            int countB = Integer.parseInt(b.split(":")[1]);
            return Integer.compare(countB, countA); // Ordre décroissant
        })for (Text value : values) {
            String val = value.toString();
            if (val.startsWith("direct:")) {
                directFriends.add(val.substring(7));
            } else {
                recommendations.add(val);
            }
        }

        StringBuilder topRecommendations = new StringBuilder();
        int count = 0;
        while (!recommendations.isEmpty() && count < 5) {
            String rec = recommendations.poll();
            String recommendedUser = rec.split(":")[0];

            if (!directFriends.contains(recommendedUser)) {
                if (topRecommendations.length() > 0) {
                    topRecommendations.append(",");
                }
                topRecommendations.append(rec);
                count++;
            }
        }

        result.set(topRecommendations.toString());
        context.write(key, result);
    }
}
