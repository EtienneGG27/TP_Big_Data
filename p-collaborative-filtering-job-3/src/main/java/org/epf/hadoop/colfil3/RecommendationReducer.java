package org.epf.hadoop.colfil3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.PriorityQueue;

public class RecommendationReducer extends Reducer<Text, Text, Text, Text> {
    private Text result = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // Utiliser une PriorityQueue pour trier les relations par le nombre de relations communes
        PriorityQueue<String> queue = new PriorityQueue<>((a, b) -> {
            int countA = Integer.parseInt(a.split(":")[1]);
            int countB = Integer.parseInt(b.split(":")[1]);
            return Integer.compare(countB, countA); // Ordre décroissant
        });

        for (Text value : values) {
            queue.add(value.toString());
        }

        // Construire la liste des 5 meilleures recommandations
        StringBuilder recommendations = new StringBuilder();
        int count = 0;
        while (!queue.isEmpty() && count < 5) {
            if (recommendations.length() > 0) {
                recommendations.append(",");
            }
            recommendations.append(queue.poll());
            count++;
        }

        result.set(recommendations.toString());
        context.write(key, result);
    }
}
