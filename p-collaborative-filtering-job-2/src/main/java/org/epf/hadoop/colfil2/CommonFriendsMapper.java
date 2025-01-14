package org.epf.hadoop.colfil2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class CommonFriendsMapper extends Mapper<Object, Text, UserPair, Text> {
    private UserPair userPair = new UserPair();
    private Text friend = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] parts = line.split("\t");

        if (parts.length != 2) {
            return; // Ignore les lignes invalides
        }

        String user = parts[0];
        String[] friends = parts[1].split(",");
        Set<String> friendSet = new HashSet<>(Arrays.asList(friends));

        // Émettre toutes les paires de relations communes
        for (String friend1 : friends) {
            for (String friend2 : friends) {
                if (!friend1.equals(friend2)) {
                    userPair = new UserPair(friend1, friend2);
                    context.write(userPair, new Text(user));
                }
            }
        }

        // Propager les relations directes
        for (String friend : friendSet) {
            userPair = new UserPair(user, friend);
            context.write(userPair, new Text("direct"));
        }
    }
}
