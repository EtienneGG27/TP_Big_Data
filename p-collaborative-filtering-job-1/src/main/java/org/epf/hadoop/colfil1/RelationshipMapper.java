package org.epf.hadoop.colfil1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class RelationshipMapper extends Mapper<LongWritable, Relationship, Text, Text> {

    private Text user = new Text();  // Clé (utilisateur)
    private Text friend = new Text();  // Valeur (ami)

    @Override
    protected void map(LongWritable key, Relationship value, Context context) throws IOException, InterruptedException {
        // Log pour afficher la clé et la valeur d'entrée
        System.out.println("Mapper Input Key: " + key + ", Value: " + value);

        // Première relation : Utilisateur -> Ami
        user.set(value.getId1());
        friend.set(value.getId2());
        context.write(user, friend);

        // Log pour afficher ce qui est écrit pour la première relation
        System.out.println("Mapper Output: " + user + " -> " + friend);

        // Deuxième relation : Ami -> Utilisateur (relation inversée)
        user.set(value.getId2());
        friend.set(value.getId1());
        context.write(user, friend);

        // Log pour afficher ce qui est écrit pour la deuxième relation
        System.out.println("Mapper Output: " + user + " -> " + friend);
    }
}
