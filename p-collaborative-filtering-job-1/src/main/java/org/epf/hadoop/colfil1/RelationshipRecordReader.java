package org.epf.hadoop.colfil1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import java.io.IOException;

public class RelationshipRecordReader extends RecordReader<LongWritable, Relationship> {
    private LineRecordReader lineRecordReader = new LineRecordReader();
    private LongWritable currentKey = new LongWritable();
    private Relationship currentValue = new Relationship();

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        lineRecordReader.initialize(split, context);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        // Vérifie s'il reste une ligne à lire
        boolean hasNext = lineRecordReader.nextKeyValue();

        if (hasNext) {
            // Obtient le numéro de ligne
            currentKey.set(lineRecordReader.getCurrentKey().get());

            // Récupère la ligne brute
            String line = lineRecordReader.getCurrentValue().toString();

            // Sépare la relation et le timestamp
            String[] parts = line.split(",", 2); // Séparation autour de la première virgule

            if (parts.length == 2) {
                String[] users = parts[0].split("<->"); // Séparation des utilisateurs autour de "<->"

                if (users.length == 2) {
                    // Attribue les utilisateurs à l'objet Relationship
                    currentValue.setId1(users[0].trim());
                    currentValue.setId2(users[1].trim());
                } else {
                    // Si le format est incorrect, vide les valeurs
                    currentValue.setId1("");
                    currentValue.setId2("");
                }
            } else {
                // Si le format est incorrect, vide les valeurs
                currentValue.setId1("");
                currentValue.setId2("");
            }
        }

        return hasNext;
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return currentKey;
    }

    @Override
    public Relationship getCurrentValue() throws IOException, InterruptedException {
        return currentValue;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return lineRecordReader.getProgress();
    }

    @Override
    public void close() throws IOException {
        lineRecordReader.close();
    }
}
