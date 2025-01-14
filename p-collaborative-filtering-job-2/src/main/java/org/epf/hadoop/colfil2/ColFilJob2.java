package org.epf.hadoop.colfil2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ColFilJob2 {
    public static void main(String[] args) throws Exception {
        // Vérification des arguments
        if (args.length != 2) {
            System.err.println("Usage: ColFilJob2 <input path> <output path>");
            System.exit(-1);
        }

        // Configuration du job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Collaborative Filtering Job 2");

        job.setJarByClass(ColFilJob2.class);

        // Mapper et Reducer
        job.setMapperClass(CommonFriendsMapper.class);
        job.setReducerClass(CommonFriendsReducer.class);

        // Partitioner pour deux reducers
        job.setPartitionerClass(UserPairPartitioner.class);
        job.setNumReduceTasks(2);

        // Clés et valeurs en sortie du Mapper
        job.setMapOutputKeyClass(UserPair.class);
        job.setMapOutputValueClass(Text.class);

        // Clés et valeurs en sortie finale
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Chemins d'entrée et de sortie
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Exécution du job
        boolean success = job.waitForCompletion(true);
        System.exit(success ? 0 : 1);
    }
}
