package cs455.hadoop.nine;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Q9. Imagine a song with a higher hotttnesss score than the song in your answer to Q3. List this
 * songs tempo, time signature, danceability, duration, mode, energy, key, loudness, when it
 * stops fading in, when it starts fading out, and which terms describe the artist who made it.
 * Give both the song and the artist who made it unique names.
 *
 * InputPath : /data/metadata
 * InputPath : /data/analysis
 * OutputPath :  /home/nine-out
 */
public class Job {
    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            // Give the MapRed job a name. You'll see this name in the Yarn webapp.
            org.apache.hadoop.mapreduce.Job job = org.apache.hadoop.mapreduce.Job.getInstance(conf, "Hotter Than Hot");
            // Current class.
            job.setJarByClass(Job.class);
            // Mapper
            job.setMapperClass(MetadataMapper.class);
            job.setMapperClass(AnalysisMapper.class);
            // Combiner. We use the reducer as the combiner in this case.
//            job.setCombinerClass(UnoCombiner.class);
            // Reducer
            job.setReducerClass(ImaginarySongReducer.class);
            // Outputs from the Mapper.
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            // Outputs from Reducer. It is sufficient to set only the following two properties
            // if the Mapper and Reducer has same key and value types. It is set separately for
            // elaboration.
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            // path to input in HDFS
            MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MetadataMapper.class);
            MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, AnalysisMapper.class);
            // path to output in HDFS
            FileOutputFormat.setOutputPath(job, new Path(args[2]));
            // Block until the job is completed.
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
        catch (IOException | InterruptedException | ClassNotFoundException e) {
            System.err.println(e.getMessage());
        }
    }
}
