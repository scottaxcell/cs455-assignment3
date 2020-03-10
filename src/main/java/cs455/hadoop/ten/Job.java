package cs455.hadoop.ten;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Has the speed and loudness of songs increased as time has increased?
 *
 * InputPath : /data/metadata
 * InputPath : /data/analysis
 * OutputPath :  /home/ten-out
 */
public class Job {
    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            // Give the MapRed job a name. You'll see this name in the Yarn webapp.
            org.apache.hadoop.mapreduce.Job job = org.apache.hadoop.mapreduce.Job.getInstance(conf, "Faster, Louder, Longer");
            // Current class.
            job.setJarByClass(Job.class);
            // Mapper
            job.setMapperClass(MetadataMapper.class);
            job.setMapperClass(AnalysisMapper.class);
            // Combiner. We use the reducer as the combiner in this case.
//            job.setCombinerClass(TenReducer.class);
            // Reducer
            job.setReducerClass(TenReducer.class);
            // Outputs from the Mapper.
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            // Outputs from Reducer. It is sufficient to set only the following two properties
            // if the Mapper and Reducer has same key and total types. It is set separately for
            // elaboration.
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            // path to input in HDFS
            MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MetadataMapper.class);
            MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, AnalysisMapper.class);
//            FileInputFormat.addInputPath(job, new Path(args[0]));
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
