
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class SecondarySort extends Configured implements Tool {
    public int run(String[] args) throws Exception {
        Job job = new Job();
        job.setJarByClass(SecondarySort.class);
        job.setJobName("SecondarySort");

        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.setMapOutputKeyClass(DateTemperaturePair.class);
        job.setMapOutputValueClass(IntWritable.class);
        
        job.setOutputKeyClass(DateTemperaturePair.class);
        job.setOutputValueClass(IntWritable.class);
        
        job.setMapperClass(SecondarySortingMapper.class);
        job.setReducerClass(SecondarySortingReducer.class);
        job.setPartitionerClass(DateTemperaturePartitioner.class);
        job.setGroupingComparatorClass(DateTemperatureGroupingComparator.class);

        boolean status = job.waitForCompletion(true);
        return status ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
             throw new IllegalArgumentException(
                "!!!!!!!!!!!!!! Usage!!!!!!!!!!!!!!: SecondarySort"
                + "<input-path> <output-path>");
        }
        int returnStatus = ToolRunner.run(new SecondarySort(), args);
        System.exit(returnStatus);
    }
}

