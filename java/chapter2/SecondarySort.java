
/* 此文件是二次排序的主作业流程 */
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

        /* 设置作业，指导hadoop获取jar包 */
        Job job = new Job();
        job.setJarByClass(SecondarySort.class);
        job.setJobName("SecondarySort");

        /* 获取input路径和output路径 */
        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        /* 设置mapper的输出键和输出值 */
        job.setMapOutputKeyClass(DateTemperaturePair.class);
        job.setMapOutputValueClass(IntWritable.class);
       
        /* 设置reducer的输出键和输出值 */
        job.setOutputKeyClass(DateTemperaturePair.class);
        job.setOutputValueClass(IntWritable.class);
       
        /* 指定要使用的mapper，reducer，分区器，比较器 */
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

