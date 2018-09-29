
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class MaxTemperature {
    public static void main(String[] args) throws Exception {
        /* 程序接受两个参数，一个路径指定input，一个路径指定output */
        if (args.length != 2) {
            System.err.println("Usage: MaxTemperature <input path> <out path>");
            System.exit(-1);
        }

        /* 设置作业，用来指导hadoop获取jar包 */
        Job job = new Job();
        job.setJarByClass(MaxTemperature.class);
        job.setJobName("MaxTemperature");
        
        /* 设置input路径和output路径 */
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        /* 指定要使用的mapper和reducer类型，这两个class在jar包中一起提供 */
        job.setMapperClass(MaxTemperatureMapper.class);
        job.setReducerClass(MaxTemperatureReducer.class);

        /* 控制reduce函数的输出类型，要和Reduce类的输出键和输出值参数匹配 */
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
