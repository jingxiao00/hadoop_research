
/* topN问题 */
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TopN {
    /* TopN问题的Mapper类 */
    public static class TopTenMapper extends
        Mapper<Object, Text, NullWritable, IntWritable> {
        private TreeMap<Integer, String> repToRecordMap = new TreeMap<Integer, String>();

        public void map(Object key, Text value, Context context) {
            int N = 10;
            N = Integer.parseInt(context.getConfiguration().get("N"));
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                repToRecordMap.put(Integer.parseInt(itr.nextToken()), " ");
                if (repToRecordMap.size() > N) {
                    repToRecordMap.remove(repToRecordMap.firstKey());
                }
            }
        }

        protected void cleanup(Context context) {
            for (Integer i : repToRecordMap.keySet()) {
                try {
                    context.write(NullWritable.get(), new IntWritable(i));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /* TopN问题Reducer类 */
    public static class TopTenReducer extends
        Reducer<NullWritable, IntWritable, NullWritable, IntWritable> {
        private TreeMap<Integer, String> repToRecordMap = new TreeMap<Integer, String>();

        public void reduce(NullWritable key, Iterable<IntWritable> values,
        Context context) throws IOException, InterruptedException {
            int N = 10; //默认为Top10
            N = Integer.parseInt(context.getConfiguration().get("N"));
            for (IntWritable value : values) {
                repToRecordMap.put(value.get(), " ");
                if (repToRecordMap.size() > N) {
                    repToRecordMap.remove(repToRecordMap.firstKey());
                }
            }
            for (Integer i : repToRecordMap.descendingMap().keySet()) {
                context.write(NullWritable.get(), new IntWritable(i));
            }
        }
    }

    /* 主作业流程 */
    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            throw new IllegalArgumentException(
            "!!!!!!!!!!!!!! Usage!!!!!!!!!!!!!!: hadoop jar <jar-name> "
            + "TopN.TopN "
            + "<the value of N>"
            + "<input-path> "
            + "<output-path>");
        }

        Configuration conf = new Configuration();
        conf.set("N", args[0]);
        Job job = Job.getInstance(conf, "TopN");
        job.setJobName("TopN");

        Path inputPath = new Path(args[1]);
        Path outputPath = new Path(args[2]);
        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.setJarByClass(TopN.class);
        job.setMapperClass(TopTenMapper.class);
        job.setReducerClass(TopTenReducer.class);
        job.setNumReduceTasks(1);

        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(IntWritable.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}


