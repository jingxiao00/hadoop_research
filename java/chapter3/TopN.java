
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
            /* 将文本切割成若干个token */
            StringTokenizer itr = new StringTokenizer(value.toString());
            /* 循环读取下一个token */
            while (itr.hasMoreTokens()) {
                repToRecordMap.put(Integer.parseInt(itr.nextToken()), " ");
                /* 在map中只保留N个，每次加入一个之后，删除一个最小的 */
                if (repToRecordMap.size() > N) {
                    repToRecordMap.remove(repToRecordMap.firstKey());
                }
            }
        }

        /* 将N个频率最大的数据写到mapper的输出，每个mapper都会产生N个 */
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
            int N = 10; //默认为Top10，可以从参数中解析
            N = Integer.parseInt(context.getConfiguration().get("N"));
            for (IntWritable value : values) {
                repToRecordMap.put(value.get(), " ");
                /* 再做一次检查，因为每个mapper都会输入过来N个，最终只保留N个 */
                if (repToRecordMap.size() > N) {
                    repToRecordMap.remove(repToRecordMap.firstKey());
                }
            }
            /* 剩下N个，输出 */
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

        /* 设置N的值和作业名 */
        Configuration conf = new Configuration();
        conf.set("N", args[0]);
        Job job = Job.getInstance(conf, "TopN");
        job.setJobName("TopN");

        /* 设置输入输出文件 */
        Path inputPath = new Path(args[1]);
        Path outputPath = new Path(args[2]);
        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        /* 引导hadoop找到jar包中的class */
        job.setJarByClass(TopN.class);
        job.setMapperClass(TopTenMapper.class);
        job.setReducerClass(TopTenReducer.class);
        job.setNumReduceTasks(1);

        /* 设置映射器、归约器输出键和值的类型 */
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(IntWritable.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}


