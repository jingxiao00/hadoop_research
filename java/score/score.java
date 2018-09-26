import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/* 一个和文件名同名的public class score */
public class score {
    /* 继承Mapper类 */
    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer tokenizerArticle = new StringTokenizer(line, "\n");
            while (tokenizerArticle.hasMoreElements()) {

                /* 将一行string转换成token使用 */
                StringTokenizer tokenizerLine = new StringTokenizer(tokenizerArticle.nextToken());
                String strName = tokenizerLine.nextToken();// 学生姓名部分
                String strScore = tokenizerLine.nextToken();// 成绩部分
                Text name = new Text(strName);
                int scoreInt = Integer.parseInt(strScore);

                /* 写入映射器输出，int要用IntWritable转换成输出格式 */
                context.write(name, new IntWritable(scoreInt));
            }
        }
    }

    /* 继承Reducer类 */
    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            int count = 0;
            Iterator<IntWritable> iterator = values.iterator();
            while (iterator.hasNext()) {
                /* 使用迭代器遍历一个键的多个值 */
                sum += iterator.next().get();
                count++;
            }
            int average = (int) sum / count;

            /* 将平均值作为归约器的输出 */
            context.write(key, new IntWritable(average));
        }
    }

    /* main方法 */
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapred.job.tracker", "master:9000");

        /* 指示input和output文件路径 */
        String[] ioArgs = new String[]{"input/score", "output"};
        String[] otherArgs = new GenericOptionsParser(conf, ioArgs).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: Score Average <in> <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "Score Average");
        job.setJarByClass(score.class);

        // 设置Map、Combine和Reduce处理类
        job.setMapperClass(Map.class);
        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);

        // 设置输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 将输入的数据集分割成小数据块splites，提供一个RecordReder的实现
        job.setInputFormatClass(TextInputFormat.class);

        // 提供一个RecordWriter的实现，负责数据输出
        job.setOutputFormatClass(TextOutputFormat.class);

         // 设置输入和输出目录
         FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
         FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
         System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
