
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

public class top10cat {
    public static class catmapper extends
        Mapper<Object, Text, NullWritable, Text> {
        private TreeMap<Double, String> recordmap = new TreeMap<Double, String>();
        public void map(Object key, Text value, Context context) {
            int N = 10;
            N = Integer.parseInt(context.getConfiguration().get("N"));
            String[] tokens = value.toString().split(",");
            Double weight = Double.parseDouble(tokens[0]);
            recordmap.put(weight, value.toString());
            if (recordmap.size() > N)
                recordmap.remove(recordmap.firstKey());
        }

        protected void cleanup(Context context) {
            for (String i : recordmap.values()) {
                try {
                    context.write(NullWritable.get(), new Text(i));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static class catreducer extends
        Reducer<NullWritable, Text, NullWritable, Text> {
        private TreeMap<Double, String> recordmap = new TreeMap<Double, String>();
        public void reduce(NullWritable key, Iterable<Text> values,
        Context context) throws IOException, InterruptedException {
            int N = 10;
            N = Integer.parseInt(context.getConfiguration().get("N"));
            for (Text value : values) {
                String[] tokens = value.toString().split(",");
                Double weight = Double.parseDouble(tokens[0]);
                recordmap.put(weight, value.toString());
                if (recordmap.size() > N)
                    recordmap.remove(recordmap.firstKey());
            }

            for (String i : recordmap.values()) {
                context.write(NullWritable.get(), new Text(i));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            throw new IllegalArgumentException(
            "!!!!!!!!!!!!!! Usage!!!!!!!!!!!!!!: hadoop jar <jar-name> "
            + "top10cat.top10cat "
            + "<the value of N>"
            + "<input-path> "
             + "<output-path>");
        }
        Configuration conf = new Configuration();
        conf.set("N", args[0]);
        Job job = Job.getInstance(conf, "top10cat");
        job.setJobName("top10cat");

        Path inputPath = new Path(args[1]);
        Path outputPath = new Path(args[2]);
        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.setJarByClass(top10cat.class);
        job.setMapperClass(catmapper.class);
        job.setReducerClass(catreducer.class);
        job.setNumReduceTasks(1);

        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

