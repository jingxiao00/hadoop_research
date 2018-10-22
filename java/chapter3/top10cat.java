
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
    public static class CatMapper extends
        Mapper<Object, Text, NUllWritable, String> {
        private SortedMap<Double, Text> top10cats = new TreeMap<Double, Text>();
        private int N = 10;

        protected void setup(Context context) {
            Configuration conf = context.getConfiguration();
            N = conf.get("N");
        }
        public void map(Object key, Text Value, Context context) {
            String[] tokens = value.split(",");
            Double weight = Double.parseDouble(tokens[0]);
            /* 将键值对插入集合中，键是重量，值是整个Text */
            top10cats.put(weight, value);

            /* 按照重量，只保留N个体重最大的 */
            if (top10cats.size() > N)
                top10cats.remove(top10cats.firstKey());
        }
        protected void cleanup(Context context) {
            for (String catAttributes : top10cats.values()) {
                context.write(NullWritable.get(), catAttributes);
            }
        }
    }

    public static class CatReducer extends
        Reducer<NullWritable, String, NullWritable, Text> {
        public void reduce(NullWritable key, Iterable<Text> values,
        Context context) {
            SortedMap<Double, Text> finatop10 = new TreeMap<Double, Text>();
            /* 将所有value加入集合中，然后删除到只剩下N个 */
            for (Text catRecord : values) {
                String[] tokens = catRecord.split(",");
                /* 从token[0]中解析出Double类型的键 */
                Double weight = Double.parseDouble(tokens[0]);
                finaltop10.put(weight, value);
                if (finaltop10.size() > N) {
                    finaltop10.remove(finaltop10.firstKey());
                }
            }

            /* 将最终的N个输出 */
            for (Text text : finaltop10.values()) {
                context.write(NullWritable.get(), text);
            }
        }
    }
}

