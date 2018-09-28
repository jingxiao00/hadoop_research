
import java.io.IOException;

/* hadoop提供的类型和Reducer类 */
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MaxTemperatureReducer 
        extends Reducer<Text, IntWritable, Text, IntWritable> {
    /* key是输入的年份，不用处理，values是一个年份对应的气温，自定义规则处理 */
    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int maxValue = Integer.MIN_VALUE;

        /* 遍历所有value，找最大值，作为归约器的输出 */
        for (IntWritable value : values) {
            maxValue = Math.max(maxValue, value.get());
        }
        /* 通过context.write写入的是实际的输出，其余的都被过滤掉了 */
        context.write(key, new IntWritable(maxValue));
    }
}
