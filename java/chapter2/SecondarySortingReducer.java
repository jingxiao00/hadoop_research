
/* 二次排序的reducer */
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/* 输入键和输出键的类型都是自定义的中间键 */
public class SecondarySortingReducer extends
        Reducer<DateTemperaturePair, IntWritable, DateTemperaturePair, Text> {
    @Override
    protected void reduce(DateTemperaturePair key,
            Iterable<IntWritable> values, Context context) throws IOException,
            InterruptedException {
        /* java的字符串变量，在修改场景下运行速度和效率比string高 */
        StringBuilder sortedTemperatureList = new StringBuilder();
        for (IntWritable temperature : values) {
            sortedTemperatureList.append(temperature);
            sortedTemperatureList.append(",");
        }
        sortedTemperatureList.deleteCharAt(sortedTemperatureList.length() - 1);
        context.write(key, new Text(sortedTemperatureList.toString()));
    }
}


