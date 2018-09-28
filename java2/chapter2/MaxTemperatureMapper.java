
import java.io.IOException;

/* hadoop提供的类型和Mapper类 */
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/* 公有继承自hadoop的Mapper类 */
/* 4个参数对应输入键，输入值，输出键，输出值的类型，输入键是长整型偏移，是固定的，一般不会使用这个key，
  输入值是一行文本，输出键是年份，输出值是气温 */
/* 使用的类型都是hadoop的类型而不是java内置的，适合网络序列化传输 */
public class MaxTemperatureMapper 
        extends Mapper<LongWritable, Text, Text, IntWritable> {
    private static final int MISSING = 999;         //一个不可修改的私有值
    /* 重写map方法，mapper输出调用context.write方法写入本地临时存储 */
    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        /* map输入的key用不着，输入值是文本内容，转换成一行字符 */
        String line = value.toString();
        String year = line.substring(15, 19);
        int airTemperature;
        if (line.charAt(87) == '+') {
            airTemperature = Integer.parseInt(line.substring(88, 92));
        } else {
            airTemperature = Integer.parseInt(line.substring(87, 92));
        }
        String quality = line.substring(92, 93);
        /* 将经过筛选的记录写入context */
        if (airTemperature != MISSING && quality.matches("[01459]")) {
            /* 年份按照Text格式，气温是Int，与参数对应 */
            context.write(new Text(year), new IntWritable(airTemperature));
        }
    }
}
