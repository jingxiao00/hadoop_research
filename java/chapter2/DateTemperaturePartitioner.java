
/* 此文件实现定制分区器，将相同键的所有数据发送到同一个归约器 */

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class DateTemperaturePartitioner extends
    Partitioner<DateTemperaturePair, Text> {
    @Override
    /* 入参是mapper的输出键和输出值的类型，是一个String类的内置hash算法 */
    public int getPartition(DateTemperaturePair dataTemperaturePair, Text text,
        int i) {
        return Math.abs(dataTemperaturePair.getYearMonth().hashCode() % i);
    }
}
