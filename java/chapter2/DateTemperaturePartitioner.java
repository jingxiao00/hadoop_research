/* 此文件实现定制分区器，将相同键的所有数据发送到同一个归约器 */

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class DateTemperaturePartitioner extends
    Partitioner<DateTemperaturePair, Text> {
    @Override
    public int getPartition(DateTemperaturePair dataTemperaturePair, Text text,
        int i) {
        return Math.abs(dataTemperaturePair.getYearMonth().hashCode() % i);
    }
}
