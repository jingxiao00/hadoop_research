
/* 此文件定义分组比较器 */

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class DateTemperatureGroupingComparator extends WritableComparator {
    public DateTemperatureGroupingComparator() {
        /* 调用父类构造函数 */
        super(DateTemperaturePair.class, true);
    }
    @Override
    /* 决定输出键和归约器的对应关系，保证相同键发送到同一个归约器 */
    public int compare(WritableComparable a, WritableComparable b) {
        DateTemperaturePair pair1 = (DateTemperaturePair) a;
        DateTemperaturePair pair2 = (DateTemperaturePair) b;
        return pair1.getYearMonth().compareTo(pair2.getYearMonth());
    }
}

