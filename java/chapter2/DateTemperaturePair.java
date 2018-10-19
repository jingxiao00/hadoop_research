
/* 此文件实现对组合键的排序，Writable用来持久存储数据类型，
 * WritbaleComparable用来比较定制数据类型 */

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DateTemperaturePair
    /* java不支持多重继承，使用implements继承接口，不同接口之间用逗号隔开 */
    implements Writable, WritableComparable<DateTemperaturePair> {
    private String yearMonth;   //自然键
    private String day;
    protected Integer temperature;  //次键

    /* 用这个方法指出如何对DateTemperaturePair排序 */
    public int compareTo(DateTemperaturePair o) {
        /* 调用String的字符串比较方法compareTo */
        int compareValue = this.yearMonth.compareTo(o.getYearMonth());
        if (compareValue == 0) {
            compareValue = temperature.compareTo(o.getTemperature());
        }
        /* 这样实现降序排列 */
        return -1 * compareValue;
    }

    /* DataOutput用于将java基本类型转换成二进制字符流 */
    public void write(DataOutput dataOutput) throws IOException {
        Text.writeString(dataOutput, yearMonth);
        dataOutput.writeInt(temperature);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.yearMonth = Text.readString(dataInput);
        this.temperature = dataInput.readInt();
    }

    @Override
    public String toString() {
        return yearMonth.toString();
    }

    public String getYearMonth() {
        return yearMonth;
    }

    public void setYearMonth(String text) {
        this.yearMonth = text;
    }

    public void setDay(String day) {
        this.day = day;
    }

    public Integer getTemperature() {
        return temperature;
    }

    public void setTemperature(Integer temperature) {
        this.temperature = temperature;
    }
}
