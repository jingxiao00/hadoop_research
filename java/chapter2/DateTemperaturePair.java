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
    private String yearMonth;
    private String day;
    protected Integer temperature;

    public int compareTo(DateTemperaturePair o) {
        int compareValue = this.yearMonth.compareTo(o.getYearMonth());
        if (compareValue == 0) {
            compareValue = temperature.compareTo(o.getTemperature());
        }
        return -1 * compareValue;
    }

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
