package Task1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class TupleWritable implements WritableComparable<TupleWritable>{
	private String first;
    private String second;

    public TupleWritable() {
    }

    public void setfirst(String first) {
        this.first = first;
    }

    public void setsecond(String second) {
        this.second = second;
    }

    public void readFields(DataInput in) throws IOException {
        first = in.readUTF();
        second = in.readUTF();
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(first);
        out.writeUTF(second);
    }

    public int compareTo(TupleWritable other) {
        int firstComparison = first.compareTo(other.first);
        if (firstComparison != 0) {
            return firstComparison;
        }
        return second.compareTo(other.second);
    }

    @Override
    public String toString() {
        return first + ' ' + second;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TupleWritable that = (TupleWritable) o;

        if (first != null ? !first.equals(that.first) : that.first != null) return false;
        return second != null ? second.equals(that.second) : that.second == null;
    }

    @Override
    public int hashCode() {
        int result = first != null ? first.hashCode() : 0;
        result = 31 * result + (second != null ? second.hashCode() : 0);
        return result;
    }
}
