package Task1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class MultiWritable implements WritableComparable<MultiWritable>{
	private String[] data;

    public MultiWritable() {
    }

    public void setData(String[] inputData) {
    	int length = inputData.length;
    	this.data = new String[length];
    	for(int i=0;i<length;i++)
    		this.data[i] = inputData[i];
    }

    
    public void readFields(DataInput in) throws IOException {
    	int length = in.readInt();

    	for(int i=0;i<length;i++)
    		data[i] = in.readUTF();
    }

    
    public void write(DataOutput out) throws IOException {
    	int length = data.length;
    	out.writeInt(length);
    	for(int i=0;i<length;i++)
    		out.writeUTF(data[i]);
    }

  
	@Override
	public int compareTo(MultiWritable o) {
		// TODO Auto-generated method stub
		return 0;
	}

	

	
}
