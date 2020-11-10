package Task1;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;

public class MultiWritable extends ArrayWritable{

	public MultiWritable(Text[] inputData)
	{
		super(Text.class,inputData);
	}
   
	@Override
	public Text[] get() {
	        return (Text[]) super.get();
	}

	@Override
	public String toString() {
	    Text[] values = get();
	    String rtn = " ";
	    for(int i=0;i<values.length;i++) {
	    	rtn = rtn + values[i]+" ";
	    }
	    return rtn;
	}
	

	
}
