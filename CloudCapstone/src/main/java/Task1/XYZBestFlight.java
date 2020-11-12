package Task1;

import java.io.IOException;
import java.util.stream.Stream;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class XYZBestFlight {

	public static class XYZBestFlightMapper extends Mapper<LongWritable,Text,Text,Text>{
			

		private Text keyData;
		private Text valueData;
		
		public String amOrPm(String time)
		{
			int t = Integer.parseInt(time);
			if(t<1200)
				return "AM";
			else return "PM";
		}
		
		public void map(LongWritable Key,Text LineText,Context context) {
			
			XYZBestFlightMapper XYZobject= new XYZBestFlightMapper();
			Stream.of(LineText.toString())
			.map(line -> line.split(","))
			.forEach(tokens->
			{
				//KEY VALUES
				String src = tokens[AirlineOntimeMetadata.ORIGIN_AIRPORT].replaceAll("\"", "");
				String dest = tokens[AirlineOntimeMetadata.DESTINATION_AIRPORT].replaceAll("\"", "");
				String AM_PM = XYZobject.amOrPm(tokens[AirlineOntimeMetadata.SCHEDULED_DEPARTURE_TIME].replaceAll("\"", ""));
				String date = tokens[AirlineOntimeMetadata.DEPARTURE_DATE].replaceAll("\"","");
				
				
				String carrier = tokens[AirlineOntimeMetadata.UNIQUE_CARRIER_ID].replaceAll("\"", "");
				String flightNum = tokens[AirlineOntimeMetadata.FLIGHT_NUM].replaceAll("\"", "");
				String delay = tokens[AirlineOntimeMetadata.ARRIVAL_DELAY].replaceAll("\"", "");
				String departureTime = tokens[AirlineOntimeMetadata.SCHEDULED_DEPARTURE_TIME].replaceAll("\"", "");
				String time = departureTime.substring(0, 2)
                        + ":" + departureTime.substring(2, 4);	
				
				String data = src+" "+dest+" "+AM_PM+" "+date;
				String value = carrier + " " + flightNum + " " + time + " " + delay;
				keyData  = new Text(data);
				valueData = new Text(value);
				
				
				try {
					context.write(keyData,valueData);
				} catch (IOException | InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			});
		}
	}
	
	public static class XYZBestFlightReducer extends Reducer<Text,Text,Text,Text>{
		
		public String findMin(String min,String current) {
			String rtn; 
			if(min==null)
			{
				rtn = current;
			}	
			else
			{
				String[] para1 = min.split(" ");
				String[] para2 = current.split(" ");
				
				if(Double.parseDouble(para1[3])>Double.parseDouble(para2[3]))
					rtn = current;
				else rtn = min;
			}
			return rtn;
		}
		
		public void reduce(Text Key,Iterable<Text> values,Context context) {

			String minimum=null;
			for(Text value:values) {
				String arr = value.toString();
				minimum = findMin(minimum,arr);
			}
			
			
			try {
				context.write(Key, new Text(minimum));
			} catch (IOException | InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		}
	
	public static void main(String args[]) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException{
		Job job = Job.getInstance();
	    job.setJarByClass(XYZBestFlight.class);
	    job.setJobName("Frequency");

	    // set the input and output path
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));

	    // set the Mapper and Reducer class
	    job.setMapperClass(XYZBestFlightMapper.class);
	    job.setCombinerClass(XYZBestFlightReducer.class);
	    job.setReducerClass(XYZBestFlightReducer.class);

	    // specify the type of the output
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);

	    // run the job
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  
	}
}
