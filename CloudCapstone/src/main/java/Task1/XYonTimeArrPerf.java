package Task1;

import java.io.IOException;
import java.util.stream.Stream;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class XYonTimeArrPerf {
	public static class XYonTimeMapper extends Mapper<LongWritable,Text,Text,DoubleWritable>{
		
		private Text keyData = new Text();
		private DoubleWritable arrDelay = new DoubleWritable();
		
		public void map(LongWritable key, Text lineText,Context context) {
			
			try {
				
			Stream.of(lineText.toString())
			.map(line -> line.split(","))
			.forEach(tokens->
			{
				//String org = tokens[AirlineOntimeMetadata.ORIGIN_AIRPORT].replaceAll("\"", "");
				String arrival = tokens[AirlineOntimeMetadata.ARRIVAL_DELAY].replaceAll("\"", "");
				//String dest = tokens[AirlineOntimeMetadata.DESTINATION_AIRPORT].replaceAll("\"", "");
				//String carrier = tokens[AirlineOntimeMetadata.UNIQUE_CARRIER_ID].replaceAll("\"", "");
				
				
				//data[0]=org ; data[1] = delay ; data[2]=dest ; data[3]=carrier 
				String data = tokens[AirlineOntimeMetadata.ORIGIN_AIRPORT].replaceAll("\"", "")+
						" " + tokens[AirlineOntimeMetadata.DESTINATION_AIRPORT].replaceAll("\"", "")+
						" " + tokens[AirlineOntimeMetadata.UNIQUE_CARRIER_ID].replaceAll("\"", "");
				
				keyData.set(data);
				arrDelay.set(Double.parseDouble(arrival));
				
				try {
					context.write(keyData, arrDelay);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			});
			
			}catch(Exception e) {
				System.err.println(e);
			}
		}
		
	}
	
	
	public static class XYonTimeReducer extends Reducer<Text,DoubleWritable,Text,DoubleWritable>{
		
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
			
			int count=0;double sum = 0;
			for(DoubleWritable val: values) {
				sum = sum + val.get();
				count = count + 1;				
			}
			
			Double avg = sum/count;
			context.write(key,new DoubleWritable(avg));
		}
	}

	public static void main(String args[])throws Exception {
		Job job = Job.getInstance();
	    job.setJarByClass(XYonTimeArrPerf.class);
	    job.setJobName("Frequency");

	    // set the input and output path
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));

	    // set the Mapper and Reducer class
	    job.setMapperClass(XYonTimeMapper.class);
	    job.setReducerClass(XYonTimeReducer.class);

	    // specify the type of the output
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(DoubleWritable.class);

	    // run the job
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  
		
	}


}
