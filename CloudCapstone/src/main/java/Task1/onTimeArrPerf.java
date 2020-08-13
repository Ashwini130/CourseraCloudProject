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


public class onTimeArrPerf {
	
	public static final int UNIQUE_CARRIER_ID = 8;
	public static final int ARRIVAL_DELAY = 38;
	public static class onTimeMapper extends Mapper<LongWritable,Text,Text,DoubleWritable>{
		
		private Text carrier = new Text();
		private DoubleWritable arrDelay = new DoubleWritable();
		
		public void map(LongWritable key, Text lineText, Context context) {
			
			try {
				
			Stream.of(lineText.toString())
			.map(line -> line.split(","))
			.forEach(tokens->
			{
				String carr = tokens[UNIQUE_CARRIER_ID].replaceAll("\"", "");
				String delay = tokens[ARRIVAL_DELAY].replaceAll("\"", "");
				
				carrier.set(carr);
				arrDelay.set(Double.parseDouble(delay));
				
				try {
					context.write(carrier, arrDelay);
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
	
	public static class onTimeReducer extends Reducer<Text,DoubleWritable,Text,DoubleWritable>{
		
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context ) throws IOException, InterruptedException {
			
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
	    job.setJarByClass(onTimeArrPerf.class);
	    job.setJobName("Frequency");

	    // set the input and output path
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));

	    // set the Mapper and Reducer class
	    job.setMapperClass(onTimeMapper.class);
	    job.setReducerClass(onTimeReducer.class);

	    // specify the type of the output
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(DoubleWritable.class);

	    // run the job
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  
		
	}
}
