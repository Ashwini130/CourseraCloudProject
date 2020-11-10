package Task1;

import java.io.IOException;
import java.util.stream.Stream;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import Task1.AirlineOntimeMetadata;
import Task1.TupleWritable;

public class onTimeDepartPerf {
	public static class onTimeMapper extends Mapper<LongWritable,Text,TupleWritable,DoubleWritable>{
		
		private TupleWritable apt_carrier = new TupleWritable();
		private DoubleWritable arrDelay = new DoubleWritable();
		
		public void map(LongWritable key, Text lineText,Context context) {
			
			try {
				
			Stream.of(lineText.toString())
			.map(line -> line.split(","))
			.forEach(tokens->
			{
				String airport = tokens[AirlineOntimeMetadata.ORIGIN_AIRPORT].replaceAll("\"", "");
				String delay = tokens[AirlineOntimeMetadata.DEPARTURE_DELAY].replaceAll("\"", "");
				String carr = tokens[AirlineOntimeMetadata.UNIQUE_CARRIER_ID].replaceAll("\"", "");
				
				//airport = src apt
				apt_carrier.setfirst(airport);
				apt_carrier.setsecond(carr);
				arrDelay.set(Double.parseDouble(delay));
				
				try {
					context.write(apt_carrier, arrDelay);
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
	
	public static class onTimeReducer extends Reducer<TupleWritable,DoubleWritable,TupleWritable,DoubleWritable>{
		
		public void reduce(TupleWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
			
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
	    job.setJarByClass(onTimeDepartPerf.class);
	    job.setJobName("Frequency");

	    // set the input and output path
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));

	    // set the Mapper and Reducer class
	    job.setMapperClass(onTimeMapper.class);
	    job.setCombinerClass(onTimeReducer.class);
	    job.setReducerClass(onTimeReducer.class);

	    // specify the type of the output
	    job.setOutputKeyClass(TupleWritable.class);
	    job.setOutputValueClass(DoubleWritable.class);

	    // run the job
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  
		
	}

	
}
