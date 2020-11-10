package Task1;
import java.io.IOException;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import Task1.AirlineOntimeMetadata;

public class popularAirports {

	public static class airportMapper extends Mapper<LongWritable, Text, Text,LongWritable>{
		
		private final static LongWritable one = new LongWritable(1);
		private Text word = new Text();
		@Override
		public void map(LongWritable key, Text lineText, Context context)
			      throws IOException, InterruptedException {
			    String line = lineText.toString();
			    String[] Airport = line.split(",");
			    
			    
			    try {
                    word.set(Airport[AirlineOntimeMetadata.ORIGIN_AIRPORT]);
                    context.write(word, one);
                } catch (Exception e) {
                    System.err.println(e);
                }
                // TO
                try {
                    word.set(Airport[AirlineOntimeMetadata.DESTINATION_AIRPORT]);
                    context.write(word, one);
                } catch (Exception e) {
                    System.err.println(e);
                }
			  }
	}
	
	
	public static class airportReducer extends Reducer<Text ,  LongWritable ,  Text ,  LongWritable > {
	     
		 public void reduce( Text airport,  Iterable<LongWritable> counts,  Context context)
	         throws IOException,  InterruptedException {

	      int sum  = 0;
	      for ( LongWritable count  : counts) {
	        sum  += count.get();
	      }
	      context.write(airport,  new LongWritable(sum));
	    }
	}
	
	 public static void main(String[] args) throws Exception {
		    if (args.length != 2) {
		      System.err.println("Usage: Frequency <input path> <output path>");
		      System.exit(-1);
		    }

		    // create a Hadoop job and set the main class
		    Job job = Job.getInstance();
		    job.setJarByClass(popularAirports.class);
		    job.setJobName("Frequency");

		    // set the input and output path
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));

		    // set the Mapper and Reducer class
		    job.setMapperClass(airportMapper.class);
		    job.setCombinerClass(airportReducer.class);
		    job.setReducerClass(airportReducer.class);

		    // specify the type of the output
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(LongWritable.class);

		    // run the job
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		  }
}
