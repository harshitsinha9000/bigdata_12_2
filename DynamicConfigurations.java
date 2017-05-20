import java.io.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

public class DynamicConfigurations extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
      int exitCode = ToolRunner.run(new DynamicConfigurations(), args);
      System.exit(exitCode);
    }

    public int run(String[] args) throws Exception {
      // Check arguments.
    	if (args.length != 2) {
        String usage =
          "Usage: " +
          "hadoop jar Configurations " +
          "<input dir> <output dir>\n";
        System.out.printf(usage);
        System.exit(-1);
      }
 
      String jobName = "WordCount";

      String inputDir = args[0];
      String outputDir = args[1];

      // Define input path and output path.
      Path inputPath = new Path(inputDir);
      Path outputPath = new Path(outputDir);

      Configuration configuration = getConf();
      
      //We will pass parameter as -D min.word.length=<value>
            
      FileSystem fs = outputPath.getFileSystem(configuration);
      fs.delete(outputPath, true);

      Job job = Job.getInstance(configuration);
      job.setJobName(jobName);
      job.setJarByClass(DynamicConfigurations.class);
      job.setMapperClass(WordMapper.class);
      job.setReducerClass(WordReducer.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(IntWritable.class);
      TextInputFormat.setInputPaths(job, inputPath);

      job.setOutputFormatClass(TextOutputFormat.class);
      FileOutputFormat.setOutputPath(job, outputPath);

      // Submit the map-only job.
      return job.waitForCompletion(true) ? 0 : 1;
    }
  
  public static class WordMapper extends
      Mapper<LongWritable, Text, Text, IntWritable> {
	  
    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {

    	String line = value.toString();
    	for (String word : line.split("\\W+")) 
    	{
            context.write(new Text(word), new IntWritable(1));
         }
       }
    }
  
  public static class WordReducer extends
  Reducer<Text, IntWritable, Text, IntWritable> {
 
	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
	    throws IOException, InterruptedException {

		int sum = 0;
		for (IntWritable value : values) {
			sum += value.get();
		}
		if(sum>Integer.parseInt(context.getConfiguration().get("min.word.count")))
		{
			context.write(key, new IntWritable(sum));
		}
		
	}
  }
}