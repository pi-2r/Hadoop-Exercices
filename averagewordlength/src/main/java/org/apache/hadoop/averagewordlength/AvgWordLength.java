package org.apache.hadoop.averagewordlength;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class AvgWordLength extends Configured implements Tool {
	private static final Logger LOG = Logger.getLogger(AvgWordLength.class);

	public static void main(String[] args) throws Exception {
		System.out.println("on test le bordel");
		int res = ToolRunner.run(new Configuration(), new AvgWordLength(), args);
		LOG.info("result: " + res);
		System.exit(res);
	}

	public int run(String[] arg0) throws Exception {
		Configuration conf = getConf();

		/*
		 * Instantiate a Job object for your job's configuration.
		 */
		Job job = new Job(conf);

		/*
		 * Specify the jar file that contains your driver, mapper, and reducer.
		 * Hadoop will transfer this jar file to nodes in your cluster running
		 * mapper and reducer tasks.
		 */
		job.setJarByClass(AvgWordLength.class);

		/*
		 * Specify an easily-decipherable name for the job. This job name will
		 * appear in reports and logs.
		 */
		job.setJobName("Average Word Length");


		/*
		 * Specify the path of input and the ouput
		 */
		String inputPath = "data/example.txt";
		String outputPath = "/tmp/averagewordlength-example/";
		LOG.info("Input path: " + inputPath);
		LOG.info("Input path: " + outputPath);
		
		job.setJarByClass(AverageReducer.class);
		job.setJobName("toto");
		
		/*
		 * erase old folder
		 */
		FileSystem fs = FileSystem.newInstance(conf);
		if (fs.exists(new Path(outputPath))) {
			fs.delete(new Path(outputPath), true);
		}
		/*
		 * Specify the paths to the input and output data based on the
		 * command-line arguments.
		 */
		FileInputFormat.setInputPaths(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		/*
		 * Specify the mapper and reducer classes.
		 */
		job.setMapperClass(LetterMapper.class);
		job.setReducerClass(AverageReducer.class);
		
		/*
		 * Specify the job's output key and value classes.
		 */
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		/*
		 * Start the MapReduce job and wait for it to finish. If it finishes
		 * successfully, return 0. If not, return 1.
		 */
		return (job.waitForCompletion(true) ? 0 : 1);
	}
}
