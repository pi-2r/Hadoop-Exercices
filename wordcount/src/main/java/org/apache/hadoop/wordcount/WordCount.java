package org.apache.hadoop.wordcount;

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

/* 
 * MapReduce jobs are typically implemented by using a driver class.
 * The purpose of a driver class is to set up the configuration for the
 * MapReduce job and to run the job.
 * Typical requirements for a driver class include configuring the input
 * and output data formats, configuring the map and reduce classes,
 * and specifying intermediate data formats.
 * 
 * The following is the code for the driver class:
 */
public class WordCount extends Configured implements Tool {
	private static final Logger LOG = Logger.getLogger(WordCount.class);
	private static String inputPath = "data/example.txt";
	private static String outputPath = "/tmp/wordcount-example/";

	/**
	 * Main entry point that uses the {@link ToolRunner} class to run the Hadoop
	 * job.
	 */
	public static void main(String[] args) throws Exception {
		LOG.info("start WordCount");
		int res = ToolRunner.run(new Configuration(), new WordCount(), args);
		if (res == 0) {
			LOG.info("result: Good");
		} else {
			LOG.info("result: Error");
		}
		System.exit(res);
	}

	/**
	 * Builds and runs the Hadoop job.
	 * 
	 * @return 0 if the Hadoop job completes successfully and 1 otherwise.
	 */

	public int run(String[] arg0) throws Exception {

		Configuration conf = getConf();
		/*
		 * Instantiate a Job object for your job's configuration.
		 */
		Job job = new Job(conf);

		/*
		 * Specify the path of input and the ouput
		 */
		LOG.info("Input path: " + inputPath);
		LOG.info("Input path: " + outputPath);

		/*
		 * Specify the jar file that contains your driver, mapper, and reducer.
		 * Hadoop will transfer this jar file to nodes in your cluster running
		 * mapper and reducer tasks.
		 */
		job.setJarByClass(WordCount.class);

		/*
		 * Specify an easily-decipherable name for the job. This job name will
		 * appear in reports and logs.
		 */
		job.setJobName("Word Count");

		/*
		 * erase old folder
		 */
		FileSystem fs = FileSystem.get(conf);
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
		job.setMapperClass(WordMapper.class);
		job.setReducerClass(SumReducer.class);

		/*
		 * For the word count application, the input file and output files are
		 * in text format - the default format.
		 * 
		 * In text format files, each record is a line delineated by a by a line
		 * terminator.
		 * 
		 * When you use other input formats, you must call the
		 * SetInputFormatClass method. When you use other output formats, you
		 * must call the setOutputFormatClass method.
		 */

		/*
		 * For the word count application, the mapper's output keys and values
		 * have the same data types as the reducer's output keys and values:
		 * Text and IntWritable.
		 * 
		 * When they are not the same data types, you must call the
		 * setMapOutputKeyClass and setMapOutputValueClass methods.
		 */

		/*
		 * Specify the job's output key and value classes.
		 */
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		/*
		 * Start the MapReduce job and wait for it to finish. If it finishes
		 * successfully, return 0. If not, return 1.
		 */
		return job.waitForCompletion(true) ? 0 : -1;
	}

}
