package org.apache.hadoop.log_file_analysis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class ProcessLogs extends Configured implements Tool {
	private static final Logger LOG = Logger.getLogger(ProcessLogs.class);

	private static String inputPath = "data/webLog.txt";
	private static String outputPath = "/tmp/log_file_analysis/";
	private static String jobName = "log_file_analysis";

	public static void main(String[] args) throws Exception {
		LOG.info("start program");
		int res = ToolRunner.run(new Configuration(), new ProcessLogs(), args);
		if (res == 0) {
			LOG.info("result: Finish");
		} else {
			LOG.info("result: ERROR");
		}
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
		job.setJarByClass(ProcessLogs.class);

		/*
		 * Specify an easily-decipherable name for the job. This job name will
		 * appear in reports and logs.
		 */
		job.setJobName(jobName);

		/*
		 * Specify the path of input and the ouput
		 */
		LOG.info("Input path: " + inputPath);
		LOG.info("Input path: " + outputPath);

		job.setJarByClass(ProcessLogs.class);

		/*
		 * erase old folder
		 */
		FileSystem fs = FileSystem.get(conf);
		// FileSystem fs = FileSystem.newInstance(conf);
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
		job.setMapperClass(LogFileMapper.class);
		job.setReducerClass(SumReducer.class);

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