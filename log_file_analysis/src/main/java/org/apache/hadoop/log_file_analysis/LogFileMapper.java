package org.apache.hadoop.log_file_analysis;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * Type of input key = Object, Type of input value =Text, Type of output key=
 * Text, Type of output value = IntWritable
 * 
 *
 */
public class LogFileMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		/* 
	     * Split the input line into space-delimited fields. 
	     */  
	    String[] fields = value.toString().split(" ");  
	    if (fields.length > 0) {  
	  
	      /* 
	       * Emit the first field - the IP address - as the key 
	       * and the number 1 as the value. 
	       */  
	      String ip = fields[0];  
	      context.write(new Text(ip), new IntWritable(1));  
	    }  
	}

}