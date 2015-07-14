package org.apache.hadoop.averagewordlength;

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
public class LetterMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

	    String line = value.toString();

	    /*
	     * The line.split("\\W+") call uses regular expressions to split the
	     * line up by non-word characters.
	     * If you are not familiar with the use of regular expressions in
	     * Java code, search the web for "Java Regex Tutorial."
	     */
	    for (String word : line.split("\\W+")) {
	      if (word.length() > 0) {

	        /*
	         * Obtain the first letter of the word and convert it to lower case
	         */
	        String letter = word.substring(0, 1).toLowerCase();

	        /*
	         * Call the write method on the Context object to emit a key
	         * and a value from the map method.  The key is the 
	         * letter (in lower-case) that the word starts with; the value is the 
	         * word's length.
	         */
	        context.write(new Text(letter), new IntWritable(word.length()));
	      }
	    }
	}

}