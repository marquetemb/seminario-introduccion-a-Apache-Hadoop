package com.paradigmatecnologico.seminario.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.log4j.Logger;

/**
 * MapReduceExample: Generate and count the concurrence of the hashtags from tweets and count them
 * 
 */
public class MapReduceExample {

	private static Logger logger = Logger.getLogger(MapReduceExample.class);

	// set if the combiner is active
	private static boolean isCombinerActive = false;

	/**
	 * 
	 * Get all the hashtags of the tweets and generates all the 2-combinations possibles and count them
	 * 
	 * @author marco
	 * 
	 */
	private static class HashTagsConcurrenceMapReduce extends MapReduceBase implements Mapper<LongWritable, Text, Text, LongWritable> {
		private final static LongWritable one = new LongWritable(1);
		private Text word = new Text();

		public void map(LongWritable key, Text value, OutputCollector<Text, LongWritable> output, Reporter reporter) throws IOException {

			// get the string of the value
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);

			// get all the hashtags
			HashSet<String> tagsCollector = new HashSet<String>();
			while (tokenizer.hasMoreTokens()) {
				String token = tokenizer.nextToken();
				if (token.startsWith("#") && token.length() > 1)
					tagsCollector.add(token);
			}

			// generate the hashtags concurrencies and send them to the reducer
			if (tagsCollector.size() > 1) {

				// generate all the hashtags concurrencies
				ArrayList<String> hashTagsConcurrence = getNGrams(tagsCollector);

				// send to the reducer
				for (String concurrence : hashTagsConcurrence) {
					word.set(concurrence);
					output.collect(word, one);
				}
			}
		}

		private ArrayList<String> getNGrams(HashSet<String> tagsCollector) {
			String[] tagsArray = new String[tagsCollector.size()];
			tagsCollector.toArray(tagsArray);
			ArrayList<String> ngrams = new ArrayList<String>();
			for (int i = 0; i < tagsArray.length; i++) {
				ngrams.addAll(generateCombinations(tagsArray, i));
			}
			return ngrams;
		}

		private ArrayList<String> generateCombinations(String[] tagsArray, int position) {
			ArrayList<String> results = new ArrayList<String>();
			for (int i = position + 1; i < tagsArray.length; i++) {
				results.add(tagsArray[position] + "_" + tagsArray[i]);
			}
			return results;
		}

	}

	/**
	 * Reduce the values adding the value to count the total
	 * 
	 * @author marco
	 * 
	 */
	public static class SumTotalReduce extends MapReduceBase implements Reducer<Text, LongWritable, Text, LongWritable> {

		public void reduce(Text key, Iterator<LongWritable> values, OutputCollector<Text, LongWritable> output, Reporter reporter) throws IOException {
			// total sum of the values
			int sum = 0;

			// variable to see how the combiner works
			int iterations = 0;
			while (values.hasNext()) {
				iterations++;
				sum += values.next().get();
			}

			// get only the results with a total greater than 100
			if (sum > 200) {
				// system out to see the work of the combiner
				logger.info("SIZE: " + iterations + " TOTAL: " + sum);

				// collect the results
				output.collect(key, new LongWritable(sum));
			}
		}
	}

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(MapReduceExample.class);
		conf.setJobName("wordcount");

		// set the output variables
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(LongWritable.class);

		// configure the mapper
		conf.setMapperClass(HashTagsConcurrenceMapReduce.class);

		// activate the combiner
		if (isCombinerActive)
			conf.setCombinerClass(SumTotalReduce.class);

		// configure the reducer
		conf.setReducerClass(SumTotalReduce.class);

		// input/output format
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		// input file, it could be in local or in hdfs
		FileInputFormat.setInputPaths(conf, new Path("hdfs://localhost:8020/corpus/corpusELEC.tsv"));
		FileOutputFormat.setOutputPath(conf, new Path("./wordcount"));

		// run it!!!
		JobClient.runJob(conf);

	}
}
