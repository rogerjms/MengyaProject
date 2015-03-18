package cn.ac.bcc.bioc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import cn.ac.bcc.bioc.DataFileInputFormat;

import java.net.URI;

/**
 * @author guixk@bcc.ac.cn (guixk@bcc.ac.cn)
 * 
 */
public class DataAnalysis {

	public static String WORKING_DIR = "working_dir";
	public static String OUTPUT_DIR = "out_dir";
	public static String EXECUTABLE = "exec_name";
	public static String PROGRAM_DIR = "exec_dir";
	public static String PARAMETERS = "params";

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		//GenericOptionsParser enable the standarad command line arguments into job configuration file
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length < 5) {
			System.err
					.println("Usage: DataAnalysis <Executable and Database Archive on HDFS> <Executable> <Working_Dir><input><output>");
			System.exit(2);
		}
		String programDir = args[0];
		String execName = args[1];
		String workingDir = args[2];
		String inputDir = args[3];
		String outputDir = args[4];
		// "#_INPUTFILE_# -p 95 -o 49 -t 100"

		int numReduceTasks = 0;// setNumReduceTasks(0).We don't need reduce here.

		Job job = new Job(conf, execName);

		// First get the file system handler, delete any previous files, add the
		// files and write the data to it, then pass its name as a parameter to
		// job
		
		Path hdMainDir = new Path(outputDir);
		FileSystem fs = FileSystem.get(conf);
		fs.delete(hdMainDir, true);

		Path hdOutDir = new Path(hdMainDir, "out");

		// Starting the data analysis.
		Configuration jc = job.getConfiguration();
		// Set the value of the name property.Thank to GenericOptionsParser.
		jc.set(WORKING_DIR, workingDir);
		jc.set(EXECUTABLE, execName);
		jc.set(PROGRAM_DIR, programDir); // this the name of the executable archive
		jc.set(OUTPUT_DIR, outputDir);
		jc.setBoolean("mapred.map.tasks.speculative.execution", false);//»•µÙÕ∆≤‚÷¥––

		// using distributed cache
		// flush it
		// reput the data into cache
		long startTime = System.currentTimeMillis();
		
		DistributedCache.addCacheArchive(new URI(programDir), jc);
		
		System.out.println("Add Distributed Cache in "
				+ (System.currentTimeMillis() - startTime) / 1000.0
				+ " seconds");

		FileInputFormat.setInputPaths(job, inputDir);
		FileOutputFormat.setOutputPath(job, hdOutDir);

		job.setJarByClass(DataAnalysis.class);
		job.setMapperClass(RunnerMap.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(DataFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setNumReduceTasks(numReduceTasks);

		startTime = System.currentTimeMillis();

		int exitStatus = job.waitForCompletion(true) ? 0 : 1;
		System.out.println("Job Finished in "
				+ (System.currentTimeMillis() - startTime) / 1000.0
				+ " seconds");
		// clean the cache
		System.exit(exitStatus);
	}
}
