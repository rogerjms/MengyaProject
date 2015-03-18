package cn.ac.bcc.bioc;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author guixk@bcc.ac.cn
 * 
 */

public class RunnerMap extends Mapper<String, String, IntWritable, Text> {
	
	private String localBlastProgram = "";
	
	  /**
	   * Called once at the beginning of the task.
	   */
	@Override
	public void setup(Context context) throws IOException{
		Configuration conf = context.getConfiguration();
		//DistributedCache files on HDFS will be relocated to every datanode.
		Path[] local = DistributedCache.getLocalCacheArchives(conf);
		System.out.println("-------------------------------------");
		System.out.println("local store:"+local[0]);
		System.out.println("-------------------------------------");
		this.localBlastProgram = local[0].toUri().getPath();
	}
	

	public void map(String key, String value, Context context) throws IOException,
		InterruptedException {
		long startTime = System.currentTimeMillis();
		String endTime = "";
		Configuration conf = context.getConfiguration();
		String execName = conf.get(DataAnalysis.EXECUTABLE);
		String workingDir = conf.get(DataAnalysis.WORKING_DIR);
		System.out.println("the map key : " + key);
		System.out.println("the value path : " + value.toString());
		// We have the full file names in the value.
		String[] tmp = value.split(File.separator);
		String fileNameOnly = tmp[tmp.length - 1];// Last part should be the file name.

		String localInputFile = workingDir + File.separator + fileNameOnly;//local working directory tmp
		
		//String outFile = workingDir + File.separator + fileNameOnly + ".out";
		String stdOutFile = workingDir + File.separator + fileNameOnly + ".stdout";
		String stdErrFile = workingDir + File.separator + fileNameOnly + ".stderr";

		// download the file from HDFS
		Path inputFilePath = new Path(value);
		FileSystem fs = inputFilePath.getFileSystem(conf);
		fs.copyToLocalFile(inputFilePath, new Path(localInputFile));

		// Prepare the arguments to the executable
		//String execCommand = cmdArgs.replaceAll("#_INPUTFILE_#", localInputFile);
		//if (cmdArgs.indexOf("#_OUTPUTFILE_#") > -1) {
		//	execCommand = execCommand.replaceAll("#_OUTPUTFILE_#", outFile);
		//}else{
		//	outFile = stdOutFile;
		//}
		
		endTime = Double.toString(((System.currentTimeMillis() - startTime) / 1000.0));
		System.out.println("Before running the executable Finished in " + endTime + " seconds");
		
		String execCommand = this.localBlastProgram + File.separator + execName + " " + localInputFile + " " + localInputFile;
		//Create the external process
		System.out.println("-------------------------------------");
		System.out.println("local command:"+execCommand);
		System.out.println("-------------------------------------");
		
		startTime = System.currentTimeMillis();
		
		Process p = Runtime.getRuntime().exec(execCommand);

		OutputHandler inputStream = new OutputHandler(p.getInputStream(), "INPUT", stdOutFile);
		OutputHandler errorStream = new OutputHandler(p.getErrorStream(), "ERROR", stdErrFile);

		// start the stream threads.
		inputStream.start();
		errorStream.start();
		
		p.waitFor();
		//end time of this procress
		endTime = Double.toString(((System.currentTimeMillis() - startTime) / 1000.0));
		System.out.println("Program Finished in " + endTime + " seconds");
		
		
		//Upload the results to HDFS
		startTime = System.currentTimeMillis();
		
		//Path outputDirPath = new Path(outputDir);
		//Path outputFileName = new Path(outputDirPath,fileNameOnly);
		//fs.copyFromLocalFile(new Path(outFile),outputFileName);

		endTime = Double.toString(((System.currentTimeMillis() - startTime) / 1000.0));
		System.out.println("Upload Result Finished in " + endTime + " seconds");
		
	}
}
