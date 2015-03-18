package cn.ac.bcc.bioc;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * Handles the writing of a stream to a file.
 * This is used to capture the output of stdout and stderr when 
 * external programs are executed from map/reduce functions.
 *
 */
public class OutputHandler extends Thread {

	InputStream inpStr;
	String strType;
	String outputFile;

	public OutputHandler(InputStream inpStr, String strType, String outputFile) {
		this.inpStr = inpStr;
		this.strType = strType;
		this.outputFile=outputFile;
	}

	public void run() {
		try {
			InputStreamReader inpStrd = new InputStreamReader(inpStr);
			BufferedReader buffRd = new BufferedReader(inpStrd);
			String line = null;
			BufferedWriter writer=new BufferedWriter(new FileWriter(new File(outputFile)));
			while ((line = buffRd.readLine()) != null) {
				writer.write(line+"\n");				
			}
			buffRd.close();
			writer.flush();
			writer.close();

		} catch (Exception e) {
			System.out.println(e);
		}

	}
}
