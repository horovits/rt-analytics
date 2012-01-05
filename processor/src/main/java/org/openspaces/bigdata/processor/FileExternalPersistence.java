package org.openspaces.bigdata.processor;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.logging.Logger;

public class FileExternalPersistence {

	Logger log= Logger.getLogger(this.getClass().getName());

	private File file;
	
	public FileExternalPersistence(File file) throws IOException {
		this.file = file;
		log.info("using file persistence: "+file.getAbsolutePath());
		
		if(file.exists()){
			file.delete();
		}
		
		log.info("creating file for file persistence: "+file.getAbsolutePath());
		file.createNewFile();

	}

	public void write(String data) throws IOException {
		FileWriter fileWritter = new FileWriter(file,true);
        BufferedWriter bufferWritter = new BufferedWriter(fileWritter);
        bufferWritter.write(data);
//        bufferWritter.newLine();
        bufferWritter.close();
        fileWritter.close();
	}
	
	public void writeBulk(Object[] dataArray) throws IOException {
		if (dataArray.length < 1) return;
		StringBuffer data = new StringBuffer("");
		for (Object obj : dataArray) {
			data.append(obj.toString()).append("\n");
		}
		write(data.toString());
	}
	
}
