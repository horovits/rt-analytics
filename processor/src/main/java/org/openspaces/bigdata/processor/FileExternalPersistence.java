/*
* Copyright (c) 2012 GigaSpaces Technologies Ltd. All rights reserved
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*      http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.openspaces.bigdata.processor;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.logging.Logger;

/**
 * This is an {@link org.openspaces.bigdata.processor.ExternalPersistence} implementation to a local file system.
 * 
 * @author Dotan Horovits
 *
 */
public class FileExternalPersistence implements ExternalPersistence {

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

	/* (non-Javadoc)
	 * @see org.openspaces.bigdata.processor.ExternalPersistence#write(java.lang.Object)
	 */
	@Override
	public void write(Object data) throws IOException {
		FileWriter fileWritter = new FileWriter(file,true);
        BufferedWriter bufferWritter = new BufferedWriter(fileWritter);
        bufferWritter.write(data.toString());
//        bufferWritter.newLine();
        bufferWritter.close();
        fileWritter.close();
	}
	
	/* (non-Javadoc)
	 * @see org.openspaces.bigdata.processor.ExternalPersistence#writeBulk(java.lang.Object[])
	 */
	@Override
	public void writeBulk(Object[] dataArray) throws IOException {
		if (dataArray.length < 1) return;
		StringBuffer data = new StringBuffer("");
		for (Object obj : dataArray) {
			data.append(obj.toString()).append("\n");
		}
		write(data.toString());
	}
	
}
