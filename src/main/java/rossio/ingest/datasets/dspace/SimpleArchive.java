package rossio.ingest.datasets.dspace;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import rossio.util.ZipArchiveExporter;

public class SimpleArchive {
	File destZipFile;
	ZipArchiveExporter zip;
	
	public SimpleArchive(File destZipFile) throws IOException {
		this.destZipFile = destZipFile;
		restart();
	}

	public void addItem(String identifier, DcMetadata metadata) throws IOException {
		zip.addFolder(identifier);
		zip.addFile(identifier+"/dublin_core.xml");
		zip.outputStream().write(metadata.getAsXmlString().getBytes(StandardCharsets.UTF_8));
	}

	public OutputStream openBitStream(String identifier, String filename) throws IOException {
		zip.addFile(identifier+"/"+filename);
		return zip.outputStream();
	}
	
	public void addBitStream(String identifier, String filename, byte[] content) throws IOException {
		zip.addFile(identifier+"/"+filename);
		zip.outputStream().write(content);
	}

	public void close() throws IOException {
		zip.close();
	}

	public void abort() throws IOException {
		close();
		destZipFile.delete();
	}

	public void restart() throws IOException {
		zip=new ZipArchiveExporter(destZipFile);		
	}

	public void setCollections(String sourceId, String dSpaceCollections) throws IOException {
		addBitStream(sourceId, "collections", dSpaceCollections.getBytes(StandardCharsets.UTF_8));
	}

	public void setContents(String sourceId, HashMap<String, String> filenameDescriptionMap) throws IOException {
		String contentsFileContent="";
		for(Entry<String, String> cont: filenameDescriptionMap.entrySet()) {
			if(cont.getValue()==null)
				contentsFileContent+=cont.getKey();
			else
				contentsFileContent+=cont.getKey()+"\tdescription:"+cont.getValue();
			contentsFileContent+="\n";
		}
		addBitStream(sourceId, "contents", contentsFileContent.getBytes(StandardCharsets.UTF_8));		
	}

	
	
}
