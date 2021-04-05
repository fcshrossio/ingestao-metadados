package rossio.ingestao.dspace;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

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
	
	
}
