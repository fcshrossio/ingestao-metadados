package rossio.export;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import jakarta.json.Json;
import jakarta.json.JsonArray;
import jakarta.json.JsonArrayBuilder;
import jakarta.json.JsonObjectBuilder;
import rossio.data.models.DcTerms;
import rossio.data.models.Edm;
import rossio.data.models.Ore;
import rossio.data.models.Rdf;
import rossio.export.ExporterMapIntel.MetadataToExport;
import rossio.util.RdfUtil;

import org.apache.commons.io.output.FileWriterWithEncoding;
import org.apache.commons.lang3.StringUtils;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.NodeIterator;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Seq;
import org.apache.jena.rdf.model.Statement;
import org.apache.solr.client.solrj.SolrServerException;


public class MapintelJsonWriter {
	static int maxRecsPerFile = 1000;
	static int maxFilesPerFolder = 1000;
//			final int maxRecsPerFile=20;
//			final int maxFilesPerFolder=10;

	File outputFolder;
	int folderCounter = 0;
	int fileCounter = 0;
	int recordCounter = 0;
	FileWriterWithEncoding writer;

	public MapintelJsonWriter(File outputFolder) {
		super();
		this.outputFolder = outputFolder;
	}

	

	public void write(MetadataToExport md) throws IOException {
		if (writer == null) {
			fileCounter++;
			if (fileCounter == 1) {
				folderCounter++;
			}
			File folder = new File(outputFolder, "rossio-export-mapintel_" + String.format("%03d", folderCounter));
			if (!folder.exists())
				folder.mkdirs();
			writer = new FileWriterWithEncoding(new File(folder, "rossio-export-mapintel_" + String.format("%03d", folderCounter)
					+ "_" + String.format("%04d", fileCounter) + ".json"), StandardCharsets.UTF_8);
			writer.append("[\n");
		} else
			writer.append(",\n");
		writeJson(md);
		recordCounter++;
		if (recordCounter == maxRecsPerFile) {
			writer.append("]");
			writer.flush();
			writer.close();
			writer = null;
			recordCounter = 0;
			if (fileCounter == maxFilesPerFolder)
				fileCounter = 0;
		}
	}

	private void writeJson(MetadataToExport md) throws IOException {
		JsonObjectBuilder ret=Json.createObjectBuilder();
		ret.add("id", Json.createValue(md.uri));
		ret.add("title", writeJsonArraywriteStatement(md.titleSt));
		ret.add("description", writeJsonArraywriteStatement(md.descriptionSt));
		ret.add("subject", writeJsonArraywriteStatement(md.subjectSt));
		writer.append(ret.build().toString());
	}

	private JsonArray writeJsonArraywriteStatement(Statement st) {
		JsonArrayBuilder dcLangArray = Json.createArrayBuilder();
		if (st.getObject().isLiteral()) {
			dcLangArray.add(st.getObject().asLiteral().getString());
		} else if(st.getObject().isResource() && RdfUtil.isSeq(st.getObject().asResource())) {
			Seq seq = RdfUtil.getAsSeq(st.getObject().asResource());
			NodeIterator iter2 = seq.iterator();
		    while (iter2.hasNext()) {
		    	Literal litValue = iter2.next().asLiteral();
		    	dcLangArray.add(litValue.getString());
		    }
		}
		return dcLangArray.build();
	}
	
	
	public void close() throws IOException {
		if (writer != null) {
			writer.append("]");
			writer.flush();
			writer.close();
			writer = null;
		}
	}
}