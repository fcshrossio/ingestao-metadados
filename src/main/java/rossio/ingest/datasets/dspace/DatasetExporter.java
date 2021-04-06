package rossio.ingest.datasets.dspace;

import java.io.File;
import java.io.FileOutputStream;
import java.util.List;
import java.util.Properties;
import java.util.zip.GZIPOutputStream;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.riot.system.StreamRDFOps;
import org.apache.jena.riot.system.StreamRDFWriter;

import rossio.data.models.DcTerms;
import rossio.dspace.DspaceApiClient;
import rossio.dspace.DspaceApiClient.ModelHandler;
import rossio.util.Global;

public class DatasetExporter {
	
	private DspaceApiClient dspaceApiClient;
	private File exportDestinationFolder;

	public DatasetExporter(DspaceApiClient dspaceApiClient, File exportDestinationFolder) {
		super();
		this.dspaceApiClient = dspaceApiClient;
		this.exportDestinationFolder = exportDestinationFolder;
	}
 
	public void runExport() throws Exception { 
		List<Resource> collections = dspaceApiClient.listCollectionsMetadata();
		for(Resource colRes:collections) {
			String colUuid=colRes.getURI();
			colUuid=colUuid.substring(colUuid.lastIndexOf('/')+1);
			
			File exportFile=new File(exportDestinationFolder, colUuid+".ttl.gz");
			FileOutputStream fos=new FileOutputStream(exportFile);
			GZIPOutputStream outStream=new GZIPOutputStream(fos);
			StreamRDF writer = StreamRDFWriter.getWriterStream(outStream, Lang.TURTLE) ;
//			StreamRDF writer = StreamRDFWriter.getWriterStream(outStream, Lang.NTRIPLES) ;
			writer.prefix("dct", DcTerms.NS);
			
			StreamRDFOps.graphToStream(colRes.getModel().getGraph(), writer) ;
			
			dspaceApiClient.getAllItemsMetadataInCollection(colUuid , new ModelHandler() {
				@Override
				public boolean handle(Model model) {
					try {
//						model.write(outStream, Lang.RDFXML.getName());
						StreamRDFOps.graphToStream(model.getGraph(), writer) ;
						return true;
					} catch (Exception e) {
						e.printStackTrace();
						return false;
					}
				}
			});
			outStream.close();
			fos.close();
			
		}
	}
	
}
