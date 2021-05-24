package rossio.ingest.solr;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFParser;

import rossio.data.models.Rossio;
import rossio.ingest.solr.RepositoryWithSolr.ItemHandler;
import rossio.util.RdfUtil;
import rossio.util.RdfUtil.Jena;

public class run_TestIndexing {

	public static void main(String[] args) throws Exception {
//		RepositoryWithSolr repository=new RepositoryWithSolr("http://localhost:8983/solr/repositorio/");
//		Indexer indexer=new Indexer("http://localhost:8983/solr/pesquisa/");
		RepositoryWithSolr repository=new RepositoryWithSolr("http://dados.rossio.fcsh.unl.pt:8984/solr/testes-repositorio/");
		Indexer indexer=new Indexer("http://dados.rossio.fcsh.unl.pt:8984/solr/testes-pesquisa/");
		
		String source = "CML-AML";
		repository.getItemsInSource(source, new ItemHandler() {
			@Override
			public boolean handle(String uuid, String idAtSource, String lastUpdate, byte[] content) throws Exception {
				RDFParser reader = RDFParser.create().lang(Lang.RDFTHRIFT).source(new ByteArrayInputStream(content)).build();
				Model model = Jena.createModel();
				reader.parse(model);
				RdfUtil.printOutRdf(model);
				
				indexer.addItem(source, model, Rossio.NS_ITEM+uuid);
				return true;
			}
		});
		indexer.commit();
	}
}
