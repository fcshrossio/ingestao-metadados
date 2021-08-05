package rossio.ingest.solr;

import java.io.File;

import rossio.ingest.solr.manager.Logger;

public class run_TestOaiHarvestIntoSolr {

	public static void main(String[] args) throws Exception {
		//NOT WORKING
		
		
//		RepositoryWithSolr repository=new RepositoryWithSolr("http://localhost:8983/solr/repositorio/");
		RepositoryWithSolr repository=new RepositoryWithSolr("http://dados.rossio.fcsh.unl.pt:8983/solr/testes-repositorio/");
		
//		repository.removeAllFrom("CML-AML");
//		HarvestOaiSourceIntoSolrWithHandler harvest=new HarvestOaiSourceIntoSolrWithHandler("CML-AML", "http://vocabs.rossio.fcsh.unl.pt/agente/xpto", "https://arquivomunicipal3.cm-lisboa.pt/X-arqOAI/oai2.aspx", null, "oai_dc", repository);

//http://vocabs.rossio.fcsh.unl.pt/agentes/c_438671c9|https://digitarq.adbgc.arquivos.pt/oai-pmh/|ADBGC|oai_dc|FAILURE||oai_dc:::ADBGC:105000
//		HarvestOaiSourceIntoSolrWithHandler harvest=new HarvestOaiSourceIntoSolrWithHandler("CML-AML", "http://vocabs.rossio.fcsh.unl.pt/agente/xpto", "https://digitarq.adbgc.arquivos.pt/oai-pmh/", "ADBGC", "oai_dc", null, repository);
//		harvest.resumeWithToken("oai_dc:::ADBGC:119000");
		
//		HarvestOaiSourceIntoSimpleArchive harvest=new HarvestOaiSourceIntoSimpleArchive("https://digitarq.arquivos.pt/oai-pmh", "DO", archive);
//		HarvestOaiSourceIntoSimpleArchive harvest=new HarvestOaiSourceIntoSimpleArchive("http://biblioteca.teatro-dmaria.pt/OAI/", "iconografia", archive);
//		HarvestOaiSourceIntoSimpleArchive harvest=new HarvestOaiSourceIntoSimpleArchive("http://biblioteca.teatro-dmaria.pt/OAI/", "partituras", archive);
//		HarvestOaiSourceIntoSimpleArchive harvest=new HarvestOaiSourceIntoSimpleArchive("http://repox.gulbenkian.pt:80/repox/OAIHandler", "biblioteca_digital", archive);
//		HarvestOaiSourceIntoSimpleArchive harvest=new HarvestOaiSourceIntoSimpleArchive("http://run.unl.pt/oaiextended/request", "com_10362_1967", "oai_dc", archive);

		
		Logger log=new Logger("c:/user/nfrei/desktop/harvester.log.txt");
		
//		System.out.println( harvest.run(100, log).toLogString());
	}
}
