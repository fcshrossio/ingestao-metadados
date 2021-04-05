package rossio.ingestao.dspace;

import java.io.File;

public class run_TestOaiHarvest {

	public static void main(String[] args) throws Exception {
		File simpleArchiveZip=new File("C:\\Users\\nfrei\\Desktop\\DspaceSimpleArchive.zip");
		SimpleArchive archive=new SimpleArchive(simpleArchiveZip);
		
//		HarvestOaiSourceIntoSimpleArchive harvest=new HarvestOaiSourceIntoSimpleArchive("https://arquivomunicipal3.cm-lisboa.pt/X-arqOAI/oai2.aspx", null, archive);
//		HarvestOaiSourceIntoSimpleArchive harvest=new HarvestOaiSourceIntoSimpleArchive("https://digitarq.arquivos.pt/oai-pmh", "DO", archive);
//		HarvestOaiSourceIntoSimpleArchive harvest=new HarvestOaiSourceIntoSimpleArchive("http://biblioteca.teatro-dmaria.pt/OAI/", "iconografia", archive);
//		HarvestOaiSourceIntoSimpleArchive harvest=new HarvestOaiSourceIntoSimpleArchive("http://biblioteca.teatro-dmaria.pt/OAI/", "partituras", archive);
//		HarvestOaiSourceIntoSimpleArchive harvest=new HarvestOaiSourceIntoSimpleArchive("http://repox.gulbenkian.pt:80/repox/OAIHandler", "biblioteca_digital", archive);
		HarvestOaiSourceIntoSimpleArchive harvest=new HarvestOaiSourceIntoSimpleArchive("http://run.unl.pt/oaiextended/request", "com_10362_1967", archive);
		harvest.run(100);
	}
}
