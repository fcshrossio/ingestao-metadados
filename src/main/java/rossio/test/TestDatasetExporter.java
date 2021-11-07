package rossio.test;

import rossio.export.CommandLineDatasetExporter;

public class TestDatasetExporter {
	public static void main(String[] args) throws Exception {
		
		for(String srcId: new String[] {
//				"https://digitarq.adlra.arquivos.pt/oai-pmh/#ADLRA",
//					"-source_id", "https://digitarq.adctb.arquivos.pt/oai-pmh/#ADCTB",
//					"-source_id", "http://run.unl.pt/oaiextended/request#com_10362_1967",
//					"-source_id", "http://biblioteca.tndm.pt/OAI/#FOTOGRAFIAS",
//					"-source_id", "http://biblioteca.tndm.pt/OAI/#PARTITURAS",
//					"-source_id", "https://digitarq.advrl.arquivos.pt/oai-pmh/#ADVRL",
//					"-source_id", "http://biblioteca.tndm.pt/OAI/#TEXTOS_CENICOS",
//					"-source_id", "https://projetos.dhlab.fcsh.unl.pt/oai#memoriacovid",
//					"-source_id", "https://digitarq.adfar.arquivos.pt/oai-pmh/#ADFAR",
//					"-source_id", "https://digitarq.ahu.arquivos.pt/oai-pmh/#AHU",
//					"-source_id", "https://digitarq.adptg.arquivos.pt/oai-pmh/#ADPTG",
//					"-source_id", "https://arquivomunicipal3.cm-lisboa.pt/X-arqOAI/oai2.aspx#",
//					"-source_id", "https://medievalista.iem.fcsh.unl.pt/index.php/medievalista/oai#medievalista",
//					"-source_id", "https://baimages.gulbenkian.pt/oai-pmh/oai/oai.aspx#IMD",
//					"-source_id", "https://digitarq.adbja.arquivos.pt/oai-pmh/#ADBJA",
//					"-source_id", "https://digitarq.adavr.arquivos.pt/oai-pmh/#ADAVR",
//					"-source_id", "https://digitarq.adstr.arquivos.pt/oai-pmh/#ADSTR",
//					"-source_id", "http://biblioteca.tndm.pt/OAI/#PROGRAMAS",
//					"-source_id", "http://biblioteca.tndm.pt/OAI/#EJM",
//					"-source_id", "http://ojs.letras.up.pt/index.php/tm/oai#tm",
//					"-source_id", "http://oai.openedition.org/#journals:etnografica",
//					"-source_id", "https://digitarq.adstb.arquivos.pt/oai-pmh/#ADSTB",
//					"-source_id", "http://oai.openedition.org/#journals:cultura",
//					"-source_id", "https://digitarq.adevr.arquivos.pt/oai-pmh/#ADEVR",
//					"-source_id", "http://biblioteca.tndm.pt/OAI/#FOLHETOS",
//					"-source_id", "https://digitarq.advct.arquivos.pt/oai-pmh/#ADVCT",
//"https://www.fcsh.unl.pt/rcl/index.php/rcl/oai#rcl",
//"https://impactum-journals.uc.pt/mj/oai#mj",
//"https://digitarq.cpf.arquivos.pt/oai-pmh/#CPF",
//"https://digitarq.advis.arquivos.pt/oai-pmh/#ADVIS",
//"http://pesquisa.adporto.arquivos.pt/oai-pmh/#ADPRT",
//"https://digitarq.adbgc.arquivos.pt/oai-pmh/#ADBGC",
//"http://oai.openedition.org/#journals:sociologico",
//"https://digitarq.adgrd.arquivos.pt/oai-pmh/#ADGRD",
"https://digitarq.arquivos.pt/oai-pmh/#TT",
"https://revistas.rcaap.pt/index.php/pdh/oai#pdh",
		}) {
		
			CommandLineDatasetExporter.main(new String[] {
					"-solr_url_repository", "http://192.168.111.170:8983/solr/repositorio",
					"-sources_file", "src/data/oai_sources.ttl",		
//		"-sources_file", "src/data/oai_sources-test-harvest-short.ttl",		
					"-export_folder", "C:\\Users\\nfrei\\Desktop\\ROSSIO_Exports\\dataset",
					"-source_id", srcId,
//		"-sample_size", "100",
			});
		}
		
//	        "https://digitarq.adptg.arquivos.pt/oai-pmh/#ADPTG",130188,
//	        "https://baimages.gulbenkian.pt/oai-pmh/oai/oai.aspx#IMD",51978,
//	        "https://arquivomunicipal3.cm-lisboa.pt/X-arqOAI/oai2.aspx#",50620,
//	        "http://biblioteca.tndm.pt/OAI/#CARTAZES",773,
//	        "https://projetos.dhlab.fcsh.unl.pt/oai#GTComenta",663,
//	        "https://medievalista.iem.fcsh.unl.pt/index.php/medievalista/oai#medievalista",249,
//	        "https://projetos.dhlab.fcsh.unl.pt/oai#memoriacovid
		
//		CommandLineDatasetExporter.main(new String[] {
//				"-solr_url_repository", "http://datarossio.dglab.gov.pt:8983/solr/testes-repositorio",
//				"-sources_file", "src/data/oai_sources_debug.ttl",		
////		"-sources_file", "src/data/oai_sources-test-harvest-short.ttl",		
//				"-export_folder", "C:\\Users\\nfrei\\Desktop\\ROSSIO_Exports\\dataset",
//				"-source_id", "https://arouca.fcsh.unl.pt/fontes/oai#",
////		"-sample_size", "100",
//		});
		
		
	}

}
