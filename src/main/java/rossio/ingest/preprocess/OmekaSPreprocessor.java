package rossio.ingest.preprocess;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Seq;
import org.apache.jena.rdf.model.Statement;
import org.htmlcleaner.HtmlCleaner;
import org.w3c.dom.Element;

import rossio.data.models.DcTerms;
import rossio.data.models.Rossio;
import rossio.ingest.solr.RossioRecord;
import rossio.util.RdfUtil;

public class OmekaSPreprocessor implements MetadataPreprocessor {
  static Pattern apiUrlPattern = Pattern.compile("(https?://[^/]+/)api/items/(\\d+)");

  @Override
  public Model preprocess(String uuid, String sourceId, String dataProviderUri, Element metadata) {
    Model model = RossioRecord.fromOaidcToRossio(uuid, sourceId, dataProviderUri, metadata);
    String providedChoUri = Rossio.NS_ITEM + uuid;
    Resource cho = model.createResource(providedChoUri);

    for (Statement st : cho.listProperties(DcTerms.identifier).toList()) {
      if (st.getObject().isLiteral()) {
        Matcher m = apiUrlPattern.matcher(st.getObject().asLiteral().getString());
        if (m.matches()) {
          String newValue = m.group(1) + "s/"+sourceId.substring(sourceId.lastIndexOf('#') + 1) + "/item/" + m.group(2);
          st.changeObject(model.createLiteral(newValue));
        }
      } else if (st.getObject().isResource() && RdfUtil.isSeq(st.getObject().asResource())) {
        Seq seq = RdfUtil.getAsSeq(st.getObject().asResource());
        for (int i = 1; i <= seq.size(); i++) {
          RDFNode node = seq.getObject(i);
          if (node.isLiteral()) {
            Matcher m = apiUrlPattern.matcher(node.asLiteral().getString());
            if (m.matches()) {
              String newValue = m.group(1) + "s/"+sourceId.substring(sourceId.lastIndexOf('#') + 1) + "/item/" + m.group(2);
              st.changeObject(model.createLiteral(newValue));
              seq.set(i, model.createLiteral(newValue));
            }
          }
        }
      }
    }
    return model;
  }

  
  
  @Override
  public Model preprocess(String uuid, String sourceId, String dataProviderUri, CSVRecord metadata) {
    throw new NotImplementedException();
  }

  public static void main(String[] args) throws Exception {
    String test = "https://projetos.dhlab.fcsh.unl.pt/api/items/54397";
    Matcher m = apiUrlPattern.matcher(test);
    if (m.matches()) {
      String newValue = m.group(1) + "s/"+"wsdroadmap" + "/item/" + m.group(2);
      System.out.println(newValue);
    }else
      System.out.println("No match");
  }
}
