package rossio.ingest.preprocess;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.jena.rdf.model.Property;

import rossio.data.models.DcTerms;

public class CsvMapping {
	Map<Integer, Property> mapping=new HashMap<Integer, Property>();
	
	public CsvMapping(String resourceName, boolean hasHeaderRow) throws IOException {
		this(new InputStreamReader(CsvMapping.class.getClassLoader().getResourceAsStream(resourceName)), hasHeaderRow);
	}
	
	public CsvMapping(Reader mappingDefinition, boolean hasHeaderRow) throws IOException {
		CSVParser parser=new CSVParser(mappingDefinition, CSVFormat.DEFAULT);
		boolean first=true;
		for(CSVRecord rec: parser) {
			if(first) {
				first=false;
				if(hasHeaderRow) continue;
			}
			int column=Integer.parseInt(rec.get(0));
			String propName=rec.get(2);
			Property prop=DcTerms.fromName(propName);
			mapping.put(column, prop);
		}
		mappingDefinition.close();
	}
	
	public Property getMappingForColumn(int col) {
		return mapping.get(col);
	}
	
}
