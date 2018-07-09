package it.deib.polimi.diaprivacy.library;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

import it.deib.polimi.diaprivacy.model.PrivacyContext;


public class PrivacyContextParser extends RichMapFunction<String, PrivacyContext> {

	@Override 
	public void open(Configuration conf) {
		
	}
	
	@Override
	public PrivacyContext map(String tuple) throws Exception {
		            String[] fields = tuple.split(",");
            return new PrivacyContext(
                    fields[0],
                    fields[1],
                    fields[2]
                    );
	}
	
}
