package Lucene.sentiment.lexicalParser;

import java.util.TreeSet;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;



@SuppressWarnings("serial")
public class NegativeWordsReader extends TreeSet<String> {

	public NegativeWordsReader() {
		super();
		addEnglishGrammaWords();
		convertNegativeWordsToLucene();
	}
	
	private void addEnglishGrammaWords() {		
		NegativeWords NegativeWords = new NegativeWords();
		this.addAll(NegativeWords);
	}
	
	private void convertNegativeWordsToLucene() {
        try {
    		String wordsTogether = getStringFromArray(this.toArray());
    		LuceneEnglishAnalyzer luceneAnalyzer = new LuceneEnglishAnalyzer(wordsTogether);			
            TokenStream tokenStream = luceneAnalyzer.getTokenStream();
            TermAttribute termAttribute = luceneAnalyzer.getTermAttribute();
    	    this.clear();
        	while (tokenStream.incrementToken()) {
			   	this.add(termAttribute.term());
			}
			luceneAnalyzer.close();
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(0);
		}
	}
	
	private String getStringFromArray(Object[] terms) {
		String termsTogether = "";
		for(Object term : terms) {
			termsTogether = termsTogether.concat((String)term).concat(" ");
		}
		return termsTogether;
	}

}