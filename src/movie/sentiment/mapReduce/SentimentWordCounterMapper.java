package movie.sentiment.mapReduce;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import Lucene.sentiment.lexicalParser.LuceneEnglishAnalyzer;
import Lucene.sentiment.lexicalParser.StopWordsReader;
import Lucene.sentiment.lexicalParser.NegativeWordsReader;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.lucene.analysis.TokenStream;
//import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;


public class SentimentWordCounterMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {     
      
	private Text word = new Text();
	private static Set<String> stopWords =  new StopWordsReader();
	private static Set<String> negativeWords =  new NegativeWordsReader();
	
	private final static IntWritable one = new IntWritable(1);
	private final static IntWritable minusone = new IntWritable(-1);
   	int pflag = 0;
   	int nflag = 0;
	
	private Map<String,String> wordsFromReview = null;
	//private int numUsefulVotes = 0;
	
    public void map(LongWritable key, Text value, 
    		 	OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
		String line = value.toString();
		int length = line.length();
		String movieid = line.substring(0,9);
		//System.out.println(movieid);
		String review = line.substring(10, length-1);
		//System.out.println(review);
				
		wordsFromReview = getParsedWords(review);
		//System.out.println(wordsFromReview);

		
		Iterator<Entry<String, String>> it = wordsFromReview.entrySet().iterator();
	    while (it.hasNext()) {
	        Map.Entry pairs = (Map.Entry)it.next();
	        //System.out.println(pairs.getKey() + " = " + pairs.getValue());
	        it.remove(); // avoids a ConcurrentModificationException
	        //word = (Text) pairs.getKey(); 
	        if (pairs.getValue() == "p"){
            output.collect(new Text (movieid.toString()), one);   
            //output.collect(new Text (pairs.getKey().toString()), one);
	        }
	        else if (pairs.getValue() == "n"){
	            output.collect(new Text (movieid.toString()), minusone);   
	           // output.collect(new Text (pairs.getKey().toString()), minusone);
	        }
	    }
    	       
        /*for(String wordText : wordsFromReview) {
            word.set(wordText);
            
            //if (pflag == 1)
            //{	
            output.collect(new Text (movieid.toString()), one);   
            output.collect(word, one);
            //pflag = 0;
            //}
           /* else if (nflag == 1)	
            {
            output.collect(new Text (movieid.toString()), minusone);   
            output.collect(word, minusone);
            nflag = 0;
            }*/
        //}
    }

    private Map<String,String> getParsedWords(String line) throws IOException {
    	Map<String, String> parsedWords = new TreeMap<String,String>();
    	
    	line = removeNonAlphanumericCharsFromLine(line);
    	
    	LuceneEnglishAnalyzer luceneAnalyzer = new LuceneEnglishAnalyzer(line);
        TokenStream tokenStream = luceneAnalyzer.getTokenStream();
        @SuppressWarnings("deprecation")
		TermAttribute termAttribute = luceneAnalyzer.getTermAttribute();
	    try {
			while (tokenStream.incrementToken()) {
			   	@SuppressWarnings("deprecation")
				String wordText = termAttribute.term();
			   	//System.out.println (wordText);

			    if(!isShortString(wordText) && stopWords.contains(wordText) && !isNumeric(wordText)) {
			    	parsedWords.put(wordText, "p");
			    	//System.out.println (" positive words");
			    	//pflag  = 1;
			    }
			    else if (!isShortString(wordText) && negativeWords.contains(wordText) && !isNumeric(wordText)) {
			    	parsedWords.put(wordText, "n");
			    	//System.out.println ("negative words");
			    	//nflag = 1;
			      
			     }	
			}
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(0);
		}
	    luceneAnalyzer.close();

    	return parsedWords;
    }    
    
	private String removeExtraWhiteSpaceFromTerm(String textLine) {
		textLine = textLine.replaceAll("\\s{1,}", " ");
		textLine = textLine.toLowerCase();
		return textLine;
	}
	
	private String removeNonAlphanumericCharsFromLine(String strLine) {
		strLine = strLine.replaceAll("[^A-Za-z0-9]", " ").trim();
		strLine = removeExtraWhiteSpaceFromTerm(strLine);
		return strLine;
	}
	
	private boolean isShortString(String word) {
		if (word.length() <= 2) {
			return true;
		} else {
			return false;
		}
	}
	
	private boolean isNumeric(String wordText) {  
	   try  {  
	      Integer.parseInt(wordText);  
	      return true;  
	   } catch(Exception e) {
	      return false;  
	   }  
	}
}