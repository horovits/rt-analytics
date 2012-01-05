package org.openspaces.bigdata.processor;

import java.util.Map;

import org.junit.Test;

public class TweetParserTest {
	
	@Test
	public void testTokenize() {

		String text = "Twitter is an online social networking service and microblogging service that enables its users to send and read text-based posts of up to 140 characters, known as \"tweets\".";
		Map<String, Integer> tokenMap = TweetParser.tokenize(text);
		for (Map.Entry<String, Integer> e : tokenMap.entrySet())
		    System.out.println(e.getKey() + ": " + e.getValue());
	}
}
