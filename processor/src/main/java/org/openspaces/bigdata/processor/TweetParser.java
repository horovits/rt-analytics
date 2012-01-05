package org.openspaces.bigdata.processor;

import java.util.Map;
import java.util.StringTokenizer;
import java.util.logging.Logger;

import javax.annotation.Resource;

import org.openspaces.bigdata.processor.events.TokenizedTweet;
import org.openspaces.core.GigaSpace;
import org.openspaces.events.EventDriven;
import org.openspaces.events.EventTemplate;
import org.openspaces.events.TransactionalEvent;
import org.openspaces.events.adapter.SpaceDataEvent;
import org.openspaces.events.polling.Polling;
import org.springframework.transaction.annotation.Isolation;

import com.gigaspaces.document.DocumentProperties;
import com.gigaspaces.document.SpaceDocument;
import com.j_spaces.core.client.SQLQuery;

/**
 * This polling container processor parses raw tweets, generating TokenizedTweets
 * 
 * @author Dotan Horovits
 *
 */

@EventDriven
@Polling(gigaSpace = "gigaSpace", concurrentConsumers = 2, maxConcurrentConsumers = 2)
@TransactionalEvent(timeout = 100)
public class TweetParser {
	
	@Resource(name = "clusteredGigaSpace")
	GigaSpace clusteredGigaSpace;

	@Resource(name = "gigaSpace")
	GigaSpace gigaSpace;

    Logger log= Logger.getLogger(this.getClass().getName());

//    @EventTemplate
//    SpaceDocument unprocessedTweet() {
//    	DocumentProperties properties = new DocumentProperties()
//    	.setProperty("Processed", false);
//    	SpaceDocument template = new SpaceDocument("Tweet", properties);
//    	return template;
//    }

    @EventTemplate
    SQLQuery<SpaceDocument> unprocessedTweet() {
    	SQLQuery<SpaceDocument> query = 
    		new SQLQuery<SpaceDocument>("Tweet", "Processed = "+false);
    	return query;
    }

    @SpaceDataEvent
    public SpaceDocument eventListener(SpaceDocument tweet) {
    	log.info("parsing tweet "+tweet);

    	Long id = (Long)tweet.getProperty("Id");
    	String text = tweet.getProperty("Text");
    	if (text != null) {
    		Map<String, Integer> tokenMap = tokenize(text);
    		TokenizedTweet tt = new TokenizedTweet(id,tokenMap);
    		gigaSpace.write(tt);
    	}
    	
    	tweet.setProperty("Processed", true);
    	return tweet;
    }

    public static Map<String, Integer> tokenize(String text) {
    	Map<String, Integer> tokenMap = new java.util.HashMap<String, Integer>();
    	StringTokenizer st = new StringTokenizer(text);

    	while(st.hasMoreTokens()) { 
    		String token = st.nextToken(); 
        	Integer count = tokenMap.get(token);
        	count = (count == null? 1 : count+1);
        	tokenMap.put(token, count);
    	}
    	return tokenMap;
    }
    

}
