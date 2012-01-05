package org.openspaces.bigdata.processor;

import java.util.Map;
import java.util.logging.Logger;

import javax.annotation.Resource;

import org.openspaces.bigdata.processor.events.LocalCountBulk;
import org.openspaces.bigdata.processor.events.TokenizedTweet;
import org.openspaces.core.GigaSpace;
import org.openspaces.events.EventDriven;
import org.openspaces.events.EventTemplate;
import org.openspaces.events.TransactionalEvent;
import org.openspaces.events.adapter.SpaceDataEvent;
import org.openspaces.events.polling.Polling;
import org.openspaces.events.polling.ReceiveHandler;
import org.openspaces.events.polling.receive.MultiTakeReceiveOperationHandler;
import org.openspaces.events.polling.receive.ReceiveOperationHandler;

/**
 * This polling container processor performs token count on bulks of tokenized tweets
 * 
 * @author Dotan Horovits
 *
 */

@EventDriven
@Polling(gigaSpace = "gigaSpace", passArrayAsIs = true, concurrentConsumers = 1, maxConcurrentConsumers = 1, receiveTimeout = 5000)
@TransactionalEvent
public class LocalTokenCounter {

	private static final int LEASE_TTL = 5000;

	@Resource(name = "gigaSpace")
	GigaSpace gigaSpace;

    Logger log= Logger.getLogger(this.getClass().getName());

    private static final int BATCH_SIZE = 100;
	
	@javax.annotation.PostConstruct
	void postConstruct() {
		log.info(this.getClass().getName()+" initialized");
	}

	@ReceiveHandler 
    ReceiveOperationHandler receiveHandler() {
        MultiTakeReceiveOperationHandler receiveHandler = new MultiTakeReceiveOperationHandler();
        receiveHandler.setMaxEntries(BATCH_SIZE);
        receiveHandler.setNonBlocking(true); 
        receiveHandler.setNonBlockingFactor(1); 
        return receiveHandler;
    }


    @EventTemplate
    TokenizedTweet tokenizedFilteredTweet() {
    	TokenizedTweet template = new TokenizedTweet();
    	template.setFiltered(true);
    	return template;
    }
    
    @SpaceDataEvent
    public TokenizedTweet[] eventListener(TokenizedTweet[] tokenizedTweetArray) {

    	log.info("local counting of a bulk of "+tokenizedTweetArray.length+" tweets");
    	//Map<String, Integer> tokenMap = tokenizedTweetArray[0].getTokenMap();
    	Map<String, Integer> tokenMap = new java.util.HashMap<String, Integer>();
    	for (int i = 0; i < tokenizedTweetArray.length; i++) {
    		log.info("--"+tokenizedTweetArray[i]);
            for (Map.Entry<String, Integer> entry : tokenizedTweetArray[i].getTokenMap().entrySet()) {
            	String token = entry.getKey();
            	Integer count = entry.getValue();
            	log.finest("put token "+token+" with count "+(tokenMap.containsKey(token) ? tokenMap.get(token)+count : count));
            	tokenMap.put(token,  
            			(tokenMap.containsKey(token) ? tokenMap.get(token)+count : count));
            }
    	}
    	
    	log.info("--writing LocalCountBulk of size "+tokenMap.size());
    	gigaSpace.write(new LocalCountBulk(tokenMap),LEASE_TTL);

    	return null;
    }

}
