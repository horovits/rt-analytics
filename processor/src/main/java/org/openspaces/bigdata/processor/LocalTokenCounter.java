/*
* Copyright (c) 2012 GigaSpaces Technologies Ltd. All rights reserved
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*      http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.openspaces.bigdata.processor;

import java.util.Map;
import java.util.logging.Logger;

import javax.annotation.Resource;

import org.openspaces.bigdata.processor.events.TokenCounter;
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

/**
 * Event polling container processor filters out non-informative tokens, such as prepositions, 
 * from filtered {@link TokenizedTweet} instances.
 * 
 * @author Dotan Horovits
 */
@EventDriven
@Polling(gigaSpace = "gigaSpace", passArrayAsIs = true, concurrentConsumers = 1, maxConcurrentConsumers = 1, receiveTimeout = 10000)
@TransactionalEvent(timeout=1000)
public class LocalTokenCounter {

	private static final int LEASE_TTL = 5000;

	@Resource(name = "clusteredGigaSpace")
	GigaSpace clusteredGigaSpace;

	@Resource(name = "gigaSpace")
	GigaSpace gigaSpace;

    Logger log= Logger.getLogger(this.getClass().getName());

    private static final int BATCH_SIZE = 5;
	
	@ReceiveHandler 
    ReceiveOperationHandler receiveHandler() {
        MultiTakeReceiveOperationHandler receiveHandler = new MultiTakeReceiveOperationHandler();
        receiveHandler.setMaxEntries(BATCH_SIZE);
        receiveHandler.setNonBlocking(true);
        receiveHandler.setNonBlockingFactor(1); 
        return receiveHandler;
    }


	/**
	 * This method returns the template of a filtered {@link TokenizedTweet}. 
	 * @return template for the event container
	 */
    @EventTemplate
    TokenizedTweet tokenizedFilteredTweet() {
    	TokenizedTweet template = new TokenizedTweet();
    	template.setFiltered(true);
    	return template;
    }
    
    /**
	 * Event handler that takes a bulk of {@link TokenizedTweet}, counts appearances of tokens in the bulk,
	 * and generates a corresponding {@link TokenCounter} for each token.
     * @param tokenizedTweetArray array of {@link TokenizedTweet} matching the event template
     */
    @SpaceDataEvent
    public void eventListener(TokenizedTweet[] tokenizedTweetArray) {

    	log.info("local counting of a bulk of "+tokenizedTweetArray.length+" tweets");
    	Map<String, Integer> tokenMap = new java.util.HashMap<String, Integer>();
    	for (int i = 0; i < tokenizedTweetArray.length; i++) {
    		log.fine("--processing "+tokenizedTweetArray[i]);
            for (Map.Entry<String, Integer> entry : tokenizedTweetArray[i].getTokenMap().entrySet()) {
            	String token = entry.getKey();
            	Integer count = entry.getValue();
            	log.finest("put token "+token+" with count "+(tokenMap.containsKey(token) ? tokenMap.get(token)+count : count));
            	tokenMap.put(token,  
            			(tokenMap.containsKey(token) ? tokenMap.get(token)+count : count));
            }
    	}

        TokenCounter[] counterArray = new TokenCounter[tokenMap.size()];
    	int i = 0;
        for (Map.Entry<String, Integer> entry : tokenMap.entrySet()) {
        	String token = entry.getKey();
        	Integer count = entry.getValue();
        	TokenCounter tokenCounter = new TokenCounter(token, count);
        	log.finest("new TokenCounter: token="+tokenCounter.getToken()+", count="+tokenCounter.getCount());
			counterArray[i++] = tokenCounter;
        }

    	clusteredGigaSpace.writeMultiple(counterArray,LEASE_TTL);

    }

}
