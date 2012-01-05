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
 * This polling container processor filters out non-informative tokens, such as prepositions
 * 
 * @author Dotan Horovits
 *
 */
@EventDriven
@Polling(gigaSpace = "gigaSpace", concurrentConsumers = 1, maxConcurrentConsumers = 1, receiveTimeout = 5000)
@TransactionalEvent
public class TokenFilter {

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
	TokenizedTweet tokenizedNonFilteredTweet() {
		TokenizedTweet template = new TokenizedTweet();
		template.setFiltered(false);
		return template;
	}

	@SpaceDataEvent
	public TokenizedTweet eventListener(TokenizedTweet tokenizedTweet) {
		log.info("filtering tweet "+tokenizedTweet.getId());
		Map<String, Integer> tokenMap = tokenizedTweet.getTokenMap();
		int numTokensBefore = tokenMap.size();
		for (String token : tokenMap.keySet()) {
			if (isTokenRequireFilter(token))
				tokenMap.remove(token);
		}
		int numTokensAfter = tokenMap.size();
		tokenizedTweet.setFiltered(true);
		log.info("filtered out "+(numTokensBefore-numTokensAfter)+" tokens from tweet "+tokenizedTweet.getId());
		return tokenizedTweet;
	}

	private boolean isTokenRequireFilter(String token) {
		// TODO implement filtering logic
		return false;
	}

}
