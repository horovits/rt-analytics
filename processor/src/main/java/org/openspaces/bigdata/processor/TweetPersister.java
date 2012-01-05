package org.openspaces.bigdata.processor;

import java.io.IOException;
import java.util.logging.Logger;

import javax.annotation.PostConstruct;

import org.openspaces.events.EventDriven;
import org.openspaces.events.EventTemplate;
import org.openspaces.events.TransactionalEvent;
import org.openspaces.events.adapter.SpaceDataEvent;
import org.openspaces.events.polling.Polling;
import org.openspaces.events.polling.ReceiveHandler;
import org.openspaces.events.polling.receive.MultiTakeReceiveOperationHandler;
import org.openspaces.events.polling.receive.ReceiveOperationHandler;

import com.gigaspaces.document.SpaceDocument;
import com.j_spaces.core.client.SQLQuery;

/**
 * This polling container processor removes processed tweets and persists it to an external data store
 * 
 * @author Dotan Horovits
 *
 */

@EventDriven
@Polling(gigaSpace = "gigaSpace", passArrayAsIs = true, concurrentConsumers = 2, maxConcurrentConsumers = 2, receiveTimeout = 10000)
@TransactionalEvent
public class TweetPersister {
	
    Logger log= Logger.getLogger(this.getClass().getName());

    private static final int BATCH_SIZE = 100; //TODO replace w/ Spring3 EL using @Value to inject property, instead of constant
    
    private FileExternalPersistence persister; //TODO replace w/ real persistence
	
	@PostConstruct
	void postConstruct() throws IOException {
		log.info("initializing connection to back-end persistence store");
		persister = new FileExternalPersistence(new java.io.File("tweetRepo.txt")); //TODO replace w/ real persistence
		
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


//    @EventTemplate
//    SpaceDocument processedTweet() {
//    	DocumentProperties properties = new DocumentProperties()
//    	.setProperty("Processed", true);
//    	SpaceDocument template = new SpaceDocument("Tweet", properties);
//    	return template;
//    }
    
    @EventTemplate
    SQLQuery<SpaceDocument> processedTweet() {
    	SQLQuery<SpaceDocument> query = 
    		new SQLQuery<SpaceDocument>("Tweet", "Processed = "+true);
    	return query;
    }

    @SpaceDataEvent
    public SpaceDocument[] eventListener(SpaceDocument[] tweetArray) {
    	
    	log.info("writing behind a bulk of "+tweetArray.length+" tweets to backend persistence store");

		try {
			persister.writeBulk(tweetArray); //TODO: remove when real persistence is set
		} catch (IOException e) {
			log.severe("error persisting tweet bulk: "+e.getMessage());
		}

		return null;
    }
    
}
