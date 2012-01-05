package org.openspaces.bigdata.feeder;

import org.openspaces.core.GigaSpace;
import org.openspaces.core.SpaceInterruptedException;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;

import com.gigaspaces.document.DocumentProperties;
import com.gigaspaces.document.SpaceDocument;

import java.util.Date;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import javax.annotation.Resource;

/**
 * A feeder bean starts a scheduled task that writes a new Data objects to the space
 * (in an unprocessed state).
 *
 * <p>The space is injected into this bean using OpenSpaces support for @GigaSpaceContext
 * annotation.
 *
 * <p>The scheduling uses the java.util.concurrent Scheduled Executor Service. It
 * is started and stopped based on Spring life cycle events.
 *
 * @author Dotan Horovits
 */
public class Feeder implements InitializingBean, DisposableBean {

    Logger log= Logger.getLogger(this.getClass().getName());
    
    private ScheduledExecutorService executorService;

    private ScheduledFuture<?> sf;

    private long numberOfUsers = 10;

    private long defaultDelay = 1000;

    private FeederTask feederTask;
    
    @Resource
    private java.util.List<String> tweetTextList;
    
    @Resource
    private GigaSpace gigaSpace;

    /**
     * Sets the number of users that will be used to simulate interacting users.
     */
    public void setNumberOfUsers(long numberOfTypes) {
        this.numberOfUsers = numberOfTypes;
    }

    public void setDefaultDelay(long defaultDelay) {
        this.defaultDelay = defaultDelay;
    }

    public void afterPropertiesSet() throws Exception {
    	assert tweetTextList != null;
    	log.info("tweet list size: " +tweetTextList.size());
        log.info("--- STARTING FEEDER WITH CYCLE [" + defaultDelay + "]");
        executorService = Executors.newScheduledThreadPool(1);
        feederTask = new FeederTask();
        sf = executorService.scheduleAtFixedRate(feederTask, defaultDelay, defaultDelay,
                TimeUnit.MILLISECONDS);
    }

    public void destroy() throws Exception {
        sf.cancel(false);
        sf = null;
        executorService.shutdown();
    }
    
    public long getFeedCount() {
        return feederTask.getCounter();
    }

    
    public class FeederTask implements Runnable {

        private long counter = 1;

        private Random randomGenerator = new Random();

        public void run() {
            try {
                long toUserId = randomGenerator.nextInt((int)numberOfUsers), fromUserId  = randomGenerator.nextInt((int)numberOfUsers);
                SpaceDocument tweet = constructTweet(counter++, tweetTextList.get(randomGenerator.nextInt(tweetTextList.size())), new Date(), toUserId, fromUserId, false);
                gigaSpace.write(tweet);
                log.info("--- FEEDER WROTE " + tweet);
            } catch (SpaceInterruptedException e) {
                // ignore, we are being shutdown
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        public long getCounter() {
            return counter;
        }
    }
    
    public SpaceDocument constructTweet(long id, String text, Date createdAt, long toUserId, long fromUserId, boolean processed) {
        DocumentProperties properties = new DocumentProperties()
            .setProperty("Id", id)
            .setProperty("Text", text)
            .setProperty("CreatedAt", createdAt)
            .setProperty("FromUserId", fromUserId)
            .setProperty("ToUserId", toUserId)
            .setProperty("Processed", processed);
        SpaceDocument tweet = new SpaceDocument("Tweet", properties);
        return tweet;
    }

    
}
