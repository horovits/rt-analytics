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
 * A feeder bean that generates and feeds simulated tweets to a remote space periodically using scheduled task.
 * The feeder uses tweetTextList, a predefined Spring-injected list of tweet texts, 
 * and numberOfUsers for generating user ids. 
 * 
 * @author Dotan Horovits
 */
public class Feeder implements InitializingBean, DisposableBean {

    private static final int NUM_THREADS = 5;

	private Logger log= Logger.getLogger(this.getClass().getName());
    
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
        executorService = Executors.newScheduledThreadPool(NUM_THREADS);
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
                log.fine("--- FEEDER WROTE " + tweet);
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
