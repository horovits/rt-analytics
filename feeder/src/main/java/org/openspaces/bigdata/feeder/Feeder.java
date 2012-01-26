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

import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import javax.annotation.PostConstruct;

import org.openspaces.core.GigaSpace;
import org.openspaces.core.SpaceInterruptedException;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import com.gigaspaces.document.DocumentProperties;
import com.gigaspaces.document.SpaceDocument;

/**
 * A feeder bean that generates and feeds simulated tweets to a remote space periodically using scheduled task.
 * <p>
 * The feeder uses tweetTextList, a predefined Spring-injected list of tweet texts, and numberOfUsers for generating user ids.
 * 
 * @author Dotan Horovits
 */
public class Feeder implements DisposableBean {
    private Logger log = Logger.getLogger(Feeder.class.getSimpleName());
    private static final int NUM_THREADS = 5;

    private ScheduledExecutorService executorService = Executors.newScheduledThreadPool(NUM_THREADS);

    private ScheduledFuture<?> sf;

    @Value("${tweet.delayInMs:1000}")
    private long numberOfUsers = 10;

    private long defaultDelay = 1000;

    private FeederTask feederTask = new FeederTask();

    @Autowired(required = true)
    private List<String> tweetTextList;

    @Autowired
    private GigaSpace gigaSpace;

    @PostConstruct
    void onPostConstruct() throws Exception {
        log.info("tweet list size: " + tweetTextList.size());
        log.info("--- STARTING FEEDER WITH CYCLE [" + defaultDelay + "]");
        sf = executorService.scheduleAtFixedRate(feederTask, defaultDelay, defaultDelay, TimeUnit.MILLISECONDS);
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
                long toUserId = randomGenerator.nextInt((int) numberOfUsers), fromUserId = randomGenerator.nextInt((int) numberOfUsers);
                SpaceDocument tweet = constructTweet(counter++, tweetTextList.get(randomGenerator.nextInt(tweetTextList.size())), new Date(), toUserId,
                        fromUserId, false);
                gigaSpace.write(tweet);
                log.fine("--- FEEDER WROTE " + tweet);
            } catch (SpaceInterruptedException ignore) {
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
        return new SpaceDocument("Tweet", new DocumentProperties() //
                .setProperty("Id", id) //
                .setProperty("Text", text) //
                .setProperty("CreatedAt", createdAt) //
                .setProperty("FromUserId", fromUserId) //
                .setProperty("ToUserId", toUserId) //
                .setProperty("Processed", processed));
    }

}
