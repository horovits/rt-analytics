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

import static java.lang.Runtime.getRuntime;
import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.Executors.newScheduledThreadPool;

import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Resource;

import org.openspaces.core.GigaSpace;
import org.openspaces.core.SpaceInterruptedException;
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
public class Feeder {
    private Logger log = Logger.getLogger(Feeder.class.getSimpleName());

    private static final int NUM_THREADS = getRuntime().availableProcessors() * 2;

    private ScheduledExecutorService executorService = newScheduledThreadPool(NUM_THREADS);

    private ScheduledFuture<?> sf;

    @Value("${tweet.numberOfUsers:10}")
    private int numberOfUsers = 10;

    @Value("${tweet.delayInMs:1000}")
    private int defaultDelay = 1000;

    private FeederTask feederTask = new FeederTask();

    @Resource
    private List<String> tweetTextList;

    @Resource
    private GigaSpace gigaSpace;

    @PostConstruct
    void onPostConstruct() throws Exception {
        log.info("tweet list size: " + tweetTextList.size());
        log.info("--- STARTING FEEDER WITH CYCLE [" + defaultDelay + "]");
        sf = executorService.scheduleAtFixedRate(feederTask, defaultDelay, defaultDelay, TimeUnit.MILLISECONDS);
    }

    @PreDestroy
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
                SpaceDocument tweet = buildRandomTweet();
                gigaSpace.write(tweet);
                log.fine("--- FEEDER WROTE " + tweet);
            } catch (SpaceInterruptedException e) {
                log.fine("We are being shutdown " + e.getMessage());
            } catch (Exception e) {
                log.warning(e.getMessage());
            }
        }

        private SpaceDocument buildRandomTweet() {
            String randomTweet = tweetTextList.get(randomGenerator.nextInt(tweetTextList.size()));
            return buildTweet(counter++ //
                    , randomTweet //
                    , currentTimeMillis() //
                    , randomGenerator.nextInt(numberOfUsers) //
                    , randomGenerator.nextInt(numberOfUsers));
        }

        public long getCounter() {
            return counter;
        }
    }

    public SpaceDocument buildTweet(long id, String text, long createdAt, long toUserId, long fromUserId) {
        return new SpaceDocument("Tweet", new DocumentProperties() //
                .setProperty("Id", id) //
                .setProperty("Text", text) //
                .setProperty("CreatedAt", new Date(createdAt)) //
                .setProperty("FromUserId", fromUserId) //
                .setProperty("ToUserId", toUserId) //
                .setProperty("Processed", false));
    }

}
