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

import java.util.Formatter;
import java.util.List;
import java.util.logging.Logger;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

import org.openspaces.core.GigaSpace;
import org.springframework.dao.DataAccessException;
import org.springframework.social.twitter.api.Tweet;
import org.springframework.social.twitter.api.Twitter;
import org.springframework.social.twitter.api.impl.TwitterTemplate;

import com.gigaspaces.document.DocumentProperties;
import com.gigaspaces.document.SpaceDocument;

/**
 * A feeder bean that connects to Twitter, retrieves the public timeline tweets, 
 * converts them to standard Tweet SpaceDocument format, and writes them to the remote space. 
 *
 * @author Dotan Horovits
 */
public class TweetFeeder {

	private Logger log= Logger.getLogger(this.getClass().getName());

    @Resource
    private GigaSpace gigaSpace;
    
    private int delay = 1000; //milliseconds
	private int period = 1000; //milliseconds


	public int getDelay() {
		return delay;
	}

	public void setDelay(int delay) {
		this.delay = delay;
	}

	public int getPeriod() {
		return period;
	}

	public void setPeriod(int period) {
		this.period = period;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		new TweetFeeder().execute();

	}
	
	@PostConstruct
	public void execute() {
		new java.util.Timer().scheduleAtFixedRate(
				new java.util.TimerTask() {
					Formatter formatter = new Formatter();
					@Override
					public void run() {

						try {
							Twitter twitter = new TwitterTemplate(); 

							List<Tweet> tweets = twitter.timelineOperations().getPublicTimeline();

							for (Tweet tweet : tweets) {
								log.fine(formatter.format("Tweet id=%d\tfromUser=%s\ttext=%s \n",
										tweet.getId(), tweet.getFromUser(), tweet.getText()).toString());
//							System.out.format("Tweet id=%d\tfromUser=%s\ttext=%s \n",
//									tweet.getId(), tweet.getFromUser(), tweet.getText());
								gigaSpace.write(constructTweetDocument(tweet));
							}
						} catch (DataAccessException e) {
							log.severe("error feeding tweets: "+e.getMessage());
						}
					}
				}, 
				delay, 
				period);		
	}

    public SpaceDocument constructTweetDocument(Tweet tweet) {
        DocumentProperties properties = new DocumentProperties()
            .setProperty("Id", tweet.getId())
            .setProperty("Text", tweet.getText())
            .setProperty("CreatedAt", tweet.getCreatedAt())
            .setProperty("FromUserId", tweet.getFromUserId())
            .setProperty("ToUserId", tweet.getToUserId())
            .setProperty("Processed", Boolean.FALSE);
        SpaceDocument tweetDoc = new SpaceDocument("Tweet", properties);
        return tweetDoc;
    }

}
