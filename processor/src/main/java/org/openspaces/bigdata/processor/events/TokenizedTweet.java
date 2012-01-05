package org.openspaces.bigdata.processor.events;

import java.util.Map;

import com.gigaspaces.annotation.pojo.SpaceClass;
import com.gigaspaces.annotation.pojo.SpaceId;

/**
 * This class represents a tokenized version of a tweet.
 * 
 * @author Dotan Horovits
 *
 */
@SpaceClass
public class TokenizedTweet {
	
	private Long id = null;
	private Map<String, Integer> tokenMap = null;
	private Boolean filtered = null;
	
	public TokenizedTweet(Long id, Map<String,Integer> tokenMap) {
		this.id = id;
		this.tokenMap = tokenMap;
		filtered = false;
	}
	
	public TokenizedTweet() {}
	
	public Boolean getFiltered() {
		return filtered;
	}

	public void setFiltered(Boolean filtered) {
		this.filtered = filtered;
	}

	@SpaceId(autoGenerate=false)
	public Long getId() {
		return id;
	}
	public void setId(Long id) {
		this.id = id;
	}
	public Map<String, Integer> getTokenMap() {
		return tokenMap;
	}
	public void setTokenMap(Map<String, Integer> tokenMap) {
		this.tokenMap = tokenMap;
	}

	@Override
	public String toString() {
		return "TokenizedTweet [id=" + id + ", tokenMap=" + tokenMap + "]";
	}
	
}
