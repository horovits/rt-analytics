package org.openspaces.bigdata.processor.events;

import java.util.Map;

import com.gigaspaces.annotation.pojo.SpaceClass;
import com.gigaspaces.annotation.pojo.SpaceId;
import com.gigaspaces.annotation.pojo.SpaceProperty;
import com.gigaspaces.annotation.pojo.SpaceRouting;

/**
 * representation of token count on a bulk of local tweets, namely in same partition
 * 
 * @author Dotan Horovits
 *
 */
@SpaceClass
public class LocalCountBulk implements java.io.Serializable {
	
	private static final long serialVersionUID = 1L;

	private String id;
	
	private Map<String, Integer> tokenMap = null;
	
	public LocalCountBulk() {}
	
	public LocalCountBulk(Map<String, Integer> tokenMap) {
		this.tokenMap = tokenMap;
	}

	@SpaceId(autoGenerate=true)
	@SpaceRouting
	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}
	
	public void setTokenMap(Map<String, Integer> tokenMap) {
		this.tokenMap = tokenMap;
	}

	public Map<String, Integer> getTokenMap() {
		return tokenMap;
	}
	
}
