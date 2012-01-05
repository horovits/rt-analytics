package org.openspaces.bigdata.processor.events;

import com.gigaspaces.annotation.pojo.SpaceClass;
import com.gigaspaces.annotation.pojo.SpaceId;
import com.gigaspaces.annotation.pojo.SpaceRouting;

@SpaceClass
public class TokenCounter {
	
	private String token;
	private Integer count;
	
	public TokenCounter() {}
	
	public TokenCounter(String token, Integer count) {
		this.token = token;
		this.count = count;
	}

	@SpaceId(autoGenerate=false)
	@SpaceRouting
	public String getToken() {
		return token;
	}

	public void setToken(String token) {
		this.token = token;
	}

	public Integer getCount() {
		return count;
	}

	public void setCount(Integer count) {
		this.count = count;
	}
	
}
