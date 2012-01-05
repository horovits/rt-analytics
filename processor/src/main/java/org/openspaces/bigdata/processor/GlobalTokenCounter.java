package org.openspaces.bigdata.processor;

import java.util.Map;
import java.util.logging.Logger;

import javax.annotation.Resource;

import org.openspaces.bigdata.processor.events.LocalCountBulk;
import org.openspaces.core.GigaMap;
import org.openspaces.core.cluster.ClusterInfo;
import org.openspaces.core.cluster.ClusterInfoContext;
import org.openspaces.events.EventDriven;
import org.openspaces.events.EventTemplate;
import org.openspaces.events.TransactionalEvent;
import org.openspaces.events.adapter.SpaceDataEvent;
import org.openspaces.events.notify.Notify;
import org.openspaces.events.notify.NotifyType;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@EventDriven
@Notify(gigaSpace = "clusteredGigaSpace"/*, guaranteed = true*/)
@NotifyType(write = true)
@TransactionalEvent
public class GlobalTokenCounter {

    Logger log= Logger.getLogger(this.getClass().getName());

    @ClusterInfoContext
    private ClusterInfo clusterInfo;
    
    @Resource
    GigaMap gigaMap;

	@javax.annotation.PostConstruct
	void postConstruct() {
		log.info(this.getClass().getName()+" initialized");
	}

    @EventTemplate
    LocalCountBulk localCountBulk() {
    	LocalCountBulk template = new LocalCountBulk();
    	return template;
    }
    
    @SpaceDataEvent
    LocalCountBulk eventListener(LocalCountBulk bulk) {
        if (bulk.getTokenMap() == null) log.severe("LocalCountBulk w/ null tokenMap");
    	log.info("processing LocalCountBulk of bulk size "+bulk.getTokenMap().size());
    	for (Map.Entry<String, Integer> entry : bulk.getTokenMap().entrySet()) {
        	String token = entry.getKey();
        	Integer count = entry.getValue();
        	
        	if (isTokenLocal(token)) {
        		log.info("incementing local token "+token+" by "+count);
        		incrementLocalToken(token, count);
        	}
        }
        
        return null;
    }

	private boolean isTokenLocal(String token) {
		final int instanceIdZeroBased = clusterInfo.getInstanceId() - 1;
		return (token.hashCode() % clusterInfo.getNumberOfInstances() 
				== instanceIdZeroBased);
	}

    @Transactional(readOnly = false, propagation = Propagation.REQUIRES_NEW, isolation  = Isolation.READ_COMMITTED)
    private void incrementLocalToken(String token, Integer count) {
//		gigaMap.lock(token);
		Integer globalCount = gigaMap.containsKey(token) ? (Integer)gigaMap.get(token)+count : count;
		gigaMap.put(token, globalCount);
//		gigaMap.unlock(token);
	}

}
