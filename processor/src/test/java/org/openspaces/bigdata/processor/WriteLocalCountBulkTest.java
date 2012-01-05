package org.openspaces.bigdata.processor;

import java.util.Map;
import java.util.logging.Logger;

import org.junit.After;
import org.junit.Test;
import org.junit.Before;
import org.junit.runner.*;
import static org.junit.Assert.*;
import org.openspaces.bigdata.processor.events.TokenCounter;
import org.openspaces.core.GigaSpace;
import org.openspaces.core.GigaSpaceConfigurer;
import org.openspaces.core.space.UrlSpaceConfigurer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.j_spaces.core.IJSpace;

//@RunWith(SpringJUnit4ClassRunner.class)
//@ContextConfiguration(locations = "classpath:META-INF/spring/pu.xml")
public class WriteLocalCountBulkTest {
    Logger log= Logger.getLogger(this.getClass().getName());

    //@Autowired
    GigaSpace gigaSpace;

    @Before
    public void setupSpace() {
    	IJSpace space = new UrlSpaceConfigurer("/./testSpace").space();
    	gigaSpace = new GigaSpaceConfigurer(space).gigaSpace();
    }
    
    @After
    public void clearSpace() {
        gigaSpace.clear(null);
    }

    @Test
    public void testWriteLocalCountBulk() {
    	Map<String, Integer> tokenMap = new java.util.HashMap<String, Integer>();
    	gigaSpace.write(new TokenCounter("foo", 3));
    	gigaSpace.write(new TokenCounter("bar", 8));

       	log.info("reading TokenCounter");
       	TokenCounter template = new TokenCounter();
       	TokenCounter[] ret = gigaSpace.readMultiple(template);
    	
    	assertNotNull(ret);
    	assertEquals(2, ret.length);
    	assertNotNull(ret[0].getToken());
    	assertNotNull(ret[1].getToken());
  	
    }
	
}
