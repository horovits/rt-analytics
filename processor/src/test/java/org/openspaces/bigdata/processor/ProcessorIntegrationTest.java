package org.openspaces.bigdata.processor;

import org.openspaces.bigdata.common.Data;

import org.junit.runner.RunWith;
import org.junit.Before;
import org.junit.After;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.beans.factory.annotation.Autowired;

import org.openspaces.core.GigaSpace;


/**
 * Integration test for the Processor. Uses similar xml definition file (ProcessorIntegrationTest-context.xml)
 * to the actual pu.xml. Writs an unprocessed Data to the Space, and verifies that it has been processed by
 * taking a processed one from the space.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class ProcessorIntegrationTest {

    @Autowired
    GigaSpace gigaSpace;

    @Before
    @After
    public void clearSpace() {
        gigaSpace.clear(null);
    }

    @Test
    public void verifyProcessing() throws Exception {
        // write the data to be processed to the Space
        Data data = new Data(1, "test");
        gigaSpace.write(data);

        // create a template of the processed data (processed)
        Data template = new Data();
        template.setType(1l);
        template.setProcessed(true);

        // wait for the result
        Data result = gigaSpace.take(template, 500);
        // verify it
        assertNotNull("No data object was processed", result);
        assertEquals("Processed Flag is false, data was not processed", true, result.isProcessed());
        assertEquals("Processed text mismatch", "PROCESSED : " + data.getRawData(), result.getData());
    }
}
