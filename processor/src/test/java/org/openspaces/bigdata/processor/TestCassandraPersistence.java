package org.openspaces.bigdata.processor;

import com.gigaspaces.document.SpaceDocument;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Created by IntelliJ IDEA.
 * User: uri1803
 * Date: 1/21/12
 * Time: 11:55 PM
 * To change this template use File | Settings | File Templates.
 */


@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class TestCassandraPersistence {

    @Autowired
    private CassandraExternalPersistence persister;


    @Test
    public void testPersistence() {
        String id = String.valueOf(System.currentTimeMillis());
        System.out.println("id = " + id);
        persister.write(new SpaceDocument("Tweet")
                .setProperty("Id", id)
                .setProperty("Text", "text")
                .setProperty("CreatedAt", "now")
                .setProperty("FromUserId", "uri1803")
                .setProperty("ToUserId", "natishalom"));



    }

}
