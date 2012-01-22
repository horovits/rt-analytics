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

/**
 * @author Uri Cohen
 */
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
