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
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import org.springframework.beans.factory.annotation.Value;

import javax.annotation.PostConstruct;
import java.util.logging.Level;
import java.util.logging.Logger;

public class CassandraExternalPersistence implements ExternalPersistence {
    private final static StringSerializer stringSerializer = StringSerializer.get();

    private final Logger log = Logger.getLogger(getClass().getName());
    private Cluster cluster;

    @Value("${cassandra.keyspace}")
    private String keyspaceName;

    @Value("${cassandra.cf}")
    private String columnFamily;

    @Value("${cassandra.host}")
    private String host = "localhost";

    @Value("${cassandra.port}")
    private int port = 9160;

    private Keyspace keyspace;


    @PostConstruct
    public void init() throws Exception {
        cluster = HFactory.getOrCreateCluster(keyspaceName, host + ":" + port);
        KeyspaceDefinition keyspaceDefinition = cluster.describeKeyspace(keyspaceName);
//        if (keyspaceDefinition == null) {
//            createSchema(cluster);
//        }
        keyspace = HFactory.createKeyspace(keyspaceName, cluster);
    }

    public void write(Object data) {

        HFactory.createMutator(keyspace, new StringSerializer());

        if (!(data instanceof SpaceDocument)) {
            log.log(Level.WARNING, "Received non document event");
            return;
        }
        SpaceDocument document = (SpaceDocument) data;


        Mutator<String> mutator = HFactory.createMutator(keyspace, stringSerializer);
        String uid = document.getProperty("Id");
        for (String key : document.getProperties().keySet()) {
            Object value = document.getProperty(key);
            if (value != null) {
                mutator.addInsertion(uid, columnFamily, HFactory.createColumn(key, value.toString(), stringSerializer,stringSerializer));
            }
        }
        mutator.execute();
    }

    public void writeBulk(Object[] dataArray) {
        for (Object o : dataArray) {
            write(o);
        }
    }
}
