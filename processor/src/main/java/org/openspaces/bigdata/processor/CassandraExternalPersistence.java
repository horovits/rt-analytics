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

/**
 * Created by IntelliJ IDEA.
 * User: uri1803
 * Date: 1/21/12
 * Time: 9:36 PM
 * To change this template use File | Settings | File Templates.
 */
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
