import org.cloudifysource.dsl.context.ServiceContextFactory
import java.util.concurrent.TimeUnit
import groovy.util.ConfigSlurper;


config = new ConfigSlurper().parse(new File("cassandra.properties").toURL())




ip = InetAddress.localHost.hostAddress;
println "ip is:" + ip
seeds = "- ${ip}\n"

conf = "${config.home}/conf"
yaml = new File("${conf}/cassandra.yaml")
println "cassandra yaml location: " + yaml.getAbsolutePath()
yamltext = yaml.text
backup = new File("${conf}/cassandra.yaml_bak")
backup.write yamltext
//yamltext = yamltext.replaceAll("- 127.0.0.1\n", seeds)
//yamltext = yamltext.replaceAll("listen_address: localhost", "listen_address: " + ip)
//yamltext = yamltext.replaceAll("rpc_address: localhost", "rpc_address: 0.0.0.0")
yamltext = yamltext.replaceAll("/var/lib/cassandra/data", "../lib/cassandra/data")
yamltext = yamltext.replaceAll("/var/lib/cassandra/commitlog", "../lib/cassandra/commitlog")
yamltext = yamltext.replaceAll("/var/lib/cassandra/saved_caches", "../lib/cassandra/saved_caches")
yaml.write yamltext
println "wrote new yaml"
logprops = new File(conf + "/log4j-server.properties")
logpropstext = logprops.text
logpropstext = logpropstext.replaceAll("/var/log/cassandra/system.log", "../log/cassandra/system.log")
logprops.write logpropstext
