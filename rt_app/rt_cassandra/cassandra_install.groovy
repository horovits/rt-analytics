import java.util.concurrent.TimeUnit
import groovy.util.ConfigSlurper

config = new ConfigSlurper().parse(new File("cassandra.properties").toURL())

new AntBuilder().sequential {
	mkdir(dir:config.installDir)
	get(src:config.downloadPath, dest:"${config.installDir}/${config.zipName}", skipexisting:true)
	untar(src:"${config.installDir}/${config.zipName}", dest:config.installDir, compression:"gzip")
	chmod(dir:"${config.home}/bin", perm:'+x', excludes:"*.bat")
}	

