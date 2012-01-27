
application {
	name="rt_app"
	
	service {
		name = "feeder"
		dependsOn = ["processor"]
	}
	service {
		name = "processor"
		dependsOn = ["rt_cassandra"]		
	}
	service {
		name = "rt_cassandra"	
	}
	
	
}