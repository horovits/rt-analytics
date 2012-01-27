service {
  icon "icon.png"
  name "feeder"
  numInstances 1
  statelessProcessingUnit {	
    binaries "rt-analytics-feeder.jar"    
    sla {
      memoryCapacity 8
      maxMemoryCapacity 8
      highlyAvailable false
      memoryCapacityPerContainer 8 
    }
  }	
}