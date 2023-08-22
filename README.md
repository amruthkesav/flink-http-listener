# flink-http-listener
An embedded HTTP Jetty listener as Flink Source Function. Suitable for applications that does not require fault tolerance and recovery.

# Quick start

1. Install Zookeeper
```
brew install zookeeper
zkServer start
```
2. Use ZkCli and run some test commands `zkCli`
```
create /test ""
ls /test
getAcl /test
# getAcl should return world:anyone:cdrwa 
```
3. Open the project in your fav IDE, build it and Run `ExampleApp`. This will start a Flink job with a HttpSourceFunction listening at port 8090. 
4. The above should register the coordinates in ZK. Use `zkCli` and check
```
ls /test/servers
```
5. You should see `localhost:8090:00000...` with the above command
6. Run the `HttpClient` now (Keep the Flink job running). The HttpClient will discover the http coordinates and publish few strings to the source function.
7. The Flink job would have printed some stuff in console with 'Hello' prepended for every message
