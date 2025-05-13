# pulsar-trace-demo

Package and run the demo:

Compile and Package 
```bash
mvn clean package-DskipTests
```

Run server
```bash
java -javaagent:path/opentelemetry-javaagent-1.32.1.jar \
     -Dotel.resource.attributes=service.name=spring-pulsar \
     -Dotel.traces.exporter=otlp \
     -jar path/pulsar-trace-demo-0.0.1-SNAPSHOT.jar
```

Run client to request the server
```bash
mvn test -Dtest=TestGrpcClient
```