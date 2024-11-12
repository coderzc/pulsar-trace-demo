# pulsar-trace-demo

Compile and run the demo:

```bash
mvn clean package
```

```bash
java -javaagent:path/opentelemetry-javaagent-1.32.1.jar \
     -Dotel.resource.attributes=service.name=spring-pulsar \
     -Dotel.traces.exporter=otlp \
     -jar path/pulsar-trace-demo-0.0.1-SNAPSHOT.jar
```

