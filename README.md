Simple ETL for storing events generated from [eventsim](https://github.com/dinhhh/eventsim) into hadoop

# Start service guide

- Change directory to hadoop/sbin
```agsl
./start-dfs.sh
./start-yarn.sh
```

- Change directory to kafka folder

Update file connect-file-source.properties
```agsl
name=local-file-source
connector.class=FileStreamSource
tasks.max=1
file={eventsim_folder}/data/fake-rep.json
topic=eventsim
```

Update file connect-file-sink.properties
```agsl
name=local-file-sink
connector.class=FileStreamSink
tasks.max=1
file=fake.sink.txt
topics=eventsim
```

Keep config/zookeeper.properties and config/server.properties original and Start server via command line
```agsl
bin/zookeeper-server-start.sh config/zookeeper.properties
bin/kafka-server-start.sh config/server.properties
bin/connect-standalone.sh config/connect-standalone.properties config/connect-file-source.properties config/connect-file-sink.properties
```

- Run project with Java 8

- Clone [event simulator project](https://github.com/dinhhh/eventsim) to generate fake events

Use this command to generate events and append to data/fake-rep.json file, which is Kafka connect listen to
```agsl
bin/eventsim --config examples/example-config.json | tee -a data/fake-rep.json
```

To generate large number of events
```agsl
bin/eventsim --config examples/example-config.json --from 365 --nusers 1000 --growth-rate 0.30 | tee -a data/fake-rep.json
```

Go to Hadoop Overview Web interface to view created file

# Note
Export JAVA_HOME path
```agsl
export JAVA_HOME=/usr/lib/jvm/OpenJDK8U-jdk_x64_linux_hotspot_8u372b07/jdk8u372-b07
```

Delete all messages in topic
```agsl
bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic eventsim
```