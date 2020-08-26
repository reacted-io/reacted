# ReActed

Version 0.1

##Core Features

- Actor based reactive parallel programming
- React by message type support (virtual bus sniffing) dynamically configurable
- Replay local(logged) session
- ZooKeeper integration for service discovery
- Seamless communication with remote reactors (belonging to other reactor systems, also remote)
- Local and remote message delivery guarantee.
- Automatically load balanced services (per node)
- Centralized logging support
- Chronicle Queue local and remote driver (session recording a message exchange with another ReActor system)
- Grpc remote driver
- Kafka remote driver
- Ask pattern support
- Local/Remote backpressured stream publisher

##TO DO
- ReActed streams language
- Dynamic diff during replay
- Eureka driver
- Nomad driver
- REST driver
- Rest API gateway
- Statistics/Monitoring over rest -> micrometer?
- Istio/Prometheus driver?