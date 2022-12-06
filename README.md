![ReActed Logo](https://github.com/reacted-io/reacted/blob/master/doc/artwork/logo.png)

<p align="center">
<a href="https://www.reacted.io">Reactive Actor framework for flexible/distributed/scalable and monitorable applications</a>
</p>

# Introduction
 ReActed is an actor oriented framework for creating reactive applications. It naturally embraces the microcomponents
 paradigm in its design, allowing the realization of distributed, flexible, reliable, monitorable and automatically scalable
 applications out of the box. ReActed amplifies the productivity of developers providing a ready to use solution that can be
 easily integrated with almost any other technology and fine-grained tuned only if required.
 
 ReActed can manage for you:
 
 - Parallelization: leveraging the actor paradigm, reactor can manage for you parallelization, saving the programmer the
 effort to write and maintain concurrent code. Synchronization or memory visibility issues problems are structurally removed
 from your code
 
 - Distribution: a ReActed instance can naturally and automatically create a cluster with other instances. Resources can
 be shared, used, added or created through a location agnostic approach, creating an automatically scalable and location
 agnostic way of designing a system
 
 - Monitoring: creating an event source or a choreography oriented microservices system can be tricky,
  but monitoring and debugging it can be even more difficult. ReActed can do this for you through its Re-playable Actors. 
  Run the application and if something goes wrong you can just analyze the log or replay the logs to reset the system
  to its state in any given instant in time.
  
 - Flexibility: in a real case scenario it's hard being able to choose from scratch all the technologies of your stack
 or even being able to use a technology for all the parts of your infrastructure. Thanks to ReActed driver system,
 different services or different part of the infrastructure can talk with each other using different technologies.
 Do you need the speed gRPC? Done. Somewhere else do you need Kafka? Done. Do you need both at the same time in different
 part of the system? Done as well and thanks to the location agnostic paradigm, you do not need to care about which 
 technology should be used to communicate with a given service, ReActed can do that for you.  
  
ReActed provides you a flexible and dependable environment that allows the developer to focus on producing business logic.

## Getting Started

If you want to jump into the code, you can give a look to the [examples](https://github.com/reacted-io/reacted/blob/master/examples) directory, otherwise give a 
look to the quickstart guide in the official documentation

# Benchmarks

Test: Reaction to a message latency time. Sender and receiver are executed in two different threads

CPU: Apple M1 Pro

Test scenario 1 
* Message Number: 10_000_000
* Message Send Rate: 1 every 10us

|  Percentile  |      Latency |
|:------------:|-------------:|
|      70      |        417ns |
|      75      |        417ns |
|      80      |        458ns |
|      85      |        458ns |
 |      90      |        541ns |
|      95      |      1.458us |
|      99      |      1.917us |
|     99.9     |          8us |
|    99.99     |         32us |
|   99.9999    |  1.0580425ms |
|     100      |  1.6896254ms |
 ------------------------------

Test scenario 2
* Message Number: 10_000_000
* Message Send Rate: 1 every 5us
* Message business payload: 8 bytes

| Percentile | Unbounded Mailbox Latency | Fast Unbounded Mailbox Latency |
|:----------:|--------------------------:|-------------------------------:|
|     70     |                     417ns |                          417ns |
|     75     |                     417ns |                          458ns |
|     80     |                     500ns |                          458ns |
|     85     |                   1.416us |                          459ns |
|     90     |                   1.541us |                        1.333us |
|     95     |                   1.625us |                        1.542us |
|     99     |                       2us |                        2.458us |
|    99.9    |                  11.750us |                        6.167us |
|   99.99    |                 442.958us |                      219.875us |
|  99.9999   |                3.826041ms |                     1.681542ms |
|    100     |                3.874833ms |                      1.72425ms |
 ---------------------------------------------------------------------------

Test scenario 3
* Message Number: 10_000_000
* Message Send Rate: 1 every 1us
* Message business payload: 8 bytes

| Percentile | Unbounded Mailbox Latency | Fast Unbounded Mailbox Latency |
|:----------:|--------------------------:|-------------------------------:|
|     70     |                     458ns |                          791ns |
|     75     |                     458ns |                          875ns |
|     80     |                     458ns |                            1us |
|     85     |                     459ns |                        1.416us |
|     90     |                     625ns |                        1.541us |
|     95     |                   1.292us |                        1.584us |
|     99     |                   2.333us |                        1.958us |
|    99.9    |                  34.041us |                       34.084us |
|   99.99    |                2.966584ms |                     1.394875ms |
|  99.9999   |                3.566709ms |                      1.90175ms |
|    100     |                 4.36955ms |                     1.909792ms |
 ---------------------------------------------------------------------------