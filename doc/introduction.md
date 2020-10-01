![ReActed Logo](https://github.com/wireknight/reacted/blob/master/doc/artwork/logo.png)

<p align="center">
<a href="https://www.reacted.io">Reactive Actor framework for flexible/distributed/scalable and monitorable applications</a>
</p>

# Introduction
 ReActed is an actor oriented framework for creating reactive applications. It naturally embraces the microcomponents
 paradigm in its design, allowing the realization of distributed, flexible, reliable, monitorable and automatically scalable
 applications out of the box. ReActed amplifies the productivity of developers providing a ready to use solution that can be
 easily integrated with almost any other technology and fine grained tuned only if required.
 
 ReActed can manage for you:
 
 - Parallelization: leveraging the actor paradigm, reactor can manage for you parallelization, saving the programmer the
 effort to write and mantain concurrent code. Synchronization or memory visibility issues problems are structurally removed
 from your code
 
 - Distribution: a ReActed instance can naturally and automatically create a cluster with other instances. Resources can
 be shared, used, added or created through a location agnostic approach, creating an automatically scalable and location
 agnostic way of designing a system
 
 - Monitoring: creating an event sourced or a choreography oriented microservices system can be tricky,
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

Getting Started

If you want to jump into the code, you can give a look to the [examples](https://github.com/wireknight/reacted/tree/master/examples) directory, otherwise give a look to the quickstart guide in the official documentation


