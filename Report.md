Assignment 1
------------

# Team Members
Linus Ihringer, Sebastian Jung

# GitHub link to your (forked) repository

https://github.com/ihringerlinus/Assignment1

# Task 4

1. (0.25pt) What are Interface Definition Languages (IDL) used for? Name and explain the IDL that you use for this task.
   Ans: IDL are used to define a contract between a client and a server. They specify the methods that can be called, the parameters they take, and the return types. In this task, we use Protocol Buffers (protobuf) as our IDL. Protobuf is a language-neutral, platform-neutral extensible mechanism for serializing structured data. It allows us to define our service methods and message types in a .proto file, which is then compiled to generate code in various programming languages for both the client and server.
2. (0.25pt) In this implementation of gRPC, you use channels. What are they used for?
   Ans: A channel represents a connection to a specific server endpoint. They are used by the client to initate and manage the connection to the server. Channels handle the underlying transport details, such as connection pooling, load balancing, and retries. They provide a way for the client to communicate with the server in a thread-safe manner.
3. (0.5pt)
   (0.25) Describe how the MapReduce algorithm works. Do you agree that the MapReduce programming model may have latency issues? What could be the cause of this?
   Ans: MapReduce algorithm processes large datasets in a distributed and parallel manner across a cluster of computers. It consists of two main functions: Map and Reduce. The Map function takes an input dataset and transforms it into a set of intermediate key-value pairs. The Reduce function then takes these intermediate key-value pairs and aggregates them to produce the final output. The MapReduce programming model can have latency issues due to several factors. One common cause is the overhead of data shuffling between the Map and Reduce phases, which involves sorting and transferring large amounts of data across the network. Additionally, if there are straggler tasks (tasks that take significantly longer to complete than others), they can delay the overall job completion time. The initialization and setup time for the distributed environment can also contribute to latency.
   (0.25) Can this programming model be suitable (recommended) for iterative machine learning or data analysis applications? Please explain your argument.
   Ans: The MapReduce programming model is generally not recommended for iterative machine learning or data analysis applications. This is because iterative algorithms often require multiple passes over the same data, which can lead to significant overhead in terms of data shuffling and reloading between iterations. Each iteration would involve reading the data from disk, processing it, and writing the results back to disk, which can be inefficient and slow. Additionally, the stateless nature of MapReduce makes it challenging to maintain state across iterations, which is often necessary for machine learning algorithms. Instead, frameworks like Apache Spark, which support in-memory processing and iterative computations, are better suited for such applications.
