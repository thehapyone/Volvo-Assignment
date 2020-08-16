# Volvo-Assignment

## Requirements 
 - Java > 8. Ensure Java is installed and the Java Environment variable "JAVA_HOME" is properly setup.
 - OS Environment : Linux or MAC
 
## Environment Setup
The Environment used for the this assignment is based on the following:
 - Kafka 2.13-2.6.0
 - Python 3.6.0
 - Ubuntu 18.04
 - Java 11.0.8
 
### Getting Started
Run the below to start the necessary kafka server. The kafka server runs on localhost:9092 and zookeeper also runs on localhost:2181.
 - run the script "start-all-server.sh" : This will automatically launch the zookeeper and kafka in two seperate shells. It is importantly runs properly. If you already have an existing Kafka and Zookeeper running on those ports, then you don't need to launch this.
 
The impplementation for the producer and the consumer is based on Python as compared to Java or Scala. For working with Python, it will require using a kafka python library. The kafka-python library can be installed with pip. See below:
 - pip install kafka-python
 
 

## Ideas for question 1
For the Back End Team
 1 - In the sample data provided, there is inconsistent formatting for the latitude location data. In the startLocation key, the latitude is a string and longitude is a number but in endlocation in which both are numbers. This lack of uniform can easily lead to errors and unnecessary conversion time.
 2 - The string for backendRegion is "uswest2", although this is just a code name to specify the region. It stills contain some information like the country name, direction (west, east, north, south)
  By having it in this current format "uswest2", it is slightly more difficult to parse out the country, and other information. But if they consider an "-" based format like "us-west-2", here we can simply find the country be picking the characters before an "-".
For the Product Owners
1 - Most of the content in the JSON seems to have bounded values aside from the location, IDs, and time data. What this means is that might be limited knowledge or discovery that might be made. For example, it will be nice to provide a comment key which might shed light on why something like for example "service_providercancelled" occurred terminating the whole flowId transition.
2 - Assumed that serviceMainType tells the service carried out or requested, which seems there is more than one party involved in the transaction. The serviceProviderName and some unknown client or customer, this latter information is not provided for which might be a good addon for the business analyst side of things.


## Ideas for question 2
Suggest a schema for storing the events in a relational format for long-term storage. The data will be consumed mainly by business analysts. 
Provide the implementation script and reasoning behind your design decision.

### Storing the Events in a Relational Format for Lng-term storage.
A relational database with 3 tables schema design is considered as shown in the image below:
![Schema_Design_Overview](./main/assets/schema_design.jpg)
![Schema_Design_Overview2](main/assets/schema_design.jpg)

The flowkey can be used to cluster service transition together. 
We can use that to track how well a service. For example From 75 - 78 The service was completed successfully.