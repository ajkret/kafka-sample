# kafka-sample

Here you can find an implementation of a Producer and a Consumer, using Apache Kafka.
The application exposes a REST endpoint and is written in Java using Spring-Boot.

In the project you will find also an implementation using Java's Blocking Queue.

To compile and run the application, you will need Java and Maven.

##Compile
To compile type

    mvn clean package


#Running
To run, type

	java -jar target/kafka-sample.jar

Before start, you will need to install Apache Kafka. Just download it from the site, and start Zookeeper and Kafka as described in the Quickstart and you will be fine.

    http://kafka.apache.org/documentation.html#quickstart

To test, you will need curl, or a Chrome plugin like Postman or Advanced REST client. You will need to POST a message to:

    http://localhost:8090/rest/kafka
    
Headers must contain:
Accept: application/json

In the body, you can actually enter with the text you want.

You can also perform a stress test, by adding a number of repetitions:

    http://localhost:8090/rest/kafka/1000

If you had MySQL, and running the script included, you can put several instances of the application to run, and check different "nodes" processing the messages.


Enjoy. Peace!
