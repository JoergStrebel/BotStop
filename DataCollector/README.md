# DataCollector

Tool for collecting data from the Twitter Streaming APIs and storing them in a PostgreSQL database.  
Current development environment: OpenJDK 1.8, Eclipse, Twitter4J, Maven  
Before building and running the data collector, please put your twitter4j.properties file in src/main/resources/. It is required to establish a connection to the Twitter REST APIs.  
Please put the Twitter userIDs and topics to be followed in the file src/main/resources/dataCollector.properties  
Build: mvn clean install  

