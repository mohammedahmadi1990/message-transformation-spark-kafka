1.message is read from a directory containing json file by message-producer and it writes to kafka topic-one
2,3. then in this two steps: message is read from kafka topic-one and a price element added then written to kafka topic-two.
4. message reader from spark reads from kafka topic-one and prints to console.

[You need to install and run kafka with to topics topic-one and topic-two , and run the applications in intellij]
