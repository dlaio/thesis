## Overview

This is a simple benchmark written in Java to compare the performance of different AMQP broker implementations.

## Prereqs

- Install Java SDK
- Install [Maven](http://maven.apache.org/download.html) 

## Building

Run:

    mvn install

## Running the Examples

In one terminal window run:

    java -cp target/amqp-benchmarks-0.1-SNAPSHOT.jar benchmarks.Consumer

In another terminal window run:

    java -cp target/amqp-benchmarks-0.1-SNAPSHOT.jar benchmarks.Producer
