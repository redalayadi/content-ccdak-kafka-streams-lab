package com.linuxacademy.ccdak.streams;

public class Main {

    public static void main(String[] args) {
        /*
        When a customer makes a purchase, a record is published to the inventory_purchases topic for each item type (i.e., "apples").
        These records have the item type as the key and the quantity purchased in the transaction as the value. An example record would look like this, 
        indicating that a customer bought five apples:

apples:5
Your task is to build a Kafka streams application that will take the data about individual item purchases and output a running total purchase quantity 
for each item type. The output topic is called total_purchases. So, for example, with the following input from inventory_purchases:

apples:5
oranges:2
apples:3
Your streams application should output the following to total_purchases:

apples:8
oranges:2
Be sure to output the total quantity as an Integer. Note that the input topic has the item quantity serialized as a String,
so you will need to work around this using type conversion.
        */
          // Set up the configuration.
          final Properties props = new Properties();
          props.put(StreamsConfig.APPLICATION_ID_CONFIG, "inventory-data");
          props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
          props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
          // Since the input topic uses Strings for both key and value, set the default Serdes to String.
          props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
          props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

          // Get the source stream.
          final StreamsBuilder builder = new StreamsBuilder();
          final KStream<String, String> source = builder.stream("inventory_purchases");

          // Convert the value from String to Integer. If the value is not a properly-formatted number, print a message and set it to 0.
          final KStream<String, Integer> integerValuesSource = source.mapValues(value -> {
              try {
                  return Integer.valueOf(value);
              } catch (NumberFormatException e) {
                  System.out.println("Unable to convert to Integer: \"" + value + "\"");
                  return 0;
              }
          });

          // Group by the key and reduce to provide a total quantity for each key.
          final KTable<String, Integer> productCounts = integerValuesSource
              .groupByKey(Grouped.with(Serdes.String(), Serdes.Integer()))
              .reduce((total, newQuantity) -> total + newQuantity);

          // Output to the output topic.
          productCounts
              .toStream()
              .to("total_purchases", Produced.with(Serdes.String(), Serdes.Integer()));

          final Topology topology = builder.build();
          final KafkaStreams streams = new KafkaStreams(topology, props);
          // Print the topology to the console.
          System.out.println(topology.describe());
          final CountDownLatch latch = new CountDownLatch(1);

          // Attach a shutdown handler to catch control-c and terminate the application gracefully.
          Runtime.getRuntime().addShutdownHook(new Thread("streams-wordcount-shutdown-hook") {
              @Override
              public void run() {
                  streams.close();
                  latch.countDown();
              }
          });

          try {
              streams.start();
              latch.await();
          } catch (final Throwable e) {
              System.out.println(e.getMessage());
              System.exit(1);
          }
          System.exit(0);
      }
    }

}
