/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.streaming.ggoals

import java.io.File
import java.util.Properties
import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala._
import com.typesafe.config.ConfigFactory
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer08, FlinkKafkaProducer08}
import org.apache.flink.api.common.serialization.SimpleStringSchema


/**
 * Skeleton for a Flink Streaming Job.
 *
 * For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * To package your appliation into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
object MirrorMaker {
  def main(args: Array[String]) {
    if(args.length < 1 ) {
      System.out.println("usage : start.sh 'your config file path'")
      System.exit(1)
    }

    val filePath = args(0)
    val config = ConfigFactory.parseFile(new File(filePath))

    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.enableCheckpointing(5000) // checkpoint every 15000 msecs
    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    env.setParallelism(config.getInt("num.parallelism"))

    env.setRestartStrategy(RestartStrategies.failureRateRestart(
      3, // max failures per unit
      Time.of(10, TimeUnit.MINUTES), //time interval for measuring failure rate
      Time.of(5, TimeUnit.SECONDS) // delay
    ))

    // for Kafka Consumer...
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", config.getString("consumer.broker.list"))
    // only required for Kafka 0.8
    properties.setProperty("zookeeper.connect", config.getString("zookeeper.connect"))
    properties.setProperty("group.id", config.getString("group.id"))
    //properties.setProperty("auto.commit.interval.ms", "1000")
    //properties.setProperty("fetch.message.max.bytes", "10485760")

    val kafkaConsumer = new FlinkKafkaConsumer08[String](
      config.getString("topic"),
      new SimpleStringSchema,
      properties)

    config.getString("consumer.from.offset") match {
      case "latest" | "Latest" | "largest" | "Largest" =>
        kafkaConsumer.setStartFromLatest()
      case "earliest" | "Earliest" | "smallest" | "Smallest" =>
        kafkaConsumer.setStartFromLatest()
      case _ =>
        kafkaConsumer.setStartFromGroupOffsets()
    }

    val inputStream = env.addSource(kafkaConsumer)


    //inputStream.print()

    val producer = new FlinkKafkaProducer08[String](
      config.getString("produce.broker.list"),
      config.getString("topic"),
      new SimpleStringSchema)

    // the following is necessary for at-least-once delivery guarantee
    producer.setLogFailuresOnly(false)   // "false" by default
    producer.setFlushOnCheckpoint(true)  // "false" by default


    inputStream.addSink(producer)

    // execute program
    env.execute(s"Flink-Mirror-Maker-${config.getString("group.id")}")
  }
}
