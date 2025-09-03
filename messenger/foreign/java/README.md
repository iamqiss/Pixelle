# Java SDK for Messenger

[Messenger GitHub](https://github.com/apache/messenger) | [Website](https://messenger.apache.org) | [Getting started](https://messenger.apache.org/docs/introduction/getting-started) | [Documentation](https://messenger.apache.org/docs) | [Blog](https://messenger.apache.org/blogs) | [Discord](https://discord.gg/C5Sux5NcRa)

[![Tests](https://github.com/apache/messenger/actions/workflows/ci-check-java-sdk.yml/badge.svg)](https://github.com/apache/messenger/actions/workflows/ci-check-java-sdk.yml)
[![x](https://img.shields.io/twitter/follow/messenger_rs_?style=social)](https://x.com/ApacheMessenger)

---

Official Java client SDK for [Apache Messenger](https://messenger.apache.org) message streaming.

The client currently supports HTTP and TCP protocols with blocking implementation.

## Adding the client to your project

Add dependency to `pom.xml` or `build.gradle` file.

You can find the latest version in Maven Central repository:

<https://central.sonatype.com/artifact/org.apache.messenger/messenger-java-sdk>

## Implement consumer and producer

You can find examples for
simple [consumer](https://github.com/apache/messenger/blob/master/foreign/java/examples/simple-consumer/src/main/java/org/apache/messenger/SimpleConsumer.java)
and [producer](https://github.com/apache/messenger/blob/master/foreign/java/examples/simple-producer/src/main/java/org/apache/messenger/SimpleProducer.java)
in the repository.
