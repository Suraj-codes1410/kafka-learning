# 📨 Apache Kafka Learning Repository
 
A hands-on, code-first journey into **Apache Kafka** using **Java 21**, **Spring Boot 3.5.x**, **Docker**, and **Maven**. This repository doubles as a personal study log and a working reference for producer/consumer patterns, Kafka internals, and event-driven design.
 
> 💡 **Goal:** Clone this repo, spin up Kafka with Docker, and start publishing/consuming messages within minutes — while building a solid mental model of how Kafka actually works under the hood.
 
---
 
## 📑 Table of Contents
 
1. [Tech Stack](#-tech-stack)
2. [Repository Structure](#-repository-structure)
3. [Learning Roadmap](#-learning-roadmap)
4. [Apache Kafka Basics](#-apache-kafka-basics)
5. [Kafka Architecture](#-kafka-architecture)
6. [Prerequisites](#-prerequisites)
7. [Running Kafka](#-running-kafka)
8. [Kafka CLI Commands](#-kafka-cli-commands)
9. [Spring Boot Configuration](#-spring-boot-configuration)
10. [Maven Commands](#-maven-commands)
11. [Producer Boilerplate](#-producer-boilerplate)
12. [Consumer Boilerplate](#-consumer-boilerplate)
13. [Sample DTO](#-sample-dto)
14. [Common Errors](#-common-errors--troubleshooting)
15. [Useful Git Commands](#-useful-git-commands)
16. [Best Practices](#-best-practices)
17. [Future Learning Roadmap](#-future-learning-roadmap)
18. [References](#-references)
---
 
## 🛠 Tech Stack
 
| Technology | Version | Purpose |
|---|---|---|
| Java | 21 (LTS) | Core language, records, virtual threads |
| Spring Boot | 3.5.x | Application framework, auto-configuration |
| Apache Kafka | Latest stable | Distributed event streaming platform |
| Docker | Latest | Local Kafka broker + Zookeeper/KRaft containers |
| Maven | 3.9+ | Build & dependency management |
| IntelliJ IDEA | Latest | Primary IDE |
 
---
 
## 📁 Repository Structure
 
> ⚠️ **Note:** No repository files were available to scan when this README was generated, so the tree below is a **recommended structure**, not a verified one. Replace this section with the actual output of `tree -L 2` once your project folders exist.
 
```
kafka-learning
│
├── kafka-producer-example/     # (recommended) Spring Boot producer service
├── kafka-consumer-example/     # (recommended) Spring Boot consumer service
├── docker/                     # (recommended) docker-compose.yml + Kafka configs
├── notes/                      # (recommended) personal study notes, diagrams
└── README.md
```
 
**To regenerate this section accurately**, run the following from the repo root and paste the output here:
 
```bash
tree -L 2 -I 'target|.git|.idea'
```
 
---
 
## 🗺 Learning Roadmap
 
Progress tracker — check items off as you complete them.
 
- [x] Kafka Setup (Docker + local broker)
- [x] Producer (basic publish)
- [x] Consumer (basic subscribe)
- [x] JSON Serialization
- [ ] Partitions
- [ ] Consumer Groups
- [ ] Message Keys
- [ ] Retry
- [ ] Dead Letter Queue (DLQ)
- [ ] Kafka Streams
- [ ] Event-Driven Architecture
---
 
## 📚 Apache Kafka Basics
 
| Concept | Simple Explanation |
|---|---|
| **Producer** | An application that publishes (writes) messages to a Kafka topic. |
| **Consumer** | An application that reads messages from a Kafka topic. |
| **Broker** | A single Kafka server that stores data and serves client requests. A cluster is made of multiple brokers. |
| **Topic** | A named stream of messages — think of it as a category or log file, e.g. `order-events`. |
| **Partition** | A topic is split into partitions so it can be parallelized across brokers and consumers. |
| **Offset** | A unique, sequential ID assigned to each message within a partition, marking its position. |
| **Consumer Group** | A set of consumers that work together to read a topic, sharing partitions between them. |
| **Event** | A record of something that happened — the actual unit of data flowing through Kafka. |
| **Message** | The physical data structure (key, value, headers, timestamp) sent through a topic. |
| **Cluster** | A group of Kafka brokers working together for scalability and fault tolerance. |
 
> 🧠 **Tip:** Think of a **topic** as a folder, **partitions** as sub-logs inside that folder, and **offsets** as line numbers in each sub-log.
 
---
 
## 🏗 Kafka Architecture
 
```
 ┌───────────┐         ┌──────────────────┐         ┌───────────┐
 │  Producer │  ───▶   │   Kafka Topic     │  ───▶   │ Consumer  │
 │ (App A)   │         │ (Partitions 0..N) │         │ (App B)   │
 └───────────┘         └──────────────────┘         └───────────┘
                                │
                                ▼
                      ┌───────────────────┐
                      │   Kafka Broker(s)  │
                      │   (Cluster)        │
                      └───────────────────┘
```
 
**Message Flow:**
 
1. A **Producer** serializes data (e.g., to JSON) and sends it to a **Topic**, optionally with a key.
2. Kafka routes the message to a specific **Partition** — either by key hash or round-robin.
3. The **Broker** appends the message to the partition's log and assigns it an **Offset**.
4. A **Consumer** (part of a **Consumer Group**) polls the partition and reads messages in order, tracking its offset.
5. Kafka retains messages for a configurable period regardless of whether they've been read — consumers don't "remove" messages.
---
 
## ✅ Prerequisites
 
Make sure the following are installed before you begin:
 
| Tool | Verify Installation |
|---|---|
| Java 21 | `java -version` |
| Docker Desktop | `docker --version` |
| Maven | `mvn -version` |
| IntelliJ IDEA | Open IDE → Check version in **About** |
| Git | `git --version` |
 
> If any command fails, install the corresponding tool before proceeding — the setup steps below assume all prerequisites are met.
 
---
 
## 🚀 Running Kafka
 
Kafka runs locally via Docker. Typical workflow:
 
```bash
# Start Kafka (and Zookeeper/KRaft) in detached mode
docker compose up -d
```
Starts all services defined in `docker-compose.yml` in the background (`-d` = detached), so your terminal stays free.
 
```bash
# Stop and remove containers, networks
docker compose down
```
Gracefully stops all running containers and cleans up the network Docker created for them. Data may be lost unless volumes are configured.
 
```bash
# Restart all services
docker compose restart
```
Useful when Kafka config changes or a broker gets into a bad state — restarts without recreating containers.
 
```bash
# List running containers
docker ps
```
Shows container IDs, names, ports, and status — use this to confirm Kafka and Zookeeper are actually up.
 
```bash
# View logs of a specific container
docker logs <container_name>
```
Streams stdout/stderr from a container — the first place to check when Kafka fails to start.
 
```bash
# Open a shell inside a running container
docker exec -it <container_name> bash
```
Drops you into an interactive shell inside the container — needed to run Kafka CLI tools directly on the broker.
 
---
 
## ⌨️ Kafka CLI Commands
 
> Run these from inside the Kafka broker container (via `docker exec`) or using a locally installed Kafka CLI.
 
**Create a Topic**
```bash
kafka-topics.sh --create --topic my-topic --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
```
Creates a new topic named `my-topic` with 3 partitions and a replication factor of 1 (single-broker setup).
 
**List Topics**
```bash
kafka-topics.sh --list --bootstrap-server localhost:9092
```
Shows all topics currently available on the cluster.
 
**Describe Topic**
```bash
kafka-topics.sh --describe --topic my-topic --bootstrap-server localhost:9092
```
Displays partition count, leader/replica assignment, and ISR (in-sync replicas) for a topic.
 
**Delete Topic**
```bash
kafka-topics.sh --delete --topic my-topic --bootstrap-server localhost:9092
```
Permanently deletes a topic and its data. Requires `delete.topic.enable=true` on the broker.
 
**Console Producer**
```bash
kafka-console-producer.sh --topic my-topic --bootstrap-server localhost:9092
```
Opens an interactive prompt — anything you type and press Enter on gets published as a message.
 
**Console Consumer**
```bash
kafka-console-consumer.sh --topic my-topic --bootstrap-server localhost:9092 --from-beginning
```
Reads and prints messages from a topic. `--from-beginning` replays all retained messages, not just new ones.
 
**List Consumer Groups**
```bash
kafka-consumer-groups.sh --list --bootstrap-server localhost:9092
```
Shows all active/known consumer groups on the cluster.
 
**Describe Consumer Group (Offsets & Lag)**
```bash
kafka-consumer-groups.sh --describe --group my-group --bootstrap-server localhost:9092
```
Shows current offset, log-end-offset, and **lag** (how far behind the consumer is) per partition — critical for debugging slow consumers.
 
---
 
## ⚙️ Spring Boot Configuration
 
### Producer (`application.yml`)
 
```yaml
spring:
  kafka:
    bootstrap-servers: localhost:9092
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.springframework.kafka.support.serializer.JsonSerializer
```
 
### Consumer (`application.yml`)
 
```yaml
spring:
  kafka:
    bootstrap-servers: localhost:9092
    consumer:
      group-id: my-consumer-group
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.springframework.kafka.support.serializer.JsonDeserializer
      properties:
        spring.json.trusted.packages: "com.example.kafka.dto"
        spring.json.value.default.type: com.example.kafka.dto.Customer
```
 
### Property Reference
 
| Property | Explanation |
|---|---|
| `bootstrap-servers` | Address(es) of the Kafka broker(s) the client connects to first, to discover the rest of the cluster. |
| `group-id` | Identifies which consumer group this consumer belongs to — partitions are load-balanced within a group. |
| `key-serializer` / `value-serializer` | Convert Java objects into bytes before sending (producer side). |
| `key-deserializer` / `value-deserializer` | Convert bytes back into Java objects on receipt (consumer side). |
| `spring.json.trusted.packages` | Whitelist of Java packages the `JsonDeserializer` is allowed to deserialize into — a security guard against arbitrary class instantiation. |
| `spring.json.value.default.type` | Fallback target type used if the message doesn't carry type headers. |
 
---
 
## 📦 Maven Commands
 
```bash
mvn clean
```
Deletes the `target/` directory, removing previously compiled artifacts — ensures a fresh build.
 
```bash
mvn compile
```
Compiles the main source code without running tests or packaging.
 
```bash
mvn package
```
Compiles code, runs tests, and packages the application into a `.jar` (or `.war`) in `target/`.
 
```bash
mvn test
```
Runs unit tests only, using the project's configured test framework (e.g., JUnit).
 
```bash
mvn spring-boot:run
```
Runs the Spring Boot application directly, without needing to build and run a separate JAR — ideal for local development.
 
```bash
mvn dependency:tree
```
Prints the full dependency graph, including transitive dependencies — useful for resolving version conflicts.
 
---
 
## 📤 Producer Boilerplate
 
**REST Controller** — exposes an HTTP endpoint to trigger a publish:
 
```java
@RestController
@RequestMapping("/api/customers")
public class CustomerController {
 
    private final CustomerPublisherService publisherService;
 
    public CustomerController(CustomerPublisherService publisherService) {
        this.publisherService = publisherService;
    }
 
    @PostMapping
    public ResponseEntity<String> publishCustomer(@RequestBody Customer customer) {
        publisherService.publish(customer);
        return ResponseEntity.ok("Customer event published");
    }
}
```
Accepts an HTTP `POST` request and delegates the actual Kafka publishing to a dedicated service — keeps the controller thin.
 
**Publisher Service** — wraps `KafkaTemplate`:
 
```java
@Service
public class CustomerPublisherService {
 
    private final KafkaTemplate<String, Customer> kafkaTemplate;
    private static final String TOPIC = "customer-events";
 
    public CustomerPublisherService(KafkaTemplate<String, Customer> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }
 
    public void publish(Customer customer) {
        kafkaTemplate.send(TOPIC, customer.id(), customer);
    }
}
```
`KafkaTemplate` is Spring's high-level abstraction over the native Kafka producer — `send()` here uses the customer's ID as the message **key**, which determines the target partition.
 
---
 
## 📥 Consumer Boilerplate
 
**Consumer Service** — listens for messages:
 
```java
@Service
public class CustomerConsumerService {
 
    @KafkaListener(topics = "customer-events", groupId = "customer-consumer-group")
    public void consume(Customer customer) {
        System.out.println("Received customer: " + customer);
        // business logic here
    }
}
```
`@KafkaListener` subscribes this method to the given topic and group — Spring handles polling, deserialization, and offset commits automatically (with default settings).
 
---
 
## 🧾 Sample DTO
 
```java
public record Customer(
    String id,
    String name,
    String email
) {}
```
A Java `record` is ideal for Kafka DTOs — immutable, concise, and comes with `equals()`, `hashCode()`, and `toString()` generated for free.
 
---
 
## 🐞 Common Errors & Troubleshooting
 
| Error | Cause | Solution |
|---|---|---|
| **Port already in use** | Another process (or leftover container) is bound to `9092` or `2181`. | Run `docker ps` to find the conflicting container, then `docker stop <id>`, or change the port mapping in `docker-compose.yml`. |
| **Kafka not running / connection refused** | Broker container isn't up, or `bootstrap-servers` points to the wrong host/port. | Run `docker compose up -d` and confirm with `docker ps` and `docker logs <kafka_container>`. |
| **Consumer not receiving messages** | Wrong topic name, mismatched `group-id`, or consumer started after messages were sent (and `auto-offset-reset` is `latest`). | Verify topic name matches exactly; set `spring.kafka.consumer.auto-offset-reset: earliest` to replay old messages. |
| **Serialization error** | Producer's serializer doesn't match the object type being sent. | Ensure `value-serializer` matches your payload type (e.g., use `JsonSerializer` for POJOs/records, not `StringSerializer`). |
| **`JsonDeserializer` trusted packages error** | Consumer blocks deserialization into a package not explicitly trusted, as a security measure. | Add the DTO's package to `spring.json.trusted.packages`, or use `*` only in local/dev environments. |
| **Missing dependency** | `spring-kafka` or `kafka-clients` not declared in `pom.xml`. | Add the `spring-kafka` starter dependency and run `mvn clean install`. |
| **Maven build errors** | Version mismatch between Spring Boot and Spring Kafka, or corrupted local `.m2` cache. | Check the Spring Kafka/Spring Boot compatibility matrix; try `mvn clean install -U` to force-update dependencies. |
| **Docker errors on startup** | Insufficient memory allocated to Docker, or stale volumes from a previous run. | Increase Docker Desktop's memory limit; run `docker compose down -v` to clear old volumes, then restart. |
| **Spring Boot version mismatch** | Using Spring Kafka features not supported by the declared Spring Boot version. | Confirm `spring-boot-starter-parent` version aligns with the Spring Kafka version in use (check the official compatibility table). |
 
---

## 🔧 Useful Git Commands
 
```bash
git clone <repo-url>
```
Downloads a full copy of a remote repository (including history) to your local machine.
 
```bash
git status
```
Shows which files are staged, unstaged, or untracked in your working directory.
 
```bash
git add <file>
```
Stages changes, marking them to be included in the next commit. Use `git add .` to stage everything.
 
```bash
git commit -m "message"
```
Saves staged changes as a new snapshot in the repository's history, with a descriptive message.
 
```bash
git push
```
Uploads local commits to the remote repository (e.g., GitHub).
 
```bash
git pull
```
Fetches and merges changes from the remote repository into your current local branch.
 
---
 
## 🌟 Best Practices
 
- **Use meaningful topic names** — e.g., `order-created`, `payment-failed`, not `topic1`.
- **One responsibility per consumer** — avoid a single `@KafkaListener` handling unrelated business logic.
- **Use DTOs instead of raw `Map<String, Object>`** — type safety catches bugs at compile time.
- **Prefer Java `record`s** for DTOs where possible — concise and immutable.
- **Separate producer and consumer modules/services** — keeps deployment and scaling independent.
- **Avoid hardcoding values** (topic names, broker URLs) — externalize to `application.yml` or environment variables.
- **Centralize configuration in `application.yml`** rather than scattering `@Value` annotations across classes.
> 💡 **Tip:** Treat Kafka topics like public API contracts — changing a message schema without versioning can silently break every downstream consumer.
 
---
 
## 🔮 Future Learning Roadmap
 
Planned projects and topics to deepen Kafka expertise:
 
- [ ] **Order Management** service (event-sourced)
- [ ] **Notification Service** (consumes multiple event types)
- [ ] **Payment Service** (with idempotency handling)
- [ ] **Inventory Service** (stock updates via events)
- [ ] Full **microservices** architecture tying the above together
- [ ] **Kafka Streams** for real-time stream processing
- [ ] **Avro** serialization
- [ ] **Schema Registry** integration
- [ ] **Dead Letter Queue (DLQ)** patterns
- [ ] **Retry** strategies (exponential backoff, retry topics)
- [ ] Kafka **Transactions** (exactly-once producer/consumer)
- [ ] **Exactly-once semantics** end-to-end
- [ ] **Monitoring** with Prometheus
- [ ] **Dashboards** with Grafana
---
 
## 📖 References
 
- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Spring for Apache Kafka Reference](https://docs.spring.io/spring-kafka/reference/)
- [Spring Boot Documentation](https://docs.spring.io/spring-boot/documentation.html)
- [Docker Documentation](https://docs.docker.com/)
---
 
<p align="center">
  <sub>Built as a personal Kafka learning log — updated as new concepts are learned and implemented.</sub>
</p>
 
