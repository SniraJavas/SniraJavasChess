# OT Backend Assessment
# Engineering Interview Notes

These notes are intended to help explain the architecture, implementation details, database design and engineering decisions behind the solution.

---

# 1. High-Level Architecture

## 1.1 The architecture

The solution uses an event-driven architecture with three projects:

1. `OT.Assessment.App`
2. `OT.Assessment.Consumer`
3. `OT.Assessment.Contracts`

The API handles incoming HTTP requests and publishes wager events to RabbitMQ.

The Consumer independently processes those events and persists them to SQL Server.

The API also provides read endpoints for player wager history and top spenders.

## 1.2 How to explain the architecture

Suggested explanation:

"I approached the problem by separating the ingestion process from the persistence process.

The API is responsible for accepting and publishing wager events. RabbitMQ acts as the messaging layer between the API and the Consumer.

The Consumer processes messages asynchronously and persists them using Dapper and SQL Server stored procedures.

This means the API does not need to wait for database processing to complete before responding to the client.

I also separated the shared message contract into its own project so the API and Consumer use the same event definition.

For querying, I implemented database-level pagination for player wager history and aggregation for the top-spenders endpoint."

## 1.3 Why this architecture?

The main benefits are:

- Separation of concerns.
- Decoupled ingestion and persistence.
- Independent scaling of API and Consumer.
- Asynchronous processing.
- Ability to buffer messages during temporary processing slowdowns.
- Centralised database operations.

The trade-off is eventual consistency. A wager accepted by the API might not immediately appear in a GET response because the Consumer processes it asynchronously.

---

# 2. Low-Level Implementation Walkthrough

Walk through the implementation in the following order.

## 2.1 API Controller

File:

```text
OT.Assessment.App/Controllers/CasinoWagerController.cs
```

Responsibilities:

- Exposes the REST endpoints.
- Validates incoming requests.
- Publishes wager events.
- Retrieves player wager history.
- Retrieves top spenders.
- Returns appropriate HTTP status codes.

Important design decision:

The controller does not contain database access logic or RabbitMQ implementation details. Those responsibilities are delegated to separate components.

The POST endpoint returns `202 Accepted` when the message has been successfully published.

This does not mean the database insert has completed.

## 2.2 Shared Message Contract

File:

```text
OT.Assessment.Contracts/Message/CasinoWagerMessage.cs
```

The contract defines the structure of the wager event.

Both the API and Consumer reference this project.

Why?

Without a shared contract, both applications could maintain separate message models, increasing the risk of inconsistencies when the event structure changes.

## 2.3 RabbitMQ Publisher

File:

```text
OT.Assessment.App/Messaging/RabbitMqPublisher.cs
```

The publisher is responsible for serialising and publishing wager events to RabbitMQ.

The queue is:

```text
ot.assessment.queue
```

The API publishes messages rather than writing directly to the database.

This keeps the HTTP request independent of the database processing time.

## 2.4 Consumer Worker

File:

```text
OT.Assessment.Consumer/Worker.cs
```

The Worker is implemented using `BackgroundService`.

It uses `AsyncEventingBasicConsumer` to receive messages asynchronously.

Its responsibilities include:

1. Connecting to RabbitMQ.
2. Declaring the queue.
3. Configuring prefetch.
4. Subscribing to the queue.
5. Deserialising incoming messages.
6. Resolving the wager repository through dependency injection.
7. Persisting messages.
8. Acknowledging successful processing.

### Manual acknowledgements

The Consumer uses:

```csharp
autoAck: false
```

This means it controls when a message is acknowledged.

The message is acknowledged only after successful database processing.

This reduces the risk of losing messages when the Consumer fails during processing.

### Prefetch

The current implementation uses a prefetch count of one.

This limits unacknowledged messages delivered to the Consumer.

The trade-off is that processing is controlled but throughput may be limited.

I would benchmark higher prefetch values before changing this configuration.

---

# 3. Database Design

Database script:

```text
DatabaseGenerate.sql
```

Database:

```text
OT_Assessment_DB
```

## 3.1 Tables

### Players

Stores player account information.

Primary key:

```sql
AccountId
```

### Providers

Stores casino game provider information.

Primary key:

```sql
ProviderId
```

A unique constraint prevents duplicate provider names.

### Games

Stores games associated with casino providers.

Primary key:

```sql
GameId
```

Foreign key:

```sql
ProviderId
```

A unique constraint on provider and game name prevents duplicate games for the same provider.

### CasinoWagers

Stores wager transaction records.

Important fields:

- WagerId
- TransactionId
- AccountId
- GameId
- Amount
- CreatedDateTime
- NumberOfBets
- CountryCode
- Duration
- ReceivedAtUtc

The wager table references Players and Games through foreign keys.

`ReceivedAtUtc` records when the database received the wager, independently of the original wager timestamp.

## 3.2 Database constraints

The database uses:

- Primary keys.
- Foreign keys.
- Unique constraints.
- Check constraints.
- Indexes.

These protect data integrity at the database level rather than relying entirely on API validation.

---

# 4. Stored Procedure Walkthrough

There are three important stored procedures.

## 4.1 dbo.usp_CasinoWager_Insert

Purpose: Insert a wager and maintain the related player, provider and game records.

### Execution flow

1. Check whether the wager already exists.
2. If it exists, return zero.
3. Insert or update the player information.
4. Insert the provider if it does not exist.
5. Insert the game if it does not exist for that provider.
6. Resolve the GameId.
7. Insert the wager.
8. Return one.

### Transaction management

The procedure uses a transaction to keep the related database operations consistent.

It also uses:

```sql
SET XACT_ABORT ON;
```

Together with `TRY/CATCH`, this provides a mechanism for handling errors and rolling back failed transactions.

### Idempotency

The most important design decision is preventing duplicate wagers.

RabbitMQ messages may be redelivered.

For example:

- A message is received.
- The database insert succeeds.
- The Consumer crashes before acknowledging the message.
- RabbitMQ delivers the message again.

Without idempotency, the second delivery could create another wager record.

The procedure checks the existing wager identifier using locking:

```sql
WITH (UPDLOCK, HOLDLOCK)
```

The primary key on `WagerId` also enforces uniqueness.

The procedure returns:

| Result | Meaning |
|---|---|
| 1 | New wager inserted |
| 0 | Duplicate wager ignored |

Interview explanation:

"I did not assume that RabbitMQ would deliver every message exactly once. Instead, I designed the database operation to be idempotent. The wager identifier acts as the unique business key, and the stored procedure ensures that processing the same message again does not create another record."

## 4.2 dbo.usp_PlayerCasinoWagers_GetPaged

Purpose: Retrieve a player's wager history using pagination.

Parameters:

```sql
@AccountId
@Page
@PageSize
```

### Execution flow

1. Receive the player account identifier.
2. Calculate the row offset.
3. Retrieve the requested page.
4. Join the game and provider information.
5. Sort the records.
6. Return the total number of matching records.

Pagination uses:

```sql
OFFSET ... ROWS
FETCH NEXT ... ROWS ONLY
```

### Why two result sets?

The first result set contains the wager records for the requested page.

The second result set contains the total number of matching wagers.

The API uses the total to calculate:

```text
TotalPages = Ceiling(Total / PageSize)
```

The repository uses Dapper's `QueryMultipleAsync` to read both result sets from one stored procedure execution.

Interview explanation:

"I implemented pagination at the database level so we do not retrieve every wager and filter in application memory. The procedure returns both the requested page and total count, allowing the API to construct the pagination metadata efficiently."

## 4.3 dbo.usp_PlayerTopSpenders_Get

Purpose: Return the highest-spending players.

Parameter:

```sql
@Count
```

### Execution flow

1. Group wagers by AccountId.
2. Calculate the total wager amount using `SUM`.
3. Join the Players table to obtain usernames.
4. Sort by total amount in descending order.
5. Return the requested number of players.

Interview explanation:

"I delegated the aggregation and sorting to SQL Server instead of retrieving all wager records and calculating totals in the API. This reduces unnecessary data transfer and application memory usage."

---

# 5. Repository and Data Access

## 5.1 WagerRepository

The Consumer uses Dapper to execute:

```text
dbo.usp_CasinoWager_Insert
```

The repository:

- Creates a SQL connection.
- Opens the connection asynchronously.
- Maps the message properties to stored procedure parameters.
- Executes the procedure.
- Returns whether the wager was inserted.

Connections are disposed after each operation. ADO.NET connection pooling allows the underlying physical connections to be reused.

## 5.2 CasinoWagerRepository

The API repository handles the two read operations.

For player history, it uses:

```csharp
QueryMultipleAsync
```

This reads the two result sets returned by the pagination stored procedure.

For top spenders, it maps the stored procedure results directly into `TopSpenderDto` objects.

The controller remains independent of SQL implementation details.

---

# 6. Important Engineering Decisions

## 6.1 Why RabbitMQ?

The API should not wait for database processing to finish before returning a response.

RabbitMQ allows the ingestion and persistence processes to operate independently.

It also provides a buffer when messages arrive faster than the Consumer can process them.

## 6.2 Why Dapper?

Dapper was selected because the database operations are implemented through stored procedures.

It provides lightweight database access, explicit parameter mapping and direct control over execution.

The trade-off is that it does not provide Entity Framework Core's change tracking or migration functionality.

## 6.3 Why stored procedures?

They centralise database logic and allow database operations to be tuned independently of application code.

## 6.4 Why separate the Contracts project?

The API and Consumer need a consistent message structure.

A shared contract reduces duplication and contract drift.

## 6.5 Why manual acknowledgements?

The Consumer should not acknowledge a message before the database operation succeeds.

This allows unacknowledged messages to be redelivered after a processing failure.

## 6.6 Why database-level idempotency?

Message redelivery is a normal consideration in distributed systems.

The database must protect against duplicate business operations, not assume the messaging system guarantees exactly-once delivery.

---

# 7. Scalability and Production Considerations

If I were extending the assessment into a production system, I would consider the following.

## Messaging reliability

- Initial RabbitMQ connection retries with exponential backoff.
- Dead-letter exchanges and queues.
- Controlled retry policies.
- Publisher confirms.
- Monitoring queue depth and processing failures.

## Performance

- Benchmark different prefetch values.
- Introduce controlled Consumer concurrency if required.
- Evaluate batch inserts.
- Inspect SQL execution plans.
- Monitor database connection pooling and lock contention.

## Data management

- Additional transaction uniqueness rules where business requirements demand them.
- Audit trails.
- Wager correction and reversal workflows.
- Data retention and archival policies.

## Observability

- Structured logging.
- Correlation IDs.
- Distributed tracing.
- Application Insights or OpenTelemetry.
- Queue-depth and processing-latency metrics.

## Analytics

For a much larger dataset, I would consider a summary table or dedicated analytics read model for the top-spenders query.

This would reduce the cost of repeatedly aggregating the entire wager history.

---

# 8. Interview Walkthrough

## 8.1 High-level walkthrough

Suggested opening:

"I approached the assessment using an event-driven architecture.

The main design consideration was separating the responsibility of accepting incoming wager events from the responsibility of persisting them.

I created an ASP.NET Core API, a background Consumer and a shared Contracts library.

When a wager is submitted, the API validates it and publishes an event to RabbitMQ. The Consumer independently processes the message and persists it to SQL Server using Dapper and stored procedures.

For querying, I implemented database-level pagination for player wager history and a SQL aggregation query for the top-spenders endpoint.

I also considered duplicate message delivery and implemented idempotency at the database level to prevent duplicate wager records."

## 8.2 Low-level walkthrough

Show the implementation in this order:

1. `CasinoWagerController.cs`: API responsibilities and HTTP responses.
2. `CasinoWagerMessage.cs`: Shared event contract.
3. `RabbitMqPublisher.cs`: Message publishing.
4. `Worker.cs`: Asynchronous message consumption and acknowledgements.
5. `WagerRepository.cs`: Dapper database execution.
6. `DatabaseGenerate.sql`: Tables, constraints, indexes and stored procedures.
7. `CasinoWagerRepository.cs`: Read queries and result mapping.

Finish by demonstrating a POST request, Consumer logs, SQL persistence and both GET endpoints.

---

# 9. Likely Interview Questions

### Why does the API return 202 rather than 200?

Because the request has been accepted for asynchronous processing. It does not claim that the database insert has completed.

### What happens if the Consumer crashes after inserting but before acknowledging?

RabbitMQ can redeliver the message. The stored procedure checks the wager identifier and prevents another insert.

### How do you prevent duplicate wagers?

The database uses a unique WagerId and an idempotent insert procedure with transaction and locking safeguards.

### How would you improve throughput?

I would measure the bottleneck first. Then I would investigate API publishing latency, queue depth, Consumer processing time and SQL execution performance.

Based on those results, I would evaluate prefetch, controlled concurrency and batch database inserts.

### What happens if SQL Server is unavailable?

The current Consumer catches processing failures and requeues the message. In production, I would introduce controlled retries, backoff and a dead-letter queue to prevent endless immediate retries.

### What happens if RabbitMQ is unavailable?

The API should return a service-unavailable response if publishing fails. The Consumer should also have an initial connection retry strategy.

### How would you scale the system?

The API can be scaled horizontally behind a load balancer. Multiple Consumer instances can process messages from the same queue, provided database capacity and concurrency are managed.

### How would you handle millions of wagers?

I would evaluate table partitioning, archival, indexing, execution plans, batch inserts and pre-aggregated analytics.

### How do you approach exactly-once processing?

"I distinguish between exactly-once message delivery and exactly-once business effects. Rather than assuming a message will only be delivered once, I use idempotent database operations to ensure repeated processing of the same wager does not create duplicate records."

---

# 10. Final Points to Remember

During the interview, focus on explaining:

1. Why the API and Consumer are separate.
2. Why asynchronous messaging was selected.
3. How a wager moves from HTTP request to SQL persistence.
4. Why manual acknowledgements matter.
5. How database idempotency protects against redelivery.
6. Why pagination and aggregation happen in SQL.
7. What the indexes are optimising.
8. What you would improve before production deployment.

Do not claim that the system guarantees exactly-once message delivery. Explain that it protects the database operation against duplicate processing.

Do not claim benchmark performance until the supplied tester has completed and the database has been reconciled.

The strongest demonstration is to walk through one wager end-to-end, explain the reasoning behind each boundary and then discuss the trade-offs and production improvements.
