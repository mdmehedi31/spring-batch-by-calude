# Spring Batch Mastery Guide

**Audience:** Java 21 backend developer who already knows Spring Boot, Spring Data JPA, MySQL and REST.
**Goal:** understand Spring Batch from first principles to production, including how the framework works inside.

---

## 0. Read this first: versions, imports, and how this guide is written

### Which version does this guide target?

As of October 2026 the current generation is **Spring Batch 6.0.x on Spring Boot 4.0.x** (Spring Framework 7). Spring Batch 6 still runs on Java 17+, so your Java 21 is fine. Many production systems are still on **Spring Batch 5.2 / Spring Boot 3.5**, so every place where the two differ is marked with a **"v5 vs v6"** note.

The changes that matter most for you (taken from the official 6.0 migration guide):

| Area | Spring Batch 5.x | Spring Batch 6.x |
|---|---|---|
| Chunk step builder | `.chunk(10, transactionManager)` (`SimpleStepBuilder`, `FaultTolerantStepBuilder`) | `ChunkOrientedStepBuilder` / `.chunk(10)` + optional `.transactionManager(tm)`; the old builders are deprecated |
| Fault tolerance | `.faultTolerant().skip(..).retry(..)` built on Spring Retry | `.faultTolerant()` on the new builder; retry is built on **Spring Framework 7 core retry**, skip is based entirely on `SkipPolicy` |
| `@EnableBatchProcessing` | Tied to JDBC (`dataSourceRef`, ...) | Common settings only; store settings moved to `@EnableJdbcJobRepository` / `@EnableMongoJobRepository` |
| Launching | `JobLauncher` | `JobOperator` (now extends `JobLauncher`); `JobLauncher` is deprecated. `jobOperator.start(job, params)` returns a `JobExecution` |
| Querying history | `JobExplorer` bean | `JobRepository` now extends `JobExplorer`; `JobExplorer` is deprecated |
| Packages | `org.springframework.batch.item.*`, `org.springframework.batch.core.Job` | Infrastructure moved to `org.springframework.batch.infrastructure.*`; `Job`, `JobExecution` moved to `org.springframework.batch.core.job`; `Step`, `StepExecution` to `org.springframework.batch.core.step`; listeners to `org.springframework.batch.core.listener`; `JobParameters` to `org.springframework.batch.core.job.parameters` |
| Domain model | Mutable, `Long` ids, `JobParameters` = `Map<String, JobParameter>` | Redesigned to be immutable, primitive `long` ids, `JobParameter` is a record that holds its own name, `JobParameters` holds a `Set<JobParameter>` |
| Artifact construction | Default constructor + setters + `afterPropertiesSet()` | Required dependencies at construction time; **use the builders** |
| Metadata sequence | `BATCH_JOB_SEQ` | Renamed `BATCH_JOB_INSTANCE_SEQ` (migration scripts provided) |
| Boot starters | `spring-boot-starter-batch` (JDBC included) | `spring-boot-starter-batch-jdbc` (metadata in database) vs `spring-boot-starter-batch` (**no database, in-memory**) |
| Metrics | Global Micrometer registry | You must provide an `ObservationRegistry` bean |
| JSON | Jackson 2 | Jackson 3 for `JsonItemReader` / `JsonFileItemWriter` and for execution-context serialization |
| New in 6 | | Graceful shutdown on SIGTERM, `CommandLineJobOperator`, local chunking, remote step execution |

> **Important Boot 4 trap.** With Boot 4, if you use `spring-boot-starter-batch` (not `-jdbc`), Spring Batch runs with **resourceless in-memory metadata**. Nothing is stored in MySQL, and restartability across JVM restarts disappears. For anything you want to restart, use `spring-boot-starter-batch-jdbc`. On upgrade from Boot 3, Batch stops writing metadata to your existing database until you switch to the `-jdbc` starter.

### Imports in this guide

Code uses the **6.x packages**. If you are on 5.x, remove `.infrastructure` from item/repeat/support imports and use the old locations for `Job`, `Step`, listeners and `JobParameters`. When I am unsure an exact class name has stayed stable, I say so.

### How each topic is taught

Every topic follows your seven points: **Concept → Why → When → Internals → Example → Common mistakes → Production practice.** Where a point would be forced or empty, I skip it rather than pad it.

### What I could and could not verify

I checked the version-sensitive facts above against the Spring Batch 6.0 migration guide, the 6.0 "What's new" page, the Boot 4.0 migration guide and the 6.0.5 Javadoc. The workspace I wrote this in cannot reach Maven Central, so **the sample application in Part 4 was written carefully but not compiled or executed here.** Treat it as a design you will compile on day one; Part 4 lists the places most likely to need a small fix.

---

# Module 1 — Spring Batch Fundamentals

## 1.1 What is Spring Batch?

**Concept.** Spring Batch is a framework for building **bulk, non-interactive, restartable** jobs on the JVM. It does not schedule jobs and it does not provide a UI. It gives you:

- a programming model (`Job` → `Step` → read/process/write),
- **chunked transactions** so millions of records commit safely in small pieces,
- a **metadata store** (`JobRepository`) that records every run so jobs can be monitored and **restarted from where they failed**,
- ready-made **readers and writers** for files, JDBC, JPA and more,
- fault tolerance (**skip**, **retry**), listeners, scaling options (threads, partitions, remote workers).

**Why it exists.** Enterprises had the same problems in every nightly job: "What if it dies at record 4,000,000 of 5,000,000?", "How do I not load everything into memory?", "How do I know what ran yesterday?". Teams kept re-implementing these badly. Spring Batch is the standard answer.

**When to use it.** When the work is large, repeatable, can run without a user, and needs auditability or restart.

**When not to use it.** A one-off 500-row script, a simple `@Scheduled` method that calls one SQL statement, or a streaming/event workload (use Kafka Streams, Flink, etc.).

## 1.2 Batch vs real-time, online vs batch

```
ONLINE / REAL-TIME                         BATCH
-----------------------------------------  -----------------------------------------
One request = one unit of work             One run = millions of units of work
User waits for the answer                  Nobody waits; result checked later
Latency matters (ms)                       Throughput matters (records/second)
Triggered by a user or an event            Triggered by a schedule, file arrival, or an operator
Fails -> user sees an error, retries       Fails -> job is restarted, must not duplicate work
Typically 1 transaction per request        Typically 1 transaction per CHUNK
```

"Real-time" and "online" overlap but are not identical: *online* means a person is interacting; *real-time* means low-latency processing (it may be fully automated, e.g. stream processing). Batch is neither: it trades latency for throughput and control.

**Real example (your domain).** A REST endpoint `POST /payments` is online. The nightly job that settles all of today's payments with the bank, applies fees and produces a reconciliation file is batch.

## 1.3 ETL

**Extract → Transform → Load.** Spring Batch's `ItemReader` / `ItemProcessor` / `ItemWriter` map directly:

```
 EXTRACT                  TRANSFORM                  LOAD
 ItemReader     ──►      ItemProcessor     ──►      ItemWriter
 (CSV, DB, API)          (validate, enrich,         (DB, file, queue)
                          filter, convert)
```

Spring Batch is *an ETL-style engine for application code*. It is not a visual ETL tool (Informatica, Talend) and it is not a distributed data-processing engine (Spark). It shines when logic is business-heavy, lives in your Java codebase, and the volume is "large but fits a few machines".

## 1.4 Typical use cases

- Nightly interest/fee calculation, statement generation
- Importing partner files (CSV/XML/JSON) into your database
- Data migration between systems or schema versions
- Report generation and exports
- Reconciliation (compare two sources)
- Cleaning up expired data, archiving
- Bulk e-mail/notification preparation
- Re-indexing data into a search engine

## 1.5 Advantages and limitations

| Advantages | Limitations |
|---|---|
| Chunked transactions: bounded memory, bounded rollback | Not a scheduler (use cron, Kubernetes CronJob, Quartz, or an orchestrator) |
| Built-in restart from last commit | Metadata tables add write load (one update per chunk) |
| Rich readers/writers | Learning curve: many concepts (`JobInstance`, `JobExecution`, scopes) |
| Skip/retry/listeners built in | Not designed for sub-second latency or streaming |
| Scaling options without rewriting the job | Heavy for tiny jobs |
| Full run history for audit | Multi-node scaling needs extra infrastructure (messaging or shared DB) |

### Common mistakes (Module 1)
- Using Spring Batch for something a single SQL `INSERT ... SELECT` would do in one second.
- Expecting Spring Batch to trigger itself. You must trigger it (scheduler, REST call, CLI, file watcher).
- Treating it as "a loop with extra steps" and ignoring the metadata. The metadata *is* the product.

### Production practice (Module 1)
- Decide up front **who triggers jobs** and **how operators restart them**.
- Run each job in its own JVM/container when possible. The 6.0 migration guide itself recommends this pattern, since the domain model no longer loads "shallow" entities.

---

# Module 2 — Spring Batch Architecture

## 2.1 The big picture

```
                    ┌─────────────────────────────────────────────┐
  Trigger           │                 YOUR APPLICATION            │
 (cron, REST,  ───► │                                             │
  CLI, queue)       │   JobOperator ──► Job ──► Step ──► Step     │
                    │       │            │       │                │
                    │       │            │       ├─ Reader        │
                    │       │            │       ├─ Processor     │
                    │       │            │       └─ Writer        │
                    │       ▼            ▼       ▼                │
                    │        ┌───────────────────────┐            │
                    │        │      JobRepository     │            │
                    │        └───────────┬───────────┘            │
                    └────────────────────┼────────────────────────┘
                                         ▼
                         ┌──────────────────────────────┐
                         │ Metadata store (MySQL tables) │
                         │ BATCH_JOB_INSTANCE, ...       │
                         └──────────────────────────────┘
```

## 2.2 Each component

### Job
**Concept.** The top-level unit of work: a named, ordered flow of steps.
**Why.** Gives a unit you can start, stop, restart and report on.
**Internals.** Implements `org.springframework.batch.core.job.Job`. Built with `JobBuilder`. Executing a job creates/uses a `JobInstance` and a `JobExecution`.

### Step
**Concept.** One independent phase of a job. Two kinds: **tasklet** (one blob of work) and **chunk-oriented** (read/process/write loop).
**Why.** Steps are the unit of restart, of transactions, of skip/retry configuration, and of scaling.

### ItemReader / ItemProcessor / ItemWriter
- **Reader**: returns **one item per call**, `null` when exhausted.
- **Processor**: optional; transforms/validates/filters **one item**; returns `null` to filter it out.
- **Writer**: receives a **whole chunk** (a `Chunk<T>`) and writes it in one go.

### JobRepository
The persistence gateway for batch metadata. The framework *calls it constantly*: when a job starts, when each chunk commits, when a step ends. In v6 it also extends `JobExplorer`, so it can be queried too.

### JobLauncher (v5) → JobOperator (v6)
- In v5, `JobLauncher.run(job, params)` starts a job.
- In v6, `JobOperator` extends `JobLauncher`, and `jobOperator.start(job, jobParameters)` returns the `JobExecution`. `JobLauncher` is deprecated.
- `JobOperator` also offers `restart(JobExecution)`, `stop(JobExecution)`, `startNextInstance(Job)` and abandon.

### JobExplorer
Read-only view of metadata ("what ran, what failed?"). In v5 it is a bean you inject. In v6 it is deprecated, and you use `JobRepository` for queries.

### JobOperator
The operational API. In v5 it used string names and ids (`start(String jobName, Properties)`, `restart(long)`), mostly for JMX/management. In v6 the object-based methods (`start(Job, JobParameters)`, `restart(JobExecution)`) are the recommended ones, and the string/`long` forms are deprecated.

### ExecutionContext
A **key/value map persisted in the metadata store**. There are two scopes:
- **Step ExecutionContext**: saved at every chunk commit. This is how a reader remembers "I'm at line 4,000".
- **Job ExecutionContext**: saved when a step finishes. Used to pass small data between steps.

## 2.3 How they cooperate: a run in 12 lines

```
1. Trigger calls   jobOperator.start(job, params)
2. Operator asks   jobRepository: "does a JobInstance for (jobName + identifying params) exist?"
                    - no  -> create JobInstance + new JobExecution
                    - yes, last execution FAILED/STOPPED -> create new JobExecution (restart)
                    - yes, COMPLETED -> reject (JobInstanceAlreadyCompleteException)
3. Job.execute()   marks JobExecution STARTED, saves it
4. For each Step:  creates a StepExecution, saves it
5.   restart?      loads previous StepExecution's ExecutionContext so reader/writer can resume
6.   chunk loop:   begin tx -> read N items -> process each -> write chunk -> update step
                    ExecutionContext -> commit tx   (metadata update is part of the commit)
7. Step ends       status+counts saved; Job ExecutionContext updated
8. Job ends        JobExecution status and exit status saved
```

### Common mistakes (Module 2)
- Confusing `JobInstance` with `JobExecution` (Module 3 clears this up).
- Calling the Job as a normal bean method to "run it". You must go through `JobOperator`/`JobLauncher`, otherwise no metadata is created.
- On Boot 4, injecting `JobExplorer` and finding no bean: the default configuration no longer registers one.

### Production practice (Module 2)
- Expose a **thin** trigger layer (REST controller, scheduler, or CLI). Keep job logic out of it.
- Treat `JobRepository`'s database as critical infrastructure: back it up, size it, index it.

---

# Module 3 — Job Deep Dive

## 3.1 What is a Job, and how do you define one?

```java
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.job.parameters.RunIdIncrementer;

@Bean
Job importJob(JobRepository jobRepository, Step importStep, Step reportStep) {
    return new JobBuilder("importJob", jobRepository)
            .incrementer(new RunIdIncrementer())   // optional: auto-generate a run.id parameter
            .start(importStep)
            .next(reportStep)
            .build();
}
```

*v5 vs v6:* the same builder pattern works in both. `RunIdIncrementer` moved to `org.springframework.batch.core.job.parameters` in v6. In v6, when an incrementer is attached and you launch via `startNextInstance`, the framework computes the parameters and **ignores extra parameters you pass (with a warning)**.

## 3.2 JobInstance vs JobExecution vs JobParameters

This is *the* concept people get wrong.

```
Job "importJob"        (a definition: code)
   │
   ├── JobInstance #1   = job name + identifying parameters {runDate=2026-10-01}
   │       ├── JobExecution #1  (FAILED)     ← first attempt
   │       └── JobExecution #2  (COMPLETED)  ← restart of the same instance
   │
   └── JobInstance #2   = {runDate=2026-10-02}
           └── JobExecution #3  (COMPLETED)
```

| Concept | Meaning | Created when |
|---|---|---|
| **Job** | The definition (bean) | App starts |
| **JobParameters** | Inputs to a run, some **identifying**, some not | Each launch |
| **JobInstance** | *Logical run* = job name + **identifying** parameters. It is the identity | First launch with those identifying params |
| **JobExecution** | *One physical attempt* to run a JobInstance | Every launch/restart |

Rules that follow from this:

1. **One JobInstance can have many JobExecutions** (failure, then restart).
2. **A COMPLETED JobInstance cannot be run again.** Launching the same job name with the same identifying parameters throws `JobInstanceAlreadyCompleteException`. This is *deliberate*: it prevents double-processing "yesterday's file".
3. **Only one JobExecution of a JobInstance may run at a time.** Otherwise `JobExecutionAlreadyRunningException`.
4. **Non-identifying parameters do not create a new instance.** Use them for things like a timeout or an e-mail address.

```java
JobParameters params = new JobParametersBuilder()
        .addString("inputFile", "/data/in/tx-2026-10-01.csv")   // identifying (default)
        .addLocalDate("runDate", LocalDate.of(2026, 10, 1))      // identifying
        .addString("notifyEmail", "ops@example.com", false)      // NOT identifying
        .toJobParameters();
JobExecution execution = jobOperator.start(importJob, params);
```

*(v6 note: `JobParameter` is a record carrying its name, and `JobParameters` is built around a set of them. `JobParametersBuilder` keeps working in the same way for typical use.)*

### Common mistakes
- Re-running a completed job with the same parameters and being surprised by the exception. Either use a new identifying parameter, or use `RunIdIncrementer` + `startNextInstance` for "just run it again" jobs.
- Putting a changing timestamp in an identifying parameter for a job you *want* to restart: every launch becomes a brand-new instance, so restart never happens. For restartable jobs the identifying parameters must be **stable** (file name, business date).

## 3.3 Job lifecycle

```
          start()
 (none) ───────────► STARTING ───► STARTED ───► COMPLETED
                                      │  │
                                      │  └── step throws ────► FAILED
                                      │
                         stop signal  └───────────────────────► STOPPING ──► STOPPED
                                                                  (after current chunk)

 FAILED / STOPPED ── restart ──► new JobExecution ──► ...
 FAILED / STOPPED ── abandon ──► ABANDONED (cannot be restarted)
```

## 3.4 Job statuses

`BatchStatus` is the **framework's** view of what happened. `ExitStatus` is a **separate, string-based** result you can customize for flow decisions (e.g. `COMPLETED WITH SKIPS`).

| Status | Meaning | Restartable? |
|---|---|---|
| **STARTING** | The execution object exists; the job has not begun running steps yet | n/a (transient) |
| **STARTED** | Running now | n/a |
| **COMPLETED** | Finished successfully | **No** (instance is done) |
| **FAILED** | An unrecoverable error occurred | **Yes** |
| **STOPPED** | Stopped on request (`JobOperator.stop`), finished its current chunk, and saved state | **Yes** |
| **ABANDONED** | An operator declared it "will never be restarted" | **No** |
| *(also)* STOPPING, UNKNOWN | Stop requested but not finished; framework could not determine the state | UNKNOWN needs investigation |

### The "zombie STARTED" problem
If the JVM is killed (`kill -9`, OOM, node loss) mid-run, the execution row stays **STARTED** forever, because nobody could update it. A restart attempt then fails with `JobExecutionAlreadyRunningException`.

Handle it deliberately:
1. Confirm the process really is dead.
2. Mark the execution as FAILED/STOPPED (or use the 6.x operational APIs that recover/abandon executions; check your version's `JobOperator`/`CommandLineJobOperator` — the 6.0.5 Javadoc mentions recovering jobs).
3. Restart.

Spring Batch 6.0 also adds **graceful shutdown on SIGTERM**: the running step is stopped and the repository is updated into a consistent, restartable state. This greatly reduces zombie executions in Kubernetes.

## 3.5 Database view of a job

After two attempts of the same instance (first failed, second succeeded), you would see roughly:

`BATCH_JOB_INSTANCE`

| JOB_INSTANCE_ID | JOB_NAME | JOB_KEY |
|---|---|---|
| 1 | importJob | `a3f9...` (hash of identifying params) |

`BATCH_JOB_EXECUTION`

| JOB_EXECUTION_ID | JOB_INSTANCE_ID | STATUS | EXIT_CODE | START_TIME | END_TIME |
|---|---|---|---|---|---|
| 1 | 1 | FAILED | FAILED | 02:00:00 | 02:07:41 |
| 2 | 1 | COMPLETED | COMPLETED | 08:15:03 | 08:19:22 |

`JOB_KEY` is a hash of the identifying parameters, and (JOB_NAME, JOB_KEY) is unique. That uniqueness is what enforces rule 2 above.

## 3.6 Job restart in one paragraph (details in Module 12)

Restart means "create a new `JobExecution` for the **same** `JobInstance`, skip steps that already COMPLETED, and resume the failed step from its last committed `ExecutionContext`". Per step, `allowStartIfComplete` and `startLimit` change that behavior. Restart is only as good as your reader's ability to save its position, and your writer's idempotency.

### Production practice (Module 3)
- Name jobs and choose identifying parameters as if they were database keys.
- Alert on **FAILED**, **STOPPED for too long** and **STARTED for longer than the job's normal duration**.
- Never `UPDATE` metadata tables by hand unless you understand every column. Use the operator APIs.

---

# Module 4 — Step Deep Dive

## 4.1 What is a Step, and why is it separate from a Job?

A **Step** is an independent, self-contained phase. Why a separate concept?

- it is the **transaction and restart boundary** (completed steps are skipped on restart),
- it carries its **own fault tolerance** configuration,
- it is the **unit of scaling** (threads, partitions).

## 4.2 Step lifecycle and StepExecution

```
 STARTING → STARTED → COMPLETED
                │   ├─► FAILED
                │   └─► STOPPED
```

`StepExecution` (table `BATCH_STEP_EXECUTION`) is the live scoreboard of a step. These counters are the best debugging data you have:

| Field | Meaning |
|---|---|
| `readCount` | Items successfully read |
| `filterCount` | Items the processor returned `null` for |
| `writeCount` | Items successfully written |
| `commitCount` | Successful chunk commits (one more empty commit appears when the item count is an exact multiple of the chunk size, because end-of-input is only discovered on the next read) |
| `rollbackCount` | Chunk rollbacks |
| `readSkipCount` / `processSkipCount` / `writeSkipCount` | Skipped items per phase |
| `exitStatus` | Result string |
| `lastUpdated` | Heartbeat of progress |

Sanity formula: `readCount − filterCount − processSkipCount = writeCount` (roughly; write skips reduce it too). If these do not add up, you have a bug.

## 4.3 Tasklet step

**Concept.** A step whose whole body is one `Tasklet.execute(...)`, called **repeatedly** until it returns `RepeatStatus.FINISHED`. Each call runs in its own transaction.

```java
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.infrastructure.repeat.RepeatStatus;

@Bean
Step archiveFileStep(JobRepository jobRepository, PlatformTransactionManager txManager) {
    Tasklet tasklet = (contribution, chunkContext) -> {
        Files.move(Path.of("/data/in/tx.csv"), Path.of("/data/archive/tx.csv"),
                   StandardCopyOption.REPLACE_EXISTING);
        return RepeatStatus.FINISHED;     // CONTINUABLE would call it again
    };
    return new StepBuilder("archiveFileStep", jobRepository)
            .tasklet(tasklet, txManager)
            .build();
}
```

*v5 vs v6:* In v5 you **must** pass a `PlatformTransactionManager` to `.tasklet(...)`. In v6 the transaction manager for tasklet and chunk steps is **optional** (the default configuration can run with a resourceless transaction manager); passing yours is still the right choice when the tasklet touches your database. The migration guide shows `.tasklet(tasklet)` without a transaction manager.

**Use when:** file moves, calling a stored procedure, `TRUNCATE`, sending a notification, one bulk SQL statement. Anything that is "one action", not "many records".

**Do not use when:** you loop over many records inside the tasklet. You lose chunking, skip/retry, and per-chunk restart. This is the most common Spring Batch anti-pattern.

## 4.3.1 Tasklet that must be restart-aware

```java
public class DeleteOldRowsTasklet implements Tasklet {
    private final JdbcTemplate jdbc;
    public DeleteOldRowsTasklet(JdbcTemplate jdbc) { this.jdbc = jdbc; }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) {
        int deleted = jdbc.update("DELETE FROM txn_staging WHERE created_at < ? LIMIT 10000",
                                  LocalDate.now().minusDays(30));
        contribution.incrementWriteCount(deleted);
        return deleted == 10000 ? RepeatStatus.CONTINUABLE : RepeatStatus.FINISHED;
    }
}
```

Returning `CONTINUABLE` makes the framework call it again, each time in a **new transaction**, so a huge delete is split into bounded transactions. Because the delete is idempotent ("delete what is old"), a restart is safe.

## 4.4 Chunk-oriented step

**Concept.** Repeatedly: read `N` items → process each → write them all → commit. `N` is the **chunk size** (commit interval).

```java
import org.springframework.batch.core.step.builder.ChunkOrientedStepBuilder;

@Bean
Step importStep(JobRepository jobRepository,
                PlatformTransactionManager txManager,
                ItemReader<TxCsv> reader,
                ItemProcessor<TxCsv, Tx> processor,
                ItemWriter<Tx> writer) {
    return new ChunkOrientedStepBuilder<TxCsv, Tx>("importStep", jobRepository, 500)
            .reader(reader)
            .processor(processor)
            .writer(writer)
            .transactionManager(txManager)
            .build();
}
```

*v5 vs v6:*

```java
// v5 (and deprecated in v6)
new StepBuilder("importStep", jobRepository)
        .<TxCsv, Tx>chunk(500, txManager)
        .reader(reader).processor(processor).writer(writer)
        .build();

// v6 documented forms
new StepBuilder("importStep", jobRepository).chunk(500).transactionManager(txManager) /* ... */;
new ChunkOrientedStepBuilder<TxCsv, Tx>("importStep", jobRepository, 500) /* ... */;
```

The reference "What's new in 6" uses the `ChunkOrientedStepBuilder<I, O>(name, jobRepository, chunkSize)` form; I use it throughout. Check your exact 6.0.x Javadoc for the generics of `StepBuilder.chunk(int)` before relying on that shorter form (a 6.0.x fix added generic readers to the new builder).

**Use when:** per-record work over a dataset that may be large. This is 90% of real batch.

## 4.5 Choosing between them

| Question | Tasklet | Chunk |
|---|---|---|
| Is it one action? | ✔ | |
| Is it "for each record"? | | ✔ |
| Need skip/retry per record? | | ✔ |
| Need restart from the middle? | | ✔ |
| Data may not fit in memory? | | ✔ |

### Common mistakes (Module 4)
- Looping over records inside a tasklet.
- Using a chunk step for a single SQL statement (a tasklet with `JdbcTemplate.update` is simpler and faster).
- Forgetting that each `Tasklet.execute` call is a transaction, so a long-running tasklet holds a long transaction.

### Production practice (Module 4)
- Make steps **small and single-purpose**, so restart granularity is useful.
- Always set meaningful step names; they are keys in metadata and in your dashboards.
- Use `.startLimit(n)` and `.allowStartIfComplete(false)` (the default) consciously.

---

# Module 5 — Reader Deep Dive

## 5.1 `ItemReader` and how reading finishes

```java
public interface ItemReader<T> {
    T read() throws Exception;   // return the next item, or null when there are no more
}
```

**How does Spring Batch know reading is finished?** Only by **`read()` returning `null`**. The chunk loop is, in essence:

```java
List<I> items = new ArrayList<>();
for (int i = 0; i < chunkSize; i++) {
    I item = reader.read();
    if (item == null) { exhausted = true; break; }   // end of input
    items.add(item);
}
// process + write whatever we have (even a partial last chunk), commit
// if exhausted -> step finishes after this chunk
```

**Why `return null;` is essential.** It is the framework's only end-of-input signal.
- If your custom reader **never returns null**, the step **never ends** (an infinite loop reading forever).
- If it returns `null` **too early** (e.g. on an empty page mid-stream), the step ends **successfully with data missing**. That is silent data loss.
- Returning `null` once means "done"; a well-behaved reader keeps returning `null` if called again.

A last partial chunk is still processed: with 1,050 items and chunk size 500 you get chunks of 500, 500, 50.

## 5.2 Restartable readers: `ItemStream`

Most built-in readers also implement `ItemStream`:

```java
public interface ItemStream {
    void open(ExecutionContext ctx);     // restore position if restarting
    void update(ExecutionContext ctx);   // save position (called at each chunk commit)
    void close();                        // release resources
}
```

`update` writes the current position (e.g. `FlatFileItemReader.read.count`) into the **step ExecutionContext**, which is persisted **in the same transaction as the chunk commit**. On restart `open` reads it back and **skips ahead**. This is the whole mechanism behind restartability (Module 12).

> If your reader/writer implements `ItemStream` but is **not registered as a stream** on the step (it is not auto-registered when it is wrapped, e.g. by a composite or a proxy), `open/update/close` are never called. Symptoms: resource not opened, or no restart position saved. Use `.stream(reader)` on the step when you wrap readers/writers.

## 5.3 `FlatFileItemReader`

**Concept.** Reads a text file line by line, turns each line into an object.
**Internals.** `LineMapper` = `LineTokenizer` (split a line into fields) + `FieldSetMapper` (fields → object). Keeps a line counter in the ExecutionContext. Single-threaded and **not thread-safe**.

```java
@Bean
@StepScope   // so #{jobParameters[...]} can be resolved at run time (Module 10)
FlatFileItemReader<TxCsv> txReader(@Value("#{jobParameters['inputFile']}") String inputFile) {
    return new FlatFileItemReaderBuilder<TxCsv>()
            .name("txReader")                         // REQUIRED so the saved keys are unique
            .resource(new FileSystemResource(inputFile))
            .linesToSkip(1)                           // header
            .delimited().delimiter(",")
            .names("txId", "accountId", "amount", "currency", "txDate")
            .fieldSetMapper(fs -> new TxCsv(          // explicit mapping: no reflection, no conversion surprises
                    fs.readString("txId"),
                    fs.readString("accountId"),
                    fs.readBigDecimal("amount"),      // "abc" -> exception -> FlatFileParseException
                    fs.readString("currency"),
                    LocalDate.parse(fs.readString("txDate"))))
            .build();
}

public record TxCsv(String txId, String accountId, BigDecimal amount, String currency, LocalDate txDate) {}
```

Why an explicit `fieldSetMapper` instead of `.targetType(TxCsv.class)`? `targetType` uses `BeanWrapperFieldSetMapper`, which binds by property name and does **not** convert `java.time` types such as `LocalDate` unless you supply a `ConversionService`. An explicit mapper is shorter than the extra configuration and fails with clear errors.

| Advantages | Disadvantages |
|---|---|
| Streams: constant memory | Not thread-safe |
| Restart via line count | Malformed lines throw `FlatFileParseException` (use skip) |
| Handles delimited, fixed width, multi-line records | Restart assumes the file is **unchanged** since the failed run |

**Mistakes:** forgetting `.name(...)` (restart state keys clash, and builder validation fails); wrong `linesToSkip`; setting the file as a hard-coded path instead of a job parameter; encoding problems (set `.encoding("UTF-8")`).
**Production:** validate the file exists before the job (a `JobParametersValidator` or a first tasklet step); move processed files to an archive; keep the input file immutable once the job has started.

## 5.4 `JdbcCursorItemReader`

**Concept.** Opens **one SQL cursor** and streams rows with `ResultSet.next()`.
**Internals.** Holds one connection and one open `ResultSet` for the **entire step**; each `read()` maps the current row. Saves the row number to the ExecutionContext for restart.

```java
@Bean
JdbcCursorItemReader<Tx> pendingTxReader(DataSource ds) {
    return new JdbcCursorItemReaderBuilder<Tx>()
            .name("pendingTxReader")
            .dataSource(ds)
            .sql("SELECT tx_id, account_id, amount, currency, tx_date FROM txn_staging WHERE status = 'NEW' ORDER BY tx_id")
            .rowMapper((rs, n) -> new Tx(rs.getString(1), rs.getString(2), rs.getBigDecimal(3),
                                         rs.getString(4), rs.getObject(5, LocalDate.class)))
            .fetchSize(1000)
            .build();
}
```

| Advantages | Disadvantages |
|---|---|
| Fastest simple reader, one query | Holds a connection for the whole step |
| Constant client memory (if the driver streams) | **Not thread-safe**, so no multi-threaded step |
| Consistent snapshot-like view of one query | **MySQL driver buffers the whole result set by default** unless you enable streaming |

**MySQL specifics (important for you).** MySQL Connector/J loads the entire result set into memory unless you either set `useCursorFetch=true` in the JDBC URL *and* a positive `fetchSize`, or use `fetchSize = Integer.MIN_VALUE` (row-by-row streaming, which blocks other statements on that connection). Without one of these, a 5-million-row query can OOM your JVM no matter how small your chunk is. Test this with a real volume.

**Another trap — reading and writing the same table.** If the writer updates the rows the cursor is reading (`status 'NEW' → 'DONE'`) on a *different* connection, behavior depends on isolation and engine. Prefer reading from a stable key range, or a paging reader sorted by key (below).

**Mistakes:** huge fetch with no streaming; long-held cursor beating a DB/proxy idle timeout; reading with a non-deterministic order.
**Production:** use when the whole read fits in one reasonable-length query and you run single-threaded.

## 5.5 `JdbcPagingItemReader`

**Concept.** Reads data **page by page** with a fresh query per page using a **sort-key condition** (`WHERE id > :lastId ORDER BY id LIMIT :pageSize`), not `OFFSET`.
**Internals.** A `PagingQueryProvider` (database-specific) builds `first page` and `remaining pages` SQL. After each page it remembers the last sort-key value. Each page query is its own short statement, so **no long-held cursor**, and **it is thread-safe** (reads are synchronized), so it works with multi-threaded steps. The sort key **must be unique and stable**.

```java
@Bean
PagingQueryProvider txQueryProvider(DataSource ds) throws Exception {
    SqlPagingQueryProviderFactoryBean f = new SqlPagingQueryProviderFactoryBean();
    f.setDataSource(ds);
    f.setSelectClause("SELECT tx_id, account_id, amount, currency, tx_date");
    f.setFromClause("FROM txn_staging");
    f.setWhereClause("WHERE status = 'NEW'");
    f.setSortKeys(Map.of("tx_id", Order.ASCENDING));
    return f.getObject();
}

@Bean
JdbcPagingItemReader<Tx> pagedTxReader(DataSource ds, PagingQueryProvider qp) {
    return new JdbcPagingItemReaderBuilder<Tx>()
            .name("pagedTxReader")
            .dataSource(ds)
            .queryProvider(qp)
            .pageSize(1000)                 // set equal to (or a multiple of) the chunk size
            .rowMapper(txRowMapper())
            .build();
}
```

| Advantages | Disadvantages |
|---|---|
| Thread-safe, scalable | More queries (one per page) |
| No long-held cursor | Needs a unique, indexed sort key |
| Sort-key paging stays fast on deep pages (no large OFFSET) | **Dangerous if the writer changes the rows the query's WHERE clause selects** (e.g. `status='NEW'`): later pages shift; rows may be missed |

> **The classic bug.** Reading `WHERE status='NEW'` with *key-based* paging is safe even when you update status (the key condition moves forward). Reading with *page-number/offset* paging (e.g. `JpaPagingItemReader`) while updating the filtered column **skips rows**, because rows disappear from the filtered set as you page. Use a stable filter, or process in two phases.

**Production:** the default recommendation for large table reads, especially with parallelism.

## 5.6 `JpaPagingItemReader`

**Concept.** Reads with JPQL through an `EntityManager`, **one page at a time**, using `setFirstResult/setMaxResults` (offset paging).
**Internals.** Each page runs a query; entities are detached after the page (`transacted` setting matters), so the persistence context does not grow forever.

```java
@Bean
JpaPagingItemReader<Customer> jpaReader(EntityManagerFactory emf) {
    return new JpaPagingItemReaderBuilder<Customer>()
            .name("customerReader")
            .entityManagerFactory(emf)
            .queryString("SELECT c FROM Customer c WHERE c.active = true ORDER BY c.id")
            .pageSize(500)
            .build();
}
```

| Advantages | Disadvantages |
|---|---|
| Familiar JPQL/entities | **OFFSET paging**: slower on deep pages (MySQL scans and discards earlier rows) |
| Lazy associations resolvable in the processor | Easy to cause N+1 queries |
| | Risk of skipped rows if the filtered data changes while reading |
| | Entity overhead (memory, dirty checking) |

**Mistakes:** forgetting `ORDER BY` (non-deterministic paging); lazy collections accessed after the page's persistence context closed → `LazyInitializationException`; mutating the filtered column during the read.
**Production:** fine up to hundreds of thousands of rows when you need entity mapping. For millions, prefer JDBC key-based paging and map to DTOs or use projections.

## 5.7 `RepositoryItemReader`

**Concept.** Reads through a **Spring Data `PagingAndSortingRepository`** method that takes a `Pageable`.
**Internals.** Calls your repository method with `PageRequest.of(page, size, sort)` repeatedly. It is offset paging (`LIMIT/OFFSET` or equivalent) under the hood, so it shares JPA paging's characteristics, plus the repository abstraction.

```java
@Bean
RepositoryItemReader<Customer> repoReader(CustomerRepository repo) {
    return new RepositoryItemReaderBuilder<Customer>()
            .name("customerRepoReader")
            .repository(repo)
            .methodName("findByActiveTrue")
            .arguments(List.of())
            .sorts(Map.of("id", Sort.Direction.ASC))
            .pageSize(500)
            .build();
}
```

| Advantages | Disadvantages |
|---|---|
| Reuses repositories and derived queries | Offset paging performance |
| Little new SQL | Pageable method must be sortable/deterministic |
| | Count queries can add cost if your method returns `Page` (return `Slice`/`List` where possible) |

**Note for v6:** the removal list includes `MongoItemReader` / `Neo4jItemReader`; for those stores use `RepositoryItemReader` (Spring Data) or a custom reader.

## 5.8 Custom readers

When you need to read from a REST API, a message queue or a proprietary format.

```java
public class ApiPagedReader implements ItemStreamReader<Partner> {
    private final PartnerApiClient client;
    private final int pageSize;
    private int page;                       // restart state
    private Iterator<Partner> current = Collections.emptyIterator();
    private boolean exhausted;

    public ApiPagedReader(PartnerApiClient client, int pageSize) { this.client = client; this.pageSize = pageSize; }

    @Override public void open(ExecutionContext ctx) { page = ctx.getInt("api.page", 0); }
    @Override public void update(ExecutionContext ctx) { ctx.putInt("api.page", page); }
    @Override public void close() { }

    @Override
    public Partner read() {
        if (!current.hasNext() && !exhausted) {
            List<Partner> next = client.fetchPage(page, pageSize);
            if (next.isEmpty()) { exhausted = true; }   // an empty page really means "no more"
            else { current = next.iterator(); page++; }
        }
        return current.hasNext() ? current.next() : null;   // null = end of data
    }
}
```

Note one subtle restart bug above: `update` is called at the chunk boundary but `page` may already be *ahead* of what was actually processed (the current page was fetched but partially consumed). A correct restartable reader must save a position that maps exactly to "items fully committed". For anything non-trivial, store the **last committed item id** instead of a page counter, and filter on it when reopening.

**Rules for custom readers:** return `null` exactly at end of data; implement `ItemStream` for restart; do not make it thread-safe unless you synchronize `read()` (wrap with `SynchronizedItemStreamReader` for multi-threaded steps).

## 5.9 Choosing a reader

| Situation | Reader |
|---|---|
| CSV/fixed-width file | `FlatFileItemReader` |
| One large DB query, single thread | `JdbcCursorItemReader` (with MySQL streaming configured) |
| Large DB table, restart/parallel needed | `JdbcPagingItemReader` |
| Need JPA entities, moderate volume | `JpaPagingItemReader` / `RepositoryItemReader` |
| REST/queue/other | Custom reader + `ItemStream` |

### Production practice (Module 5)
- Align **page size** with **chunk size**.
- Always give readers a **unique `name`**.
- Prefer **DTO projections** for reads; entities are for writing logic you need JPA for.
- Add an index on the sort/filter columns; verify with `EXPLAIN`.
- Rehearse a restart: kill the job mid-run and confirm no duplicates and no gaps.

---

# Module 6 — Processor Deep Dive

## 6.1 `ItemProcessor`

```java
public interface ItemProcessor<I, O> {
    O process(I item) throws Exception;   // return transformed item, or null to FILTER it out
}
```

**Concept.** Per-item business logic between read and write: validate, transform, enrich, filter.
**Why.** Separates *what the data is* (reader/writer) from *what the business does with it* (processor), keeping each unit testable.
**Optional.** A step may have only a reader and writer.

## 6.2 `return null;` inside a processor

This means **"drop this item"**. It is **not** an error and **not** a skip:

- the item is **not** passed to the writer,
- `filterCount` increases,
- processing continues with the next item,
- the step does not fail, and no skip limit is consumed.

Contrast with the **reader**, where `null` means *end of data* (Module 5). Same value, opposite meaning. Know which component you are in.

| Outcome | How | Counted as |
|---|---|---|
| Keep and change item | return new/modified object | write count |
| **Filter** (a normal business decision, e.g. "amount is 0") | `return null` | `filterCount` |
| **Reject as invalid** (data error you want recorded) | throw an exception + configure **skip** | `processSkipCount` |
| Abort job | throw an unskippable exception | step FAILED |

Rule of thumb: **filter** things that are *normal and expected* (duplicates, zero amounts, records of other regions). **Skip** things that are *wrong* (invalid format, failed validation) so they are logged, counted and reported.

## 6.3 Validation, transformation, filtering: one example

```java
@Component
@StepScope
public class TxProcessor implements ItemProcessor<TxCsv, Tx> {

    private static final Set<String> CURRENCIES = Set.of("USD", "EUR", "BDT");

    @Override
    public Tx process(TxCsv in) {
        // VALIDATE -> exception => counts as a (skippable) failure
        if (in.amount() == null) throw new InvalidTxException("amount missing", in);
        if (!CURRENCIES.contains(in.currency())) throw new InvalidTxException("bad currency " + in.currency(), in);

        // FILTER -> normal business rule
        if (in.amount().signum() == 0) return null;

        // TRANSFORM + ENRICH
        BigDecimal fee = in.amount().abs().multiply(new BigDecimal("0.015")).setScale(2, RoundingMode.HALF_UP);
        return new Tx(in.txId(), in.accountId(), in.amount(), in.currency(), in.txDate(), fee);
    }
}
```

**Validation with Bean Validation:** `BeanValidatingItemProcessor<T>` (needs `spring-boot-starter-validation`). It throws `ValidationException` for invalid items, which you then configure as skippable.

**Chaining:** `CompositeItemProcessor` runs processors in sequence; if any returns `null`, the item is filtered and the rest of the chain is skipped.

```java
new CompositeItemProcessorBuilder<TxCsv, Tx>().delegates(validator, enricher, mapper).build();
```

## 6.4 Internals worth knowing

1. **The processor runs inside the chunk's transaction**, after all `N` items of the chunk are read. A failure in the processor rolls back that chunk (unless skip/retry is configured).
2. **On retry or rollback, processors run again for the same items.** In the classic fault-tolerant implementation, items read in a chunk are **cached** and *reprocessed* when the chunk is retried, so processors should be **idempotent and side-effect-free**. (You can mark the step `processorNonTransactional` / similar only if you really know your processor is safe; check the builder docs of your version.)
3. **Processor output type can differ from input type**, and the writer receives the output type.

## 6.5 When to avoid a processor

- **Heavy I/O per item** (a REST call or SQL lookup for each of 1M items). That is N+1 at scale. Preload reference data into memory (a `Map`) in a `@StepScope`/`@BeforeStep` method, or use SQL joins in the reader. If you must call a remote service, batch the calls in the writer or use retry plus a bulk endpoint.
- **Side effects** (sending e-mail, calling payment gateways). Retry or rollback will repeat them. Do side effects in an idempotent writer, with an idempotency key.
- **Logic that can be a single SQL statement** (`UPDATE ... SET fee = amount*0.015`). A set-based SQL tasklet will beat any row loop by orders of magnitude.
- **No transformation needed.** Then skip the processor entirely.

### Common mistakes (Module 6)
- Returning `null` for invalid data and silently losing it, with no record. Use skip + a `SkipListener` to log rejects.
- Mutating shared state (a counter, a `HashMap`) in a singleton processor; with multi-threaded steps this is a race.
- Throwing for normal business filters, which burns the skip limit.
- Doing `repository.save(...)` inside the processor; that bypasses the writer's transaction semantics and batching.

### Production practice (Module 6)
- Keep processors **pure**: input → output, no I/O.
- Count and **log rejects** with context (the record id, the reason).
- Unit-test processors with plain JUnit; they need no Spring context.


---

# Module 7 — Writer Deep Dive

## 7.1 `ItemWriter`

```java
public interface ItemWriter<T> {
    void write(Chunk<? extends T> chunk) throws Exception;   // receives the WHOLE chunk
}
```

**Concept.** Persists or sends a whole chunk of processed items.
**Why a chunk, not one item?** So the writer can use a **single batched operation** (JDBC batch, one bulk insert, one file flush) instead of N round trips. This is where most batch performance is won.
**Contract.** The writer runs **inside the chunk transaction**. If it throws, the transaction rolls back; the chunk's rows must not be partially visible.

How a batched write works in general:

```
chunk of 500 items
   │
   ▼
 writer.write(chunk)
   │   JDBC: PreparedStatement.addBatch() x500 -> executeBatch()  (1 round trip, or a few)
   ▼
 chunk transaction COMMIT  (together with the step ExecutionContext update)
```

## 7.2 `JdbcBatchItemWriter`

**Internals.** Uses `NamedParameterJdbcTemplate.batchUpdate` (or a `PreparedStatementSetter`) to run **one JDBC batch per chunk**. With `assertUpdates(true)` (default) it throws `EmptyResultDataAccessException` if any statement affected 0 rows, which is useful for `UPDATE ... WHERE version = ?` optimistic checks.

```java
@Bean
JdbcBatchItemWriter<Tx> txWriter(DataSource ds) {
    return new JdbcBatchItemWriterBuilder<Tx>()
            .dataSource(ds)
            .sql("""
                 INSERT INTO transactions (tx_id, account_id, amount, currency, tx_date, fee)
                 VALUES (:txId, :accountId, :amount, :currency, :txDate, :fee)
                 ON DUPLICATE KEY UPDATE amount = VALUES(amount), fee = VALUES(fee)
                 """)
            .itemSqlParameterSourceProvider(tx -> new MapSqlParameterSource()
                    .addValue("txId", tx.txId())
                    .addValue("accountId", tx.accountId())
                    .addValue("amount", tx.amount())
                    .addValue("currency", tx.currency())
                    .addValue("txDate", tx.txDate())
                    .addValue("fee", tx.fee()))
            .build();
}
```

I use an explicit `ItemSqlParameterSourceProvider` lambda because `BeanPropertyItemSqlParameterSourceProvider` relies on JavaBean getters (`getTxId()`), which Java **records** (`txId()`) do not have. (If you use records, this lambda form is the safe choice.)

| | |
|---|---|
| **Performance** | Best raw throughput for inserts/updates. |
| **Memory** | Holds one chunk. |
| **Transactions** | Joins the chunk transaction. |
| **MySQL tip** | Add `rewriteBatchedStatements=true` to the JDBC URL, otherwise Connector/J sends each batched statement separately, and batching gives little benefit. |
| **Idempotency** | `INSERT ... ON DUPLICATE KEY UPDATE` makes the write safe to repeat on restart. |

## 7.3 `JpaItemWriter`

**Internals.** For each item calls `entityManager.persist()` (or `merge()` when `usePersist(false)`, the default is merge), then `flush()` at the end of the chunk. Hibernate can batch the resulting inserts only if configured (`hibernate.jdbc.batch_size`, `hibernate.order_inserts`).

```java
@Bean
JpaItemWriter<CustomerEntity> jpaWriter(EntityManagerFactory emf) {
    return new JpaItemWriterBuilder<CustomerEntity>()
            .entityManagerFactory(emf)
            .usePersist(true)          // persist() for new entities; merge() issues a SELECT first
            .build();
}
```

| | |
|---|---|
| **Performance** | Slower than JDBC: entity lifecycle, dirty checking, and an extra `SELECT` for each `merge()`. |
| **Memory** | The persistence context holds the chunk's entities until commit; keep chunk size moderate. |
| **Transactions** | Needs a `JpaTransactionManager`, **the same one** the step uses. Mixing a `DataSourceTransactionManager` for the step with JPA writing will not behave as you expect. |
| **`IDENTITY` ids** | `GenerationType.IDENTITY` (MySQL `AUTO_INCREMENT`) **disables Hibernate JDBC batching for inserts**, because each insert must run immediately to get its id. For large imports, use a sequence-like strategy (MySQL has none natively; use a table generator, or write with JDBC). |

**Use when:** you need JPA lifecycle features (cascades, entity listeners, versioning) and volume is modest.

## 7.4 `RepositoryItemWriter`

**Internals.** Calls a Spring Data repository method (default `save`) for **every item** in the chunk. Thin wrapper over the repository; with `saveAll` (via `.methodName("saveAll")` on newer versions, check Javadoc) one call per chunk is possible.

```java
@Bean
RepositoryItemWriter<CustomerEntity> repoWriter(CustomerRepository repo) {
    return new RepositoryItemWriterBuilder<CustomerEntity>()
            .repository(repo)
            .methodName("save")
            .build();
}
```

| | |
|---|---|
| **Performance** | Per-item `save()` means a `SELECT` (existence check for entities with assigned ids) + insert/update per item. Convenient, not fast. |
| **Transactions** | Runs in the chunk transaction, but each `save` is its own `@Transactional` call that joins it. |

## 7.5 `FlatFileItemWriter`

**Internals.** Writes lines to a file through a `LineAggregator` (delimited or formatted). It **buffers** and **flushes at the chunk commit**; it is **transaction-aware**: on rollback, the file is truncated back to the last committed position, using the position it saves in the ExecutionContext. That is also what makes file output restartable.

```java
@Bean
@StepScope
FlatFileItemWriter<AccountSummary> reportWriter(@Value("#{jobParameters['reportFile']}") String out) {
    return new FlatFileItemWriterBuilder<AccountSummary>()
            .name("reportWriter")
            .resource(new FileSystemResource(out))
            .headerCallback(w -> w.write("accountId,txCount,totalAmount,totalFee"))
            .delimited().delimiter(",")
            // explicit extractor: the default bean-property extractor expects getX() getters, which records lack
            .fieldExtractor(s -> new Object[] { s.accountId(), s.txCount(), s.totalAmount(), s.totalFee() })
            .shouldDeleteIfExists(true)      // default true: a re-run overwrites; careful with restarts
            .build();
}
```

| | |
|---|---|
| **Memory** | One chunk plus a buffer. |
| **Restart** | Resumes appending at the saved offset (so don't let someone else modify the file between runs). |
| **Mistake** | `shouldDeleteIfExists(true)` + restart: the framework does handle restart by *not* deleting when restarting; but a *new* instance overwrites the old file. Name output files by run date. |

## 7.6 Custom `ItemWriter`

```java
@Component
public class PartnerApiWriter implements ItemWriter<Tx> {
    private final PartnerClient client;
    public PartnerApiWriter(PartnerClient client) { this.client = client; }

    @Override
    public void write(Chunk<? extends Tx> chunk) {
        // ONE bulk call per chunk, with an idempotency key derived from the chunk's first/last id
        client.sendBatch(chunk.getItems(), idempotencyKey(chunk));
    }
}
```

**Careful:** non-transactional resources (HTTP calls, message sends) are **not** rolled back with the database transaction. If the HTTP call succeeded but the DB commit then failed, the chunk will be retried, so the remote system **must** tolerate duplicates (idempotency keys). This is the most common production incident with custom writers.

## 7.7 Other useful writers
`CompositeItemWriter` (write to several targets), `ClassifierCompositeItemWriter` (route by type), `JsonFileItemWriter`, `KafkaItemWriter`, `JmsItemWriter`/`AmqpItemWriter`, `MongoItemWriter`.
*(v6: `CompositeItemWriter` can now delegate to writers that expect a different item type through a configured function. Check the 6.0 release notes if you need this.)*

### Common mistakes (Module 7)
- Using `RepositoryItemWriter` for a million-row import and wondering why it takes hours.
- Missing `rewriteBatchedStatements=true` on MySQL.
- `IDENTITY` ids with JPA batch writes.
- Non-idempotent external writes.
- Catching exceptions inside the writer and swallowing them, so the framework thinks the chunk succeeded.

### Production practice (Module 7)
- Prefer **JDBC batch writes** for volume; make every write **idempotent** (`ON DUPLICATE KEY UPDATE`, natural keys, or a "processed" marker).
- Use the **same transaction manager** for the step and the writer's datasource.
- Measure rows/second at your target chunk size before choosing a writer.

---

# Module 8 — Chunk Processing

## 8.1 The model

```
      ┌──────────────────────── ONE CHUNK = ONE TRANSACTION ────────────────────────┐
begin │ read() x N  ──►  process() each item  ──►  write(whole chunk)  ──►  update   │ commit
 tx   │ (until N or     (filter => null)           (one batch)             step ctx  │
      │  reader==null)                                                    + metadata │
      └──────────────────────────────────────────────────────────────────────────────┘
        repeat until the reader returns null
```

**Chunk** = the group of `N` items handled in one transaction. **Commit interval** = `N` (the chunk size parameter).

Precisely, in the standard implementation:
1. **Begin transaction.**
2. **Read** up to `N` items (one `read()` call per item). Stop early if `read()` returns `null`.
3. **Process** each item (outputs collected; `null` outputs dropped).
4. **Write** all outputs in one `write(chunk)` call.
5. **Update** the `StepExecution` counters and **save the step ExecutionContext** (this calls every `ItemStream.update`).
6. **Commit.** Business data and batch metadata commit **atomically**: that is why a restart never repeats committed work and never loses an uncommitted position.

*(v6 note: `ChunkOrientedStep` is the new implementation of this loop. Same model; different internals.)*

## 8.2 Transaction boundaries and rollback behavior

| What fails | Without fault tolerance | Effect |
|---|---|---|
| Reader throws | Step fails | Current chunk rolled back; step `FAILED`; earlier chunks stay committed |
| Processor throws | Step fails | Same |
| Writer throws | Step fails | Same, the whole chunk's write is rolled back |
| Commit itself fails (DB down) | Step fails | Same |

With **skip** or **retry** configured the behavior changes (Module 13). The key mental picture: **everything up to the last successful commit is durable, everything in the in-flight chunk is lost and will be redone on restart.**

## 8.3 Checkpoints and restartability

Each successful commit is a **checkpoint**: the ExecutionContext saved *in that commit* says "I finished through item #X". After a crash the next execution starts the step with that saved context; readers jump to that position (`open()`); the loop continues. **You re-do at most one chunk of work.** That is why your writes must be idempotent (or the chunk must be all-or-nothing, which a DB transaction gives you).

## 8.4 `.chunk(10)` vs `.chunk(100)` vs `.chunk(1000)`

Per chunk, the framework also does fixed work: a transaction begin/commit, **one metadata update** (step execution + context), and a flush of the batch. Chunk size trades these costs against memory and rollback cost.

| Chunk size | Metadata/commit overhead | Memory | Redo cost on failure | Typical effect |
|---|---|---|---|---|
| **10** | Very high: 100,000 commits per 1M rows, 100,000 metadata updates | Tiny | Tiny | Slow. Fine for debugging, for heavy per-item work (an external call per item), or when an item is huge (big blobs) |
| **100** | Moderate | Small | Small | Reasonable default for general work and JPA |
| **1000** | Low | Larger (1000 objects + JPA context) | Larger, and a failing item rolls back 1000 | Often fastest for JDBC batch inserts; long transactions hold locks longer |

Rules of thumb:
- Start at **500–1000 for JDBC** writers, **100–200 for JPA** writers.
- **Increase** until throughput stops improving. Beyond that you only add memory, lock time and redo cost.
- **Decrease** if: a lock-contention/lock-wait timeout appears, `OutOfMemoryError`, or one bad item rolls back too much work.
- Make **reader page size ≥ or = chunk size** (and ideally equal) so each page maps to a chunk.
- Chunk size is not "records per second": measure.

### Example: seeing the effect

```
1,000,000 rows, JDBC batch insert, rewriteBatchedStatements=true (illustrative, not a benchmark)

chunk=10    -> 100,000 commits   (metadata write per chunk dominates)
chunk=100   ->  10,000 commits
chunk=1000  ->   1,000 commits   (usually much faster than chunk=10)
chunk=10000 ->     100 commits   (diminishing returns; bigger rollback; memory pressure)
```

These numbers explain the *shape*; your real optimum depends on row width, indexes, network latency to MySQL, and the processor.

## 8.5 Completion policies

Besides a fixed size, a step can use a `CompletionPolicy` (e.g. time-based chunks, or a custom policy such as "chunk until 5 MB of data"). Rarely needed; mention it for interviews.

### Common mistakes (Module 8)
- Using `chunk(1)` "to be safe".
- Choosing a huge chunk and getting lock wait timeouts or OOM.
- Reader page size much smaller than chunk size (many page queries per chunk).
- Doing non-idempotent work, then being surprised that a failed chunk repeats it on restart.

### Production practice (Module 8)
- Make chunk size a **configuration property**, not a literal, so you can tune without redeploying.
- Record `commitCount`, `rollbackCount`, and read/write rates in your monitoring (Module 17).

---

# Module 9 — Transactions

## 9.1 Who owns the transaction?

The **step** (not you) opens and commits transactions. You supply a `PlatformTransactionManager` (v5: required; v6: optional but recommended when you write to a database). A **chunk** is one transaction (Module 8); in a tasklet step **each call to `execute`** is one transaction.

Two separate transactional concerns exist:
1. **Business transaction**: your reads/writes against your tables.
2. **Metadata updates**: `BATCH_*` tables, managed by `JobRepository`. The chunk's *step execution + context update is part of the chunk transaction*, so that the metadata and your data stay consistent. Job-level and step-start/end updates happen in **their own short transactions** managed by the repository.

> **Same database or not?** Normally business tables and metadata live in the same MySQL schema/datasource and one `DataSourceTransactionManager` or `JpaTransactionManager` covers both. If you separate them (a dedicated metadata DB), you lose single-transaction atomicity across both, and must reason about the "committed data but metadata not updated" window. This is why people keep them together. In v6, store-specific settings such as `dataSourceRef` live on `@EnableJdbcJobRepository`.

## 9.2 Commit, rollback, boundaries

```
 chunk 1   [begin ... write ... update ctx ... COMMIT]    -> durable
 chunk 2   [begin ... write ... update ctx ... COMMIT]    -> durable
 chunk 3   [begin ... write FAILS ... ROLLBACK]           -> nothing from chunk 3
 StepExecution status set to FAILED in a separate small transaction
```

## 9.3 Nested transactions and propagation

Spring Batch runs the chunk in a transaction with the isolation/propagation you configure on the step's transaction attributes. Do not open `REQUIRES_NEW` transactions inside processors/writers casually:

- Work in `REQUIRES_NEW` **commits even when the chunk rolls back**. That is correct for audit/"reject log" rows, but wrong for business data.
- Calling a `@Transactional` service method from a processor/writer joins the chunk transaction (default `REQUIRED`), which is usually what you want.

Isolation: MySQL InnoDB default is `REPEATABLE READ`. For metadata updates under concurrency, the 5.x/6.x docs recommend care with isolation level (the repository's default is `SERIALIZABLE` for creating job executions in older configurations; there was a Boot 4 / Mongo repository issue about that attribute, so check your configuration if you customize it).

## 9.4 What happens when something fails (without skip/retry)

**Reader fails** (e.g. malformed line, SQL timeout)
- Exception propagates; chunk rolls back; step `FAILED`.
- Items already read in this chunk are discarded (readers like `FlatFileItemReader` are re-positioned on restart from the *last committed* count, so those lines are re-read).
- Committed chunks remain.

**Processor fails** (validation exception)
- Rollback of the chunk; step `FAILED` unless the exception is configured skippable.
- With skip: the item is skipped, and the chunk **is re-processed without the bad item** (Module 13).

**Writer fails** (constraint violation, deadlock, connection loss)
- Rollback of the chunk. With skip configured, the framework switches to **scanning**: it re-runs the chunk **one item at a time** to find the bad item, skips it, and continues. That's expensive, and it's why a big chunk plus a poisonous record is painful (1000 → 1000 single-item transactions).
- Connection loss/deadlock are better handled by **retry** than by skip.

## 9.5 Examples

```java
// 1. Make a transaction explicit and shared (v5-style, still valid in v6)
@Bean
PlatformTransactionManager transactionManager(DataSource ds) {
    return new JdbcTransactionManager(ds);   // preferred over DataSourceTransactionManager
}
```

`JdbcTransactionManager` (from Spring Framework) translates SQL exceptions with the same exception translator as `JdbcTemplate`; the v6 migration guide uses it in its examples.

```java
// 2. Audit rejected records in a transaction that survives rollback
@Service
class RejectAudit {
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    void record(String txId, String reason) { /* insert into rejects ... */ }
}
```

### Common mistakes (Module 9)
- Two data sources, one transaction manager (writes to datasource B go outside the transaction).
- Calling an external system and assuming rollback undoes it.
- `@Transactional` on the processor/writer with `REQUIRES_NEW`, then wondering why "failed" chunk data exists.
- JPA: `flush` errors appear at commit time; stack traces point at `commit`, not at your `save`.

### Production practice (Module 9)
- Keep business data and metadata in the **same database**.
- Keep chunk transactions **short** (avoid minutes-long locks).
- Put non-transactional side effects behind idempotency keys.

---

# Module 10 — Job Parameters and Scopes

## 10.1 `JobParameters`

**Concept.** Typed inputs of a run. **Why.** The same Job, different data (file, business date). They are part of the job instance identity (when identifying).

Supported types: `String`, `Long`, `Double`, `Date`, `LocalDate`, `LocalTime`, `LocalDateTime` (and, in 5.x+, custom types via a converter).

**Passing them**

```java
// 1. Programmatically
JobParameters p = new JobParametersBuilder()
        .addString("inputFile", "/data/in/tx-2026-10-01.csv")
        .addLocalDate("runDate", LocalDate.parse("2026-10-01"))
        .toJobParameters();
jobOperator.start(importJob, p);

// 2. Spring Boot auto-run from the command line (name=value, optionally name=value,type,identifying)
//    java -jar app.jar inputFile=/data/in/tx.csv runDate=2026-10-01,java.time.LocalDate
//    (requires spring.batch.job.enabled=true, the default)
```

**Validating them early**

```java
return new JobBuilder("importJob", jobRepository)
        .validator(new DefaultJobParametersValidator(
                new String[] {"inputFile", "runDate"},   // required
                new String[] {"notifyEmail"}))           // optional
        .start(importStep)
        .build();
```

A failed validation throws `InvalidJobParametersException` (named `JobParametersInvalidException` in 5.x and early 6 milestones) and **no `JobExecution` is created**.

## 10.2 Why scopes exist

Spring creates singleton beans **at application startup**. But **job parameters do not exist at startup**; they exist only when a job is **launched**. If a reader bean needs `inputFile`, it cannot be built at startup. Batch solves this with two **custom Spring scopes**.

## 10.3 `@StepScope`

**Concept.** The bean is created **when the step starts**, **one instance per step execution**, and destroyed when the step ends.
**What you can inject with late binding:** `#{jobParameters['x']}`, `#{jobExecutionContext['y']}`, `#{stepExecutionContext['z']}`.

```java
@Bean
@StepScope
FlatFileItemReader<TxCsv> reader(@Value("#{jobParameters['inputFile']}") String file) { ... }
```

**Internals.**
1. At startup, Spring registers a **scoped proxy** (a CGLIB/interface proxy) in place of the real bean. No `inputFile` is needed yet.
2. When the step starts, `StepSynchronizationManager` binds the current `StepExecution` to the thread.
3. The first call through the proxy asks `StepScope` for the target bean. `StepScope` creates the real instance, evaluating `#{...}` against the current `StepExecution`, and caches it for that step execution.
4. When the step finishes, the cache is cleared and the instance is destroyed (so the next execution gets a fresh one).

Consequences:
- **Fresh state each execution**: a restarted step gets a new reader instance (state restored from `ExecutionContext` by `open`), not a leftover from the previous run.
- **Thread-local binding**: `@StepScope` beans work in multi-threaded steps because each worker thread resolves the same step execution (be careful: the bean is still one per step, so it must be thread-safe if shared).
- **A scoped bean method must return the most specific type** (e.g. `FlatFileItemReader<TxCsv>` not just `ItemReader`) so the proxy still exposes `ItemStream`. Returning the interface `ItemReader` can hide `ItemStream` from the step, so `open/update/close` are never called. This is a classic restart bug. If you must return a general type, register the stream explicitly: `.stream(reader)`.
- `@StepScope` is the default scope for the proxy mode `TARGET_CLASS`; classes must be non-final with a usable constructor (records, final classes and missing no-arg constructors can complicate proxying).

## 10.4 `@JobScope`

**Concept.** Same idea, but the bean lives for the whole **job execution** (all steps), and can bind `#{jobParameters[...]}` and `#{jobExecutionContext[...]}` (not step context).

Use it for something shared across steps but parameterized by the job, e.g. a `ReferenceData` holder loaded once per job from `#{jobParameters['runDate']}`.

| | `@StepScope` | `@JobScope` |
|---|---|---|
| Lifetime | One step execution | One job execution |
| Can read | job params, job ctx, step ctx | job params, job ctx |
| Typical use | readers, writers, processors needing params | shared per-job helpers |
| Thread binding | Step execution on the thread | Job execution on the thread |

## 10.5 Reading parameters elsewhere

```java
// In a Tasklet / listener: via the StepExecution/ChunkContext
String runDate = chunkContext.getStepContext().getJobParameters().get("runDate").toString();

// In a listener
@BeforeStep
public void before(StepExecution stepExecution) {
    JobParameters p = stepExecution.getJobParameters();
}
```

*v6 note:* `JobParameter` is a record that includes its own name, and `JobParameters` is a set-based structure. `getString("name")`-style accessors on `JobParameters` continue to be the normal way to read values.

### Common mistakes (Module 10)
- Putting `@Value("#{jobParameters[...]}")` on a **singleton** bean: you get a "no step/job scope active" error at startup.
- Forgetting `@StepScope` on the reader and hard-coding file names.
- Returning a scoped bean as a too-general type (loses `ItemStream`).
- Using `@Value("${...}")` (properties) and `#{jobParameters[...]}` (late binding) interchangeably: they resolve at different times.
- Using a changing timestamp as an identifying parameter on a job you want to restart.

### Production practice (Module 10)
- Name parameters consistently; document them.
- Validate them with a `JobParametersValidator`.
- Pass **paths and dates**, not blobs of data.

---

# Module 11 — Metadata Tables

Spring Batch's JDBC `JobRepository` uses six tables and three sequences (for MySQL, the sequences are implemented as tables). Schema scripts ship in the `spring-batch-core` jar (`org/springframework/batch/core/schema-mysql.sql`). In Boot, `spring.batch.jdbc.initialize-schema` controls creation (`embedded` by default; use `always` for development with MySQL, and a migration tool such as Flyway for production).

*v6 note:* the job-instance sequence is **`BATCH_JOB_INSTANCE_SEQ`** (renamed from `BATCH_JOB_SEQ`); migration scripts are in the `spring-batch-core` jar under `.../core/migration/6.0/`. v5 job instances that FAILED cannot be restarted under v6 (parameter serialization changed), so finish or abandon them before upgrading.

## 11.1 Relationships

```
BATCH_JOB_INSTANCE (1) ──< (N) BATCH_JOB_EXECUTION (1) ──< (N) BATCH_JOB_EXECUTION_PARAMS
                                  │  │
                                  │  └── (1:1) BATCH_JOB_EXECUTION_CONTEXT
                                  │
                                  └──< (N) BATCH_STEP_EXECUTION (1) ── (1:1) BATCH_STEP_EXECUTION_CONTEXT
```

## 11.2 Table by table

### `BATCH_JOB_INSTANCE` — the logical run
| Column | Meaning |
|---|---|
| `JOB_INSTANCE_ID` | PK (from `BATCH_JOB_INSTANCE_SEQ`) |
| `JOB_NAME` | The job's name |
| `JOB_KEY` | Hash of the identifying parameters. `(JOB_NAME, JOB_KEY)` is **unique** |
| `VERSION` | Optimistic locking |

### `BATCH_JOB_EXECUTION` — one physical attempt
`JOB_EXECUTION_ID`, `JOB_INSTANCE_ID` (FK), `CREATE_TIME`, `START_TIME`, `END_TIME`, `STATUS` (`BatchStatus`), `EXIT_CODE`, `EXIT_MESSAGE` (often a stack trace), `LAST_UPDATED`, `VERSION`.

### `BATCH_JOB_EXECUTION_PARAMS` — parameters of that attempt
5.x+ stores one row per parameter: `JOB_EXECUTION_ID` (FK), `PARAMETER_NAME`, `PARAMETER_TYPE` (Java class name), `PARAMETER_VALUE`, `IDENTIFYING` (`Y`/`N`).

### `BATCH_STEP_EXECUTION` — one step in one attempt
`STEP_EXECUTION_ID`, `JOB_EXECUTION_ID` (FK), `STEP_NAME`, `START_TIME`, `END_TIME`, `STATUS`, `COMMIT_COUNT`, `READ_COUNT`, `FILTER_COUNT`, `WRITE_COUNT`, `READ_SKIP_COUNT`, `WRITE_SKIP_COUNT`, `PROCESS_SKIP_COUNT`, `ROLLBACK_COUNT`, `EXIT_CODE`, `EXIT_MESSAGE`, `LAST_UPDATED`, `VERSION`.

### `BATCH_STEP_EXECUTION_CONTEXT` — **the checkpoint**
`STEP_EXECUTION_ID` (PK/FK), `SHORT_CONTEXT` (JSON/serialized map, up to ~2500 chars), `SERIALIZED_CONTEXT` (overflow, CLOB/TEXT). Contains e.g. `{"FlatFileItemReader.read.count": 4500, "batch.taskletType": "...", "batch.stepType": "..."}`.

### `BATCH_JOB_EXECUTION_CONTEXT` — job-level shared state
Same shape, keyed by `JOB_EXECUTION_ID`. Written at the end of each step. Used to pass small values between steps.

## 11.3 Data flow during a run

| Moment | What is written |
|---|---|
| Launch | `BATCH_JOB_INSTANCE` (if new), `BATCH_JOB_EXECUTION` (STARTING→STARTED), `BATCH_JOB_EXECUTION_PARAMS`, `BATCH_JOB_EXECUTION_CONTEXT` |
| Step start | `BATCH_STEP_EXECUTION` (STARTED) + context row |
| **Every chunk commit** | `UPDATE BATCH_STEP_EXECUTION` (counts) + `UPDATE BATCH_STEP_EXECUTION_CONTEXT` (checkpoint), **in the chunk transaction** |
| Step end | Final status/counts; job context updated |
| Job end | `BATCH_JOB_EXECUTION` status/exit code/end time |

Performance implication: **one metadata `UPDATE` pair per chunk**. This is the chunk-size cost from Module 8, and why metadata tables need to stay small and indexed.

## 11.4 Example: a failed run, then a restart

After `importStep` fails at row ~4,500 with chunk size 500:

```
BATCH_JOB_EXECUTION       id=1  instance=1  STATUS=FAILED
BATCH_STEP_EXECUTION      id=1  job_exec=1  step=importStep STATUS=FAILED  READ_COUNT=4500 COMMIT_COUNT=9 ROLLBACK_COUNT=1
BATCH_STEP_EXECUTION_CONTEXT id=1  {"FlatFileItemReader.read.count":4500}
```

Restart creates:

```
BATCH_JOB_EXECUTION       id=2  instance=1  STATUS=STARTED
BATCH_STEP_EXECUTION      id=2  job_exec=2  step=importStep   (copies id=1's context into the new execution's start)
   reader.open(ctx) -> sees read.count=4500 -> skips 4500 lines -> continues at 4501
```

## 11.5 Useful queries

```sql
-- Currently running or stuck
SELECT je.JOB_EXECUTION_ID, ji.JOB_NAME, je.STATUS, je.START_TIME, je.LAST_UPDATED
FROM BATCH_JOB_EXECUTION je JOIN BATCH_JOB_INSTANCE ji USING (JOB_INSTANCE_ID)
WHERE je.STATUS IN ('STARTED','STARTING','STOPPING');

-- Step statistics for the last run of a job
SELECT se.STEP_NAME, se.STATUS, se.READ_COUNT, se.FILTER_COUNT, se.WRITE_COUNT,
       se.PROCESS_SKIP_COUNT, se.WRITE_SKIP_COUNT, se.ROLLBACK_COUNT,
       TIMESTAMPDIFF(SECOND, se.START_TIME, se.END_TIME) AS seconds
FROM BATCH_STEP_EXECUTION se
WHERE se.JOB_EXECUTION_ID = (SELECT MAX(JOB_EXECUTION_ID) FROM BATCH_JOB_EXECUTION je
                              JOIN BATCH_JOB_INSTANCE ji USING (JOB_INSTANCE_ID) WHERE ji.JOB_NAME = 'importJob');
```

### Common mistakes (Module 11)
- Manually deleting rows and breaking foreign keys or restartability.
- Never purging old metadata: the tables grow forever and slow down every chunk commit and every `findJobInstance...` query.
- Running two applications against the same schema with **different Batch versions**.
- Storing large objects in the `ExecutionContext` (it is serialized and written at **every commit**).

### Production practice (Module 11)
- Create the schema with your migration tool, not auto-init.
- Add a **retention job** that purges old executions (a dedicated step with SQL deletes ordered child → parent; Spring Batch has `JobRepository` remove helpers in test utilities only).
- Keep `ExecutionContext` tiny (ids and counters).

---

# Module 12 — Restartability

## 12.1 Concept and rules

A failed or stopped job can be **resumed**: completed steps are not rerun, and the failed step resumes from its last checkpoint. It exists because "start over" is unacceptable for a 6-hour job.

Rules:

| Setting | Default | Effect |
|---|---|---|
| Job `restartable` | `true` (`.preventRestart()` disables) | Whether a failed instance may be restarted at all |
| Step `allowStartIfComplete` | `false` | A COMPLETED step is skipped on restart. Set `true` for steps that must run every time (validation, cleanup) |
| Step `startLimit` | unlimited | How many times a step may be started per instance |
| Reader/writer `saveState` | `true` | Whether positions are saved; set `false` for non-restartable/multi-threaded readers |

## 12.2 What the framework does on restart (internals)

```
jobOperator.restart(failedExecution)              // v6 (v5: jobOperator.restart(long id) or relaunch with same params via the launcher)
 1. Load the JobInstance and its last JobExecution; reject if COMPLETED/ABANDONED/still running
 2. Create a NEW JobExecution for the same JobInstance (same identifying params)
 3. For each step in the flow:
      - last StepExecution COMPLETED and !allowStartIfComplete -> skip (reuse its status)
      - otherwise create a new StepExecution and COPY the last execution's ExecutionContext
 4. step.execute():  reader.open(ctx) / writer.open(ctx) read their saved position
 5. continue the chunk loop from there
```

You can also restart by launching the **same job name with the same identifying parameters** after a failure: the repository finds the existing `JobInstance` and creates the next execution.

## 12.3 Making your own components restartable

```java
public class LastIdReader implements ItemStreamReader<Row> {
    private long lastCommittedId;

    @Override public void open(ExecutionContext ctx) { lastCommittedId = ctx.getLong("lastId", 0L); }

    @Override public void update(ExecutionContext ctx) { ctx.putLong("lastId", lastReturnedAndCommittedId()); }

    @Override public Row read() { /* SELECT ... WHERE id > :lastCommittedId ORDER BY id LIMIT ... */ return null; }

    @Override public void close() { }
}
```

`update()` is called inside the chunk transaction **after the writer**, so the stored position matches committed data.

## 12.4 What breaks restartability

- **Changing input between failure and restart** (a replaced file, rows inserted before the saved position). The saved *count/offset* no longer points to the same items.
- **Non-idempotent writes** where a chunk partially applied outside the transaction.
- **Multi-threaded steps with a stateful reader**: position is not meaningful, so use `saveState(false)` and design restart as "reprocess everything not marked done".
- **Transient identifying parameters** (timestamps) that create a new instance for every launch.
- **Different job definition on restart** (steps added/removed): the instance sees a flow it did not run.

## 12.5 Strategies

1. **Position-based** (`ItemStream` with line count/last id): the default.
2. **Marker-based**: process rows where `status='NEW'`, set `DONE` in the same transaction. A restart naturally resumes, and parallelism is simple. Needs an index on `status` (and the paging caveat from Module 5).
3. **Idempotent full re-run**: write with upserts; restart = rerun the step from the beginning. Simplest, and fine when the job is short.

### Common mistakes (Module 12)
- Assuming restart "just works" without testing it.
- Reader without `name`, so state keys clash or are not saved.
- Forgetting that restart reuses the **same parameters**; you cannot add new identifying parameters on restart.

### Production practice (Module 12)
- Include **a restart test** in CI: run, kill/fail mid-way (an injected exception at item N), restart, assert the final data equals an uninterrupted run.
- Document the operator procedure: how to find the failed execution, fix the cause, restart or abandon.

---

# Module 13 — Error Handling

## 13.1 The four tools

| Tool | Meaning | Use for |
|---|---|---|
| **Fail** (default) | Any exception fails the step | Truly unexpected errors |
| **Skip** | Drop the bad item, continue, record it | Permanently bad *data* (invalid record) |
| **Retry** | Try the same operation again | *Transient* failures (deadlock, timeout, network blip) |
| **Restart** | Fix the cause, relaunch later | Anything you cannot handle in-flight |

## 13.2 Configuration in Spring Batch 6

Fault tolerance is configured on the new `ChunkOrientedStepBuilder` with `.faultTolerant()`. **Always pair `skip(...)` with `skipLimit(...)` and `retry(...)` with `retryLimit(...)`**: in 6.0.0 the builder threw `IllegalArgumentException` when you configured one without the other (this was reported as a bug in the 6.0 RC series, and fixed in later releases; being explicit is correct in every version).

```java
@Bean
Step importStep(JobRepository jobRepository, PlatformTransactionManager tx,
                ItemReader<TxCsv> reader, ItemProcessor<TxCsv, Tx> processor,
                ItemWriter<Tx> writer, RejectSkipListener skipListener) {

    return new ChunkOrientedStepBuilder<TxCsv, Tx>("importStep", jobRepository, 500)
            .reader(reader).processor(processor).writer(writer)
            .transactionManager(tx)
            .faultTolerant()
            // SKIP: bad data
            .skip(FlatFileParseException.class)
            .skip(InvalidTxException.class)
            .skipLimit(100)
            // RETRY: transient infrastructure errors
            .retry(TransientDataAccessException.class)   // deadlock/timeouts are translated to this family
            .retryLimit(3)
            .skipListener(skipListener)
            .build();
}
```

In 5.x the equivalent is:

```java
new StepBuilder("importStep", jobRepository)
    .<TxCsv, Tx>chunk(500, tx)
    .reader(reader).processor(processor).writer(writer)
    .faultTolerant()
    .skip(FlatFileParseException.class).skip(InvalidTxException.class).skipLimit(100)
    .retry(TransientDataAccessException.class).retryLimit(3)
    .listener(skipListener)
    .build();
```

### Retry backoff (v6)
Retry in 6 is built on **Spring Framework 7's core retry** (`org.springframework.core.retry`), not the Spring Retry library. The builder exposes `.retryPolicy(org.springframework.core.retry.RetryPolicy)` and `.retryListener(org.springframework.core.retry.RetryListener)`. The reference documentation builds a policy with `RetryPolicy.builder()`; configure maximum attempts/retries, delay and which exceptions to include through that builder. **Check the method names against your exact Spring Framework 7.0.x Javadoc** (names such as `maxRetries` versus `maxAttempts` changed during the 7.0 milestones), and use `.retry(...)/.retryLimit(...)` if you do not need backoff.

Also from the 6.x issue tracker (worth knowing, and a reason to keep skip/retry explicit):
- Fault-tolerant `retry(Class)` **traverses exception causes**, while `skip(Class)` **does not** (it matches the thrown exception hierarchy only). A wrapped exception can be retryable but not skippable.
- A bug report covered the default skip policy when only retry is configured (items could be skipped after retry exhaustion instead of failing the step). Configure `skip` explicitly, or pass `new NeverSkipItemSkipPolicy()`, if you want "retry, then fail".

## 13.3 Skip, internals

```
process(item) throws InvalidTxException   ── skippable?, under skipLimit?
        │ yes                                │ no
        ▼                                    ▼
  roll back the chunk                   step FAILS
  re-process the chunk WITHOUT the bad item (reader items are cached)
  call SkipListener.onSkipInProcess(item, ex)
  increment processSkipCount
```

Where the exception happens changes the cost:
- **Reader skip** (bad line): cheap. The reader simply moves on, and the chunk is not redone.
- **Processor skip**: the chunk is rolled back and re-processed without that item.
- **Writer skip**: the framework cannot know *which* item failed in a batch write, so it **scans**: re-processes and re-writes the chunk **item by item** (chunk size 1 each) to find the culprit. A 1000-item chunk costs up to 1000 tiny transactions. Mitigation: validate in the processor so bad items fail *there*, not in the writer; keep chunk sizes sensible.
- Skip count over the limit → `SkipLimitExceededException` → step fails.

## 13.4 `SkipPolicy`

```java
public class BadDataSkipPolicy implements SkipPolicy {
    @Override
    public boolean shouldSkip(Throwable t, long skipCount) throws SkipLimitExceededException {
        if (t instanceof InvalidTxException || t instanceof FlatFileParseException) {
            if (skipCount >= 500) throw new SkipLimitExceededException(500, t);
            return true;
        }
        return false;     // everything else fails the step
    }
}
// .faultTolerant().skipPolicy(new BadDataSkipPolicy())
```

In v6 the skip feature is based **entirely on `SkipPolicy`**; the built-in `LimitCheckingExceptionHierarchySkipPolicy(skippableExceptions, skipLimit)` is what `skip(...)`/`skipLimit(...)` create (the old `LimitCheckingItemSkipPolicy` is deprecated).

## 13.5 Examples for your four scenarios

| Scenario | Technique | Why |
|---|---|---|
| **Invalid records** (bad CSV line, failed validation) | `skip` + `SkipListener` that writes the item and reason to a `rejects` table/file; a sensible `skipLimit` (e.g. 1% of input) | Permanent: retrying cannot help |
| **Database errors** | Constraint violation → **skip** (if data problem) or fix mapping; deadlock / lock wait timeout / `CannotAcquireLockException` → **retry**; connection failure → retry with backoff, then fail and restart | Distinguish *data* errors from *infrastructure* errors |
| **Network failures** (calling a partner API) | **retry** with exponential backoff; after exhaustion, **fail** (don't skip, to avoid silent loss) or skip only with a reject record | Transient |
| **Temporary failures** (rate limit, 503) | **retry** with delay; consider a circuit breaker outside the batch | Transient |

### Skip listener that records rejects

```java
@Component
public class RejectSkipListener implements SkipListener<TxCsv, Tx> {
    private static final Logger log = LoggerFactory.getLogger(RejectSkipListener.class);
    private final RejectAudit audit;
    public RejectSkipListener(RejectAudit audit) { this.audit = audit; }

    @Override public void onSkipInRead(Throwable t) {
        log.warn("Skipped unreadable line: {}", t.getMessage());
        audit.record("READ", null, t.getMessage());
    }
    @Override public void onSkipInProcess(TxCsv item, Throwable t) {
        log.warn("Rejected tx {}: {}", item.txId(), t.getMessage());
        audit.record("PROCESS", item.txId(), t.getMessage());
    }
    @Override public void onSkipInWrite(Tx item, Throwable t) {
        log.error("Write skip for tx {}: {}", item.txId(), t.getMessage());
        audit.record("WRITE", item.txId(), t.getMessage());
    }
}
```

Combined with `REQUIRES_NEW` on `RejectAudit.record` (Module 9), rejects are saved even when the chunk transaction rolls back.

*v6 note:* a 6.0.x issue reported that skip listeners registered through `ChunkOrientedStepBuilder#listener` were ignored; use the dedicated `.skipListener(...)` method shown above. Also note the generic parameters of the skip listener were refined in later 6.0.x releases (check your exact version if the generic signature differs).

## 13.6 Retry, internals

```
write(chunk) throws TransientDataAccessException
   │ retryable, attempts left?
   ▼
 wait (backoff) -> rollback chunk transaction -> re-run (re-read from cache, re-process, re-write)
   │ attempts exhausted
   ▼
 skippable? -> scan item by item, skip the culprit      not skippable -> step FAILS
```

Important: **a retry re-executes the processor for the chunk's items** (and re-calls the writer). Processors must be repeatable; writers must be idempotent.

### Common mistakes (Module 13)
- Retrying **non-transient** errors (a constraint violation will fail 3 times, then fail).
- Skipping everything (`skip(Exception.class)` with a huge limit). You will "succeed" with missing data.
- Skipping in the **writer** with a large chunk (the scan storm).
- No reject record: skipped items vanish.
- Retry around calls with side effects and no idempotency.

### Production practice (Module 13)
- **Classify** exceptions: data errors → skip; infrastructure errors → retry; anything else → fail loudly.
- Set `skipLimit` as a **safety valve** (a sudden 40% reject rate means the *file* is bad, so fail the job).
- Alert on skip counts, not only on failures; mark the exit status "COMPLETED WITH SKIPS" so operators notice (use a `StepExecutionListener.afterStep` to return a custom `ExitStatus`).
- Test fault tolerance explicitly with injected failures (Part 4 shows the shape).


---

# Module 14 — Performance Tuning

## 14.1 First principle: find the bottleneck before tuning

A chunk step is a pipeline of **read → process → write → commit+metadata**. Throughput equals the slowest stage. Measure per stage (Module 17) and tune the *slowest*:

| Symptom | Likely bottleneck | First fix |
|---|---|---|
| CPU idle, DB busy on inserts | Writer / indexes / commit | JDBC batch, `rewriteBatchedStatements`, fewer indexes during load, bigger chunk |
| CPU high on app | Processor / mapping / JSON | Profile; precompute lookups; avoid reflection-heavy mapping |
| DB busy on selects, many page queries | Reader (OFFSET paging, N+1) | Key-based paging, joins, align page size and chunk size |
| Throughput falls as data grows | Deep OFFSET paging, un-indexed filter, big metadata tables | Sort-key paging, indexes, metadata purge |
| `OutOfMemoryError` | Driver buffering whole result set, huge chunk, JPA context, large ExecutionContext | Streaming/paging, smaller chunk, `clear()` strategy, tiny context |
| Lock waits/deadlocks | Chunk too big, concurrent steps touching the same rows | Smaller chunks, partition by key range, retry on deadlock |
| Slow only at commit | Metadata updates or fsync | Larger chunk, faster storage, `innodb_flush_log_at_trx_commit` discussion with your DBA |

## 14.2 Chunk size optimization
Covered in Module 8. Method: run the same data at chunk sizes 100 / 500 / 1000 / 2000 / 5000, record rows/second and peak heap, pick the knee of the curve, then leave 20–30% headroom.

## 14.3 Reader optimization
- Use **JDBC readers** with a DTO row mapper, not JPA entities, for pure reads.
- **Cursor vs paging:** cursor for single-threaded single-pass reads with MySQL streaming configured; paging when you need parallelism or want short statements.
- **Index** the sort key and the `WHERE` columns. Check `EXPLAIN`.
- Avoid `OFFSET` paging on big tables (`JpaPagingItemReader`, `RepositoryItemReader`): MySQL scans and discards all earlier rows for every page.
- Select **only the columns you need**.
- Push joins/lookups into the **reader SQL**, not into a per-item processor lookup.

## 14.4 Writer optimization
- **JDBC batch** inserts; MySQL URL: `rewriteBatchedStatements=true` (also consider `useServerPrepStmts=false` with it, and `cachePrepStmts=true`).
- Use `INSERT ... ON DUPLICATE KEY UPDATE` for idempotent upserts.
- For JPA writers: `hibernate.jdbc.batch_size`, `order_inserts`, `order_updates`; avoid `IDENTITY` ids for bulk loads.
- Consider **`LOAD DATA LOCAL INFILE`** in a tasklet for pure file → table bulk loads of millions of rows (an order of magnitude faster than any row-wise writer), accepting that you lose per-record skip/retry.
- Write in **primary key order** to reduce InnoDB page splits.

## 14.5 Database optimization
- **Same schema** for business tables and batch metadata, **separate** from your OLTP traffic if possible (a replica or a batch-window schedule).
- **Drop/disable non-essential secondary indexes** during huge initial loads and rebuild afterwards (only when you control the table and window).
- Right-size the **connection pool**: a multi-threaded step needs `threads + a few` connections; the pool must not be the bottleneck, and the DB must tolerate it.
- Watch InnoDB **redo log/flush** settings, **buffer pool** size, **`max_allowed_packet`** (large batches), and **lock wait timeout**.
- Keep **metadata tables small**: purge old executions on a schedule.

## 14.6 Memory optimization
- Never collect all items in a list (a reader that loads "everything", then returns items one by one, defeats chunking).
- Keep the **`ExecutionContext` tiny**; it is serialized at each commit.
- JPA: keep chunk modest; consider `entityManager.clear()` patterns in custom writers; use **projections/DTOs** for reads.
- MySQL: enable result-set streaming/cursor fetch for cursor readers.
- Reuse expensive objects (`ObjectMapper`, `DateTimeFormatter`, `Pattern`) as fields; don't create them per item.
- Size the heap for **chunk × object size × threads**, plus lookups held in memory.
- Java 21: consider **virtual threads** carefully for I/O-bound steps (see 15.1); CPU-bound work gets no benefit.

## 14.7 Recommendations by volume

These are starting points, not guarantees. Always measure.

### ~10,000 records
- **Keep it simple.** Chunk 100–500. Any reader works; `FlatFileItemReader` or `JpaPagingItemReader` is fine. JPA writer is acceptable.
- Single thread. The whole job typically takes seconds.
- Don't over-engineer; correctness, skip/reject handling and restart matter more than speed here.

### ~100,000 records
- **JDBC writer** with batching (JPA is now noticeably slower, still acceptable if needed).
- Chunk 500–1000, `pageSize` = chunk size.
- Single thread usually completes in minutes. Add indexes on read filters. Verify MySQL batching flags.

### ~500,000 records
- **JDBC reader** (key-based paging, or a cursor with streaming) + **JDBC batch writer**.
- Chunk 1000. Eliminate per-item lookups (preload into a map or join in SQL).
- Consider a **multi-threaded step** (4–8 threads, thread-safe reader) *only if* a single thread misses your window and the bottleneck is not the database.
- Watch heap and connection pool sizing. Purge old metadata.

### ~1,000,000+ records
- **Design for parallelism and restart**: **partitioning** by key range (Module 15) is the preferred scaling option over a multi-threaded step, because each partition has its own reader state and restart position.
- JDBC paging reader per partition, JDBC batch writer, chunk 1000–2000, idempotent upserts.
- Consider **set-based SQL** (`INSERT ... SELECT`, `LOAD DATA`) for steps that don't need per-record logic.
- Tune the database (buffer pool, redo logs), disable unneeded indexes during load, monitor lock waits.
- Run in a **dedicated JVM/container** with a sized heap, and define a **restart runbook**.

### Example `application.yml` knobs (expose, don't hard-code)

```yaml
batch:
  tx:
    chunk-size: 1000
    page-size: 1000
    partition-grid-size: 8
    skip-limit: 500
spring:
  datasource:
    url: jdbc:mysql://db:3306/batchdb?rewriteBatchedStatements=true&cachePrepStmts=true&useCursorFetch=true
```

### Common mistakes (Module 14)
- Tuning chunk size blindly instead of finding the bottleneck.
- Adding threads to a DB-bound job, which only adds contention.
- Per-item remote calls or per-item SQL lookups.
- Benchmarking on 1,000 rows and extrapolating.

---

# Module 15 — Scaling Spring Batch

Pick the **simplest** option that meets your window. Complexity rises quickly down this list.

```
Single thread  →  Multi-threaded step  →  Parallel steps  →  Local partitioning
                                                              →  Remote partitioning / Remote chunking
```

## 15.1 Multi-threaded step

**Concept.** One step, one reader/processor/writer, but **chunks executed by several threads** from a `TaskExecutor`.
**Why.** Cheapest way to parallelize a CPU- or I/O-bound step.
**Internals.** The step hands each chunk to a worker thread. Each thread runs read→process→write→commit as **its own transaction**. The **reader is shared**, so it must be **thread-safe** (`JdbcPagingItemReader` is; `FlatFileItemReader` and `JdbcCursorItemReader` are not; wrap with `SynchronizedItemStreamReader`). Position tracking becomes meaningless (chunks complete out of order), so set `saveState(false)` on the reader, which **sacrifices restart-from-position**.

```java
@Bean
TaskExecutor batchTaskExecutor() {
    ThreadPoolTaskExecutor ex = new ThreadPoolTaskExecutor();
    ex.setCorePoolSize(8); ex.setMaxPoolSize(8); ex.setThreadNamePrefix("batch-");
    return ex;
}
// 5.x: .chunk(1000, tx)....taskExecutor(batchTaskExecutor())   (throttleLimit was removed in 6)
```

*v6 note:* the new `ChunkOrientedStep` supports concurrency too, but there were documentation corrections in 6.0.x ("Incorrect documentation about concurrent steps in v6", "Missing documentation about transaction management in multi-threaded steps"). **Read the current reference page for concurrent steps** before using it, rather than assuming v5 behavior. The `throttleLimit` method was **removed** in v6; control concurrency with the executor's pool size.

| Pros | Cons |
|---|---|
| Few lines of config | Reader must be thread-safe |
| Good for CPU/I/O-bound processors | No ordered processing; restart-by-position lost |
| | DB contention and connection pool pressure |

**Use for:** processors with real compute or remote latency, and idempotent or marker-based restarts.
**Java 21 note:** a `SimpleAsyncTaskExecutor` with virtual threads can help blocking-I/O-heavy steps. It does **not** speed up DB-bound or CPU-bound work, and the connection pool is still the cap.

## 15.2 Parallel steps (split flows)

**Concept.** Run **different steps at the same time** inside one job.
**Internals.** A `FlowBuilder.split(taskExecutor)` runs independent flows concurrently; the job continues when all finish.

```java
Flow loadCustomers = new FlowBuilder<Flow>("loadCustomers").start(customerStep).build();
Flow loadProducts  = new FlowBuilder<Flow>("loadProducts").start(productStep).build();

Job job = new JobBuilder("loadJob", jobRepository)
        .start(new FlowBuilder<Flow>("parallel")
                 .split(new SimpleAsyncTaskExecutor()).add(loadCustomers, loadProducts).build())
        .next(reportStep)
        .build().build();
```

| Pros | Cons |
|---|---|
| Easy; steps remain independent and restartable | Only helps if you have independent steps |
| | Shared DB resources can still bottleneck |

**Use for:** independent loads that feed a later step.

## 15.3 Partitioning (local)

**Concept.** Split the data into **partitions** (e.g. id ranges); run the same **worker step** once per partition, in parallel threads in the **same JVM**.
**Why.** Gives parallelism **and keeps restart per partition**: each partition has its own `StepExecution` and `ExecutionContext`, so its reader can use a normal, stateful, non-thread-safe reader.

**Internals.**

```
                    ┌──────────────────────────┐
                    │      Manager step        │
                    │  Partitioner.partition() │──► {p0:{min:1,max:250k}, p1:{...}, ...}
                    └───────────┬──────────────┘
                                │ PartitionHandler (TaskExecutorPartitionHandler)
        ┌───────────────┬───────┴───────┬───────────────┐
        ▼               ▼               ▼               ▼
   worker step     worker step     worker step     worker step     each has its own
   (partition 0)   (partition 1)   (partition 2)   (partition 3)   StepExecution + context
        │               │               │               │
        └───────────────┴──── StepExecutionAggregator ──┘  → manager status = combined
```

```java
public class RangePartitioner implements Partitioner {
    private final JdbcTemplate jdbc;
    public RangePartitioner(JdbcTemplate jdbc) { this.jdbc = jdbc; }

    @Override
    public Map<String, ExecutionContext> partition(int gridSize) {
        long min = jdbc.queryForObject("SELECT MIN(id) FROM transactions", Long.class);
        long max = jdbc.queryForObject("SELECT MAX(id) FROM transactions", Long.class);
        long size = (max - min) / gridSize + 1;
        Map<String, ExecutionContext> result = new LinkedHashMap<>();
        for (int i = 0; i < gridSize; i++) {
            ExecutionContext ctx = new ExecutionContext();
            ctx.putLong("minId", min + i * size);
            ctx.putLong("maxId", Math.min(max, min + (i + 1) * size - 1));
            result.put("partition" + i, ctx);
        }
        return result;
    }
}

@Bean
Step managerStep(JobRepository jobRepository, Step workerStep, RangePartitioner partitioner, TaskExecutor exec) {
    return new StepBuilder("managerStep", jobRepository)
            .partitioner("workerStep", partitioner)
            .step(workerStep)
            .gridSize(8)
            .taskExecutor(exec)
            .build();
}

// the worker's reader is @StepScope and binds #{stepExecutionContext['minId']} / ['maxId']
```

*v6 note:* the partitioning API moved to `org.springframework.batch.core.partition` (`Partitioner`, `PartitionNameProvider`, `PartitionStep`, `StepExecutionAggregator`).

| Pros | Cons |
|---|---|
| Restart works per partition | More code (partitioner, scoped worker beans) |
| Reader/writer stay simple, no thread-safety needs | Skewed partitions finish at different times |
| Predictable contention: disjoint key ranges | Need a good partition key |

**Use for:** the default scale-out for 1M+ rows on one machine.

## 15.4 Remote partitioning

**Concept.** The same partitioning, but **workers are other JVMs/nodes** reached over messaging (Spring Integration with RabbitMQ/Kafka/JMS).
**Internals.** The manager step writes **partition requests** (just a `StepExecution` id, not the data) to a queue. Workers pick requests up, load the `StepExecution` from the **shared `JobRepository`** (the same database), execute their partition, and update the shared metadata. The manager polls (or receives replies) until all partitions finish. Only metadata and small messages cross the wire; **each worker reads its own data directly from the source**.

```
Manager JVM:  partitioner ──► request queue ──────────────┐
                                                           ▼
Worker JVM 1..N: listen on queue ► run worker step ► write results to DB ► update shared BATCH_* tables
Manager:  waits (polling the repository or a reply channel) ► aggregates
```

| Pros | Cons |
|---|---|
| Horizontal scale; failure isolation per partition | Needs a broker and a shared database |
| Data does not travel through the manager | Operations complexity (monitoring queue + workers) |
| Restartable per partition | Network/DB becomes the new bottleneck |

*v6 additions:* **remote step execution** and **local chunking** are new in 6.0 (see the "What's new" page) and the Integration builders were renamed/adjusted (`RemotePartitioningManagerStepBuilder`, constructor changes, `ChunkHandler` → `ChunkRequestHandler`). Re-read the Spring Batch Integration reference for your exact version before copying v5 samples.

## 15.5 Remote chunking

**Concept.** The **manager reads** items and sends **chunks of items over messaging** to **workers that process and write**.
**Internals.** Manager: `ItemReader` + `ChunkMessageChannelItemWriter` sends the raw items; Worker: `ChunkProcessorChunkHandler` runs processor + writer and sends back an acknowledgment. The manager's chunk transaction waits for replies.

| Pros | Cons |
|---|---|
| Offloads **expensive processing/writing** | **Items travel over the network** (must be serializable), manager reading is a **single-thread bottleneck** |
| Simple data source (one reader) | The broker must be **durable**; failures need careful design |
| | Less restart-friendly than partitioning |

**Use for:** cheap reads, very expensive per-item processing (e.g. heavy CPU, ML scoring) that you want on many nodes. **Prefer remote partitioning** when workers can read the data themselves.

## 15.6 Choosing

| Need | Choose |
|---|---|
| Faster CPU/IO-bound processor, easy | Multi-threaded step (accept restart limits) |
| Independent steps | Parallel flow |
| 1M+ rows, one machine, restartable | **Local partitioning** |
| More than one machine | Remote partitioning (or just run **many independent jobs** on separate key ranges, which is often the simplest "distributed" solution) |
| Heavy processing, light reading | Remote chunking |

> **Cloud-native reality check.** A lot of teams skip remote partitioning and instead launch **N independent job instances** (each with parameters `shard=0..N-1`) as separate Kubernetes Jobs. You get isolation, simple restart per shard and no broker. Consider that before building a messaging topology.

### Common mistakes (Module 15)
- Multi-threading a non-thread-safe reader.
- Parallelizing a DB-bound job and increasing lock contention.
- Partitions that overlap, or leave gaps at the boundaries.
- Forgetting that the connection pool must be at least the thread/partition count plus overhead.
- Choosing remote scaling when a bigger single node would do.

### Production practice (Module 15)
- Parallelism level is a **config property**.
- Test partition boundaries with a "count by partition = total count" assertion.
- Monitor per-partition progress (each is a `StepExecution`).

---

# Module 16 — Interview Preparation

Answers are written the way a strong candidate would say them aloud.

## 16.1 Beginner

**Q1. What is Spring Batch and when would you use it?**
A framework for building bulk, non-interactive, restartable processing jobs. You use it when you must process large volumes with transactional safety, a history of runs, and the ability to restart after failure: file imports, nightly calculations, migrations, reports. You don't use it for tiny scripts or for real-time streaming.

**Q2. Explain Job, Step, and the Reader/Processor/Writer.**
A Job is a named flow of Steps. A chunk-oriented Step repeatedly reads items one at a time, optionally processes each, and writes a chunk at once, committing a transaction per chunk. A tasklet step runs a single unit of work.

**Q3. What is a JobInstance vs a JobExecution?**
A JobInstance is the logical run, identified by job name plus identifying parameters. A JobExecution is a single attempt at running it. One instance can have several executions (a failed one, then a successful restart). A completed instance cannot run again.

**Q4. What is the JobRepository?**
The component that persists batch metadata (instances, executions, step executions, execution contexts) into the database. It's how the framework knows what ran, what failed, and where to restart. In Spring Batch 6 it also extends `JobExplorer`.

**Q5. What does `ItemReader.read()` returning null mean? And a processor returning null?**
Reader null = end of input, and the step finishes after the last chunk. Processor null = filter this item out: it isn't written, filterCount increments, nothing fails.

**Q6. What is a chunk and a commit interval?**
A chunk is the group of items handled in one transaction; the commit interval is its size. Each chunk is read, processed, written, and committed atomically along with the metadata update.

**Q7. How do you start a job?**
Through `JobOperator.start(job, params)` in Spring Batch 6 (previously `JobLauncher.run`). Spring Boot can auto-run jobs at startup (`spring.batch.job.enabled`); in production you typically disable that and trigger via a scheduler, CLI, or REST endpoint.

**Q8. Tasklet vs chunk?** One action vs per-record processing; see Module 4.

## 16.2 Intermediate

**Q9. What happens when a job fails halfway and you restart it?**
A new JobExecution is created for the same JobInstance. Completed steps are skipped (unless `allowStartIfComplete`). The failed step starts with the ExecutionContext saved at its last committed chunk, so the reader skips ahead and processing resumes. At most one chunk of work is redone, so writes must be idempotent.

**Q10. Why does `@StepScope` exist and how does it work?**
Job parameters aren't known at startup, so beans needing them can't be singletons. `@StepScope` registers a proxy at startup; the real bean is created lazily when the step starts, with `#{jobParameters[...]}` resolved against the current StepExecution, then destroyed at step end. It also gives each execution a fresh instance. Return the specific type (e.g. `FlatFileItemReader`) so `ItemStream` isn't hidden.

**Q11. How do skip and retry differ, and how do you configure them?**
Skip drops a permanently bad item and continues, bounded by a skip limit; retry re-attempts an operation for transient failures. In 6 you configure both on `ChunkOrientedStepBuilder.faultTolerant()` with `skip/skipLimit` and `retry/retryLimit` (or `skipPolicy`/`retryPolicy`), plus listeners. Retry is now built on Spring Framework 7 retry.

**Q12. Why is a writer skip more expensive than a reader skip?**
When a batch write fails the framework doesn't know which item caused it, so it rolls back and re-processes/re-writes the chunk item by item to find it. A reader skip just moves to the next line.

**Q13. What chunk size would you choose and why?**
Start with 500–1000 for JDBC batch writes, 100–200 for JPA, and measure. Too small means lots of commit and metadata overhead; too large means memory, lock time, and costly rollbacks. Align reader page size with it.

**Q14. Cursor reader vs paging reader?**
Cursor: one query, one held connection, not thread-safe, fastest simple path (with MySQL streaming configured). Paging: a query per page using a unique sort key, no long-held cursor, thread-safe, works with parallelism. Offset-based JPA/Repository paging is slower on deep pages.

**Q15. How do you pass data between steps?**
Small values via the Job `ExecutionContext` (using an `ExecutionContextPromotionListener` to promote from step to job context). Large data goes through a table or file, not the context.

**Q16. What are identifying parameters?**
Parameters that, with the job name, define the JobInstance. Changing them creates a new instance; non-identifying ones don't. Using a timestamp as an identifying parameter prevents restart.

## 16.3 Senior-level

**Q17. A 6-hour job died at hour 5. How do you make sure the restart is safe and fast?**
Design it before the failure: position-saving readers (or marker columns), idempotent writes (upserts or natural keys), small ExecutionContext, steps split so completed phases aren't repeated, stable identifying parameters, and a tested restart path. Operationally: check the execution's status (zombie STARTED vs FAILED), fix the root cause, verify the input is unchanged, restart via JobOperator, then reconcile counts.

**Q18. How would you process 50 million rows nightly in a two-hour window?**
Measure the single-thread rate first. If insufficient: JDBC key-based paging reader, batch writer with `rewriteBatchedStatements`, chunk 1000–2000, push joins into SQL, set-based SQL for steps that allow it, then partition by key range (8–16 partitions) so each has its own restartable reader. If one box isn't enough, run sharded independent jobs or remote partitioning. Tune the database (indexes, flush settings), keep metadata small, and watch lock waits.

**Q19. How do you guarantee exactly-once effects when writing to a database and calling an external API in the same chunk?**
You can't get a distributed transaction cheaply. Make each side idempotent: the DB write as an upsert; the API call carrying an idempotency key derived from stable item ids. Or use an outbox table written in the same chunk transaction and let another step/process deliver it.

**Q20. Explain what changed in Spring Batch 6 and what you'd check when migrating.**
Spring Framework 7 / Spring Data 4 baseline; `@EnableBatchProcessing` split from store configuration (`@EnableJdbcJobRepository`); `JobOperator` replaces `JobLauncher`, `JobRepository` subsumes `JobExplorer`; the new `ChunkOrientedStep` implementation with `faultTolerant()` builder and Framework-7-based retry; immutable domain model with record-based `JobParameter`; package moves (infrastructure under `...infrastructure.*`); schema sequence rename and migration scripts; failed v5 instances can't be restarted in v6; builders required instead of setters; Micrometer needs an `ObservationRegistry` bean; Jackson 3; Boot 4's resourceless-by-default starter versus `-batch-jdbc`.

**Q21. What are the failure modes of a multi-threaded step?**
Non-thread-safe readers, lost restartability (position meaningless), out-of-order processing, connection pool exhaustion, DB contention, and shared mutable state in processors.

**Q22. How do you test a batch job?**
Unit-test processors and listeners with plain JUnit. Test steps and jobs with `@SpringBatchTest` utilities (in v6 prefer `JobOperatorTestUtils` over the deprecated `JobLauncherTestUtils`) against a real MySQL via Testcontainers. Add restart tests, skip tests (inject a bad record), and volume smoke tests.

**Q23. Real scenario: after a deploy the job runs but writes zero rows and reports COMPLETED. Why?**
Commonly: the Boot 4 starter changed so metadata isn't where you expect (not a data issue but confusing); the reader's query matches nothing (`WHERE` clause/params); `JdbcPagingItemReader` sort key/where filter wrong; `@StepScope` parameters bound to empty strings; processor returning null for everything (check `filterCount`); the writer's SQL matched 0 rows with `assertUpdates(false)`. The `BATCH_STEP_EXECUTION` counts (read/filter/write) pinpoint which stage.

**Q24. Real scenario: the same job, yesterday's parameters, throws JobInstanceAlreadyCompleteException. What now?**
That's the framework preventing double-processing. If you truly intend a re-run, either use a new identifying parameter (e.g. `attempt=2`), or define a `RunIdIncrementer` and start the next instance. Never delete the metadata row to "force" it unless you've verified the previous run's data effects.

---

# Module 17 — Production Best Practices

## 17.1 Logging

- Log at **job and step boundaries** with ids: `jobExecutionId`, `jobName`, key parameters. Put them in the **MDC** so every log line carries them.
- Use listeners for structure, not scattered `log.info`:

```java
@Component
public class JobLoggingListener implements JobExecutionListener {
    private static final Logger log = LoggerFactory.getLogger(JobLoggingListener.class);

    @Override public void beforeJob(JobExecution je) {
        MDC.put("jobExecutionId", String.valueOf(je.getId()));
        log.info("Job {} started with {}", je.getJobInstance().getJobName(), je.getJobParameters());
    }
    @Override public void afterJob(JobExecution je) {
        String duration = (je.getStartTime() != null && je.getEndTime() != null)
                ? Duration.between(je.getStartTime(), je.getEndTime()).toString() : "n/a";
        log.info("Job {} finished: status={} exit={} duration={}", je.getJobInstance().getJobName(),
                 je.getStatus(), je.getExitStatus().getExitCode(), duration);
        je.getStepExecutions().forEach(s -> log.info(
            "  step {} read={} filter={} write={} skipR/P/W={}/{}/{} rollback={}",
            s.getStepName(), s.getReadCount(), s.getFilterCount(), s.getWriteCount(),
            s.getReadSkipCount(), s.getProcessSkipCount(), s.getWriteSkipCount(), s.getRollbackCount()));
        MDC.remove("jobExecutionId");
    }
}
```

- **Don't log per item** at INFO (a million lines). Log per chunk at DEBUG, and **log every reject with its key** at WARN.
- Never log sensitive fields (card numbers, PII).
- *v6 note:* listener interfaces are in `org.springframework.batch.core.listener`; the `*ListenerSupport` adapter classes were removed (use the interfaces' default methods).

## 17.2 Monitoring and metrics

Spring Batch publishes Micrometer metrics (job and step durations, active jobs, item read/process/write timing, chunk timing). **In v6 you must define an `ObservationRegistry` bean** (the global static registry usage was removed). With Boot Actuator and Micrometer on the classpath, Boot normally provides the registry and meter infrastructure; verify that the batch metrics actually appear after upgrade.

- Export to Prometheus/Grafana (or your APM). Job duration, item throughput, skip counts, failures.
- **Short-lived jobs** (Kubernetes CronJob) die before Prometheus scrapes them: push via a gateway, or emit metrics/logs to a collector, or record results from the metadata tables.
- **Alert on**: job FAILED; job running longer than N× its p95 duration; no successful run by a deadline (**absence of a run is a failure** — alert on missing runs); skip count above threshold; rollback count rising; STARTED with stale `LAST_UPDATED`.
- A tiny **metadata dashboard query** (Module 11) is surprisingly effective.

## 17.3 Error recovery and restart strategies

| Situation | Action |
|---|---|
| Transient infra failure | Retry in-step; if exhausted, job fails → alert → restart |
| Bad data above skip limit | Job fails → fix/replace file → **new instance** (new identifying param) or restart if the input is unchanged |
| JVM killed / node lost | Check for zombie STARTED; use graceful SIGTERM shutdown (v6); mark/recover; restart |
| Wrong results discovered after COMPLETED | Compensating job (reverse/reload), **not** a restart |
| Never to be rerun | **Abandon** the execution |

Runbook items to write down: how to list failed executions; how to restart (`CommandLineJobOperator restart <executionId>` in v6, or your REST/admin endpoint); how to abandon; how to reconcile.

**Graceful shutdown (v6).** On SIGTERM the running step is stopped and the repository is updated into a consistent restartable state. Combine with Kubernetes `terminationGracePeriodSeconds` longer than one chunk's duration.

## 17.4 Performance monitoring
Track rows/second per step, p95 chunk duration, DB lock waits, connection pool usage, heap and GC pauses (for chunk size), and metadata table size. Keep a **baseline** per job so regressions are noticed after releases.

## 17.5 Anti-patterns (and the fix)

| Anti-pattern | Why it hurts | Fix |
|---|---|---|
| Loop over all records inside a **tasklet** | No chunking/skip/restart; long transaction | Chunk-oriented step |
| `chunk(1)` | Metadata + commit per row | 500–1000 for JDBC |
| Per-item DB/API lookup in processor | N+1 at scale | Preload map / join in reader SQL |
| `RepositoryItemWriter`/per-item `save` for millions | Select+insert per row | JDBC batch writer |
| Business logic in listeners | Hidden behavior, runs outside chunk semantics | Keep in processor/writer |
| Timestamp as identifying param on restartable job | Never restarts | Stable business keys |
| Auto-run at app startup in production (`spring.batch.job.enabled=true` on a web app) | Surprise executions on every deploy/scale-out | Disable; trigger explicitly |
| Multi-threaded step with non-thread-safe reader | Data corruption/duplicates | Thread-safe reader or partitioning |
| Never purging metadata | Slow commits | Retention job |
| Swallowing exceptions in writers | Silent data loss | Let them propagate; use skip/retry |
| Skipping broad `Exception` | "Success" with missing data | Skip specific data exceptions with a limit |
| Non-idempotent writes | Duplicates on restart/retry | Upserts/natural keys/idempotency keys |
| Same DB for metadata and heavy OLTP with no capacity planning | Contention with the live system | Schedule off-peak; replicas; capacity plan |
| Testing only the happy path | Production failures untested | Restart, skip, retry, volume tests |
| Large `ExecutionContext` | Serialized at every commit | Store ids/counters only |
| Running the web app and batch workers in one scaled deployment | Every replica could trigger or contend | Separate batch deployment / Kubernetes Job |

## 17.6 Deployment practices
- **One job per container/JVM** and a Kubernetes `CronJob` (or an orchestrator: Airflow, Argo, Control-M) as the scheduler; Spring Batch does not need to be a long-running web app.
- Prevent **overlapping runs** (the JobRepository rejects a second execution of the same instance; use a lock or `concurrencyPolicy: Forbid` for scheduled triggers for different instances of the same job).
- Secrets via your platform, never in job parameters (parameters are stored in metadata and logged).
- Schema via **migrations**; pin the Spring Batch version; read the migration guide on every major upgrade, and drain or abandon failed instances **before** upgrading (v5 → v6 restart is not supported).
- **Security:** if you expose a REST trigger, protect it with Spring Security (you already know it) and audit who launched what.

## 17.7 Production checklist

- [ ] Idempotent writes; restart tested
- [ ] Stable identifying parameters; parameter validator
- [ ] Skip limits, reject recording, retry only for transient errors
- [ ] Chunk size, page size, pool size are config properties, with measured values
- [ ] MySQL: `rewriteBatchedStatements`, streaming for cursor reads, indexes verified with EXPLAIN
- [ ] Metadata schema migrated; retention job in place
- [ ] Metrics, logs with execution id, alerts including **missing run** and **long-running**
- [ ] Graceful shutdown; zombie-execution runbook
- [ ] Automated tests: processor unit tests, job integration test with MySQL Testcontainers, restart and skip tests
- [ ] Documented operator procedures (start, stop, restart, abandon, reconcile)


---

# Part 4 — Capstone: A Complete Spring Batch Application

> **Honesty note.** This application targets **Spring Boot 4.0.x / Spring Batch 6.0.x / Java 21 / MySQL 8**. It was written against the 6.0 migration guide and the 6.0.5 Javadoc, but the workspace I used could not reach Maven Central, so **I could not compile or run it**. Section 10 lists the exact spots most likely to need a one-line fix. Compile it on day one, and fix imports with your IDE.

## 1. What we build

**Scenario.** A payments partner drops a daily CSV of transactions. The nightly job must:

1. **Import**: read the CSV, validate and enrich each row (fee = 1.5%), upsert into MySQL, record rejects.
2. **Summarize**: aggregate that day's transactions per account into a report CSV.
3. **Archive**: move the processed input file to an archive folder.

This one app exercises: a job with three steps, a **chunk step** with **skip + retry**, a **tasklet** step, `@StepScope` with job parameters, JDBC batch writing, a custom reject audit, listeners, parameter validation, a REST trigger, and **restart**.

```
                POST /batch/txn-import?inputFile=tx-2026-10-01.csv&runDate=2026-10-01
                                   │
                          ┌────────▼─────────┐
                          │  BatchController │  (async hand-off, returns 202)
                          └────────┬─────────┘
                                   │ jobOperator.start(job, params)
                          ┌────────▼────────────────────────────────────────────┐
                          │ Job: txnImportJob                                    │
                          │                                                      │
                          │  Step 1 importStep (chunk=500, fault tolerant)       │
                          │    FlatFileItemReader ─► TxProcessor ─► JdbcBatchItemWriter
                          │        (CSV)             validate/filter/fee   (MySQL upsert)
                          │                   skip ► RejectSkipListener ► tx_rejects
                          │                                                      │
                          │  Step 2 summaryStep (chunk=100)                      │
                          │    JdbcCursorItemReader ─► FlatFileItemWriter        │
                          │    (GROUP BY account)       (summary-<date>.csv)     │
                          │                                                      │
                          │  Step 3 archiveStep (tasklet)  move CSV to archive/  │
                          └────────────────────────┬────────────────────────────┘
                                                   ▼
                                     JobRepository ─► BATCH_* tables (MySQL)
```

## 2. Project layout

```
txn-batch/
├─ pom.xml
├─ src/main/resources/
│   ├─ application.yml
│   └─ schema.sql                      (business tables)
├─ src/main/java/com/example/txnbatch/
│   ├─ TxnBatchApplication.java
│   ├─ config/BatchProps.java
│   ├─ config/TxnJobConfig.java        (reader, writer, steps, job)
│   ├─ domain/TxCsv.java  Tx.java  AccountSummary.java  InvalidTxException.java
│   ├─ processor/TxProcessor.java
│   ├─ listener/JobLoggingListener.java  RejectSkipListener.java  StepSummaryListener.java
│   ├─ reject/RejectAudit.java
│   ├─ tasklet/ArchiveTasklet.java
│   └─ web/BatchController.java
└─ src/test/java/com/example/txnbatch/processor/TxProcessorTest.java
```

## 3. The code

### 3.1 `pom.xml`

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
  <modelVersion>4.0.0</modelVersion>

  <parent>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-parent</artifactId>
    <!-- use the latest 4.0.x from https://start.spring.io -->
    <version>4.0.0</version>
    <relativePath/>
  </parent>

  <groupId>com.example</groupId>
  <artifactId>txn-batch</artifactId>
  <version>0.0.1-SNAPSHOT</version>

  <properties>
    <java.version>21</java.version>
  </properties>

  <dependencies>
    <!-- "-jdbc" = metadata stored in the database (needed for restart across JVMs).
         Plain spring-boot-starter-batch = in-memory metadata in Boot 4. -->
    <dependency>
      <groupId>org.springframework.boot</groupId>
      <artifactId>spring-boot-starter-batch-jdbc</artifactId>
    </dependency>
    <dependency>
      <groupId>org.springframework.boot</groupId>
      <artifactId>spring-boot-starter-webmvc</artifactId>
    </dependency>
    <dependency>
      <groupId>org.springframework.boot</groupId>
      <artifactId>spring-boot-starter-actuator</artifactId>
    </dependency>
    <dependency>
      <groupId>com.mysql</groupId>
      <artifactId>mysql-connector-j</artifactId>
      <scope>runtime</scope>
    </dependency>

    <dependency>
      <groupId>org.springframework.boot</groupId>
      <artifactId>spring-boot-starter-batch-jdbc-test</artifactId>
      <scope>test</scope>
    </dependency>
  </dependencies>

  <build>
    <plugins>
      <plugin>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-maven-plugin</artifactId>
      </plugin>
    </plugins>
  </build>
</project>
```

(Spring Data JPA is deliberately absent: this job uses JDBC for speed. If you add JPA, Boot configures a `JpaTransactionManager`; make sure the step and writers use the one you intend.)

### 3.2 `application.yml`

```yaml
spring:
  application:
    name: txn-batch
  datasource:
    url: jdbc:mysql://localhost:3306/batchdb?rewriteBatchedStatements=true&useCursorFetch=true
    username: batch
    password: ${DB_PASSWORD}
  batch:
    job:
      enabled: false                    # never auto-run at startup in production
    jdbc:
      initialize-schema: always         # DEV ONLY. Use Flyway/Liquibase migrations in production
  sql:
    init:
      mode: always                      # runs schema.sql (business tables), dev only

batch:
  txn:
    input-dir: /data/in
    archive-dir: /data/archive
    report-dir: /data/reports
    chunk-size: 500
    skip-limit: 100
  demo:
    fail-on-tx-id: ""                   # set to e.g. T007 to simulate a crash (restart demo)

management:
  endpoints:
    web:
      exposure:
        include: health,metrics
```

If Boot 4 reports `spring.batch.jdbc.initialize-schema` as unknown, add `spring-boot-properties-migrator` temporarily (runtime scope) to see the renamed key.

### 3.3 `schema.sql` (business tables)

```sql
CREATE TABLE IF NOT EXISTS transactions (
  tx_id       VARCHAR(40)   NOT NULL PRIMARY KEY,
  account_id  VARCHAR(40)   NOT NULL,
  amount      DECIMAL(18,2) NOT NULL,
  currency    CHAR(3)       NOT NULL,
  tx_date     DATE          NOT NULL,
  fee         DECIMAL(18,2) NOT NULL,
  loaded_at   TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  INDEX idx_tx_date_account (tx_date, account_id)
);

CREATE TABLE IF NOT EXISTS tx_rejects (
  id               BIGINT AUTO_INCREMENT PRIMARY KEY,
  job_execution_id BIGINT       NULL,
  phase            VARCHAR(10)  NOT NULL,
  tx_id            VARCHAR(40)  NULL,
  reason           VARCHAR(500) NULL,
  rejected_at      TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uq_reject (job_execution_id, phase, tx_id)
);
```

The unique key plus `INSERT IGNORE` (below) makes reject recording **idempotent**: skip callbacks can run again when a chunk is rolled back and retried.

### 3.4 `TxnBatchApplication`

```java
package com.example.txnbatch;

import com.example.txnbatch.config.BatchProps;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties(BatchProps.class)
public class TxnBatchApplication {
    public static void main(String[] args) {
        SpringApplication.run(TxnBatchApplication.class, args);
    }
}
```

There is no `@EnableBatchProcessing`: Spring Boot's auto-configuration creates the `JobRepository`, `JobOperator`, and (with the `-jdbc` starter) the JDBC metadata store. Add `@EnableBatchProcessing` only if you want to customize infrastructure (in v6, with `@EnableJdbcJobRepository` for store settings).

### 3.5 `BatchProps`

```java
package com.example.txnbatch.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import java.nio.file.Path;

@ConfigurationProperties(prefix = "batch.txn")
public record BatchProps(Path inputDir, Path archiveDir, Path reportDir, int chunkSize, int skipLimit) { }
```

### 3.6 Domain

```java
package com.example.txnbatch.domain;

import java.math.BigDecimal;
import java.time.LocalDate;

/** One parsed CSV row (input type of the processor). */
public record TxCsv(String txId, String accountId, BigDecimal amount, String currency, LocalDate txDate) { }
```

```java
package com.example.txnbatch.domain;

import java.math.BigDecimal;
import java.time.LocalDate;

/** Validated, enriched transaction (output of the processor, input of the writer). */
public record Tx(String txId, String accountId, BigDecimal amount, String currency, LocalDate txDate, BigDecimal fee) { }
```

```java
package com.example.txnbatch.domain;

import java.math.BigDecimal;

public record AccountSummary(String accountId, long txCount, BigDecimal totalAmount, BigDecimal totalFee) { }
```

```java
package com.example.txnbatch.domain;

/** A DATA problem: skippable. Never use this for infrastructure errors. */
public class InvalidTxException extends RuntimeException {
    private final String txId;
    public InvalidTxException(String message, String txId) { super(message); this.txId = txId; }
    public String getTxId() { return txId; }
}
```

### 3.7 `TxProcessor`

```java
package com.example.txnbatch.processor;

import com.example.txnbatch.domain.*;
import org.springframework.batch.infrastructure.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Set;

@Component
public class TxProcessor implements ItemProcessor<TxCsv, Tx> {

    private static final Set<String> CURRENCIES = Set.of("USD", "EUR", "BDT");
    private static final BigDecimal FEE_RATE = new BigDecimal("0.015");

    private final String failOnTxId;   // demo hook to simulate a crash; empty in real life

    public TxProcessor(@Value("${batch.demo.fail-on-tx-id:}") String failOnTxId) {
        this.failOnTxId = failOnTxId;
    }

    @Override
    public Tx process(TxCsv in) {
        if (!failOnTxId.isEmpty() && failOnTxId.equals(in.txId())) {
            // NOT an InvalidTxException => not skippable => the step FAILS (used for the restart demo)
            throw new IllegalStateException("Simulated crash at " + in.txId());
        }

        // VALIDATE: wrong data -> skippable exception
        if (in.amount() == null) throw new InvalidTxException("amount missing", in.txId());
        if (!CURRENCIES.contains(in.currency()))
            throw new InvalidTxException("unsupported currency " + in.currency(), in.txId());

        // FILTER: a normal business rule, not an error -> null
        if (in.amount().signum() == 0) return null;

        // TRANSFORM + ENRICH
        BigDecimal fee = in.amount().abs().multiply(FEE_RATE).setScale(2, RoundingMode.HALF_UP);
        return new Tx(in.txId(), in.accountId(), in.amount(), in.currency(), in.txDate(), fee);
    }
}
```

### 3.8 Reject audit

```java
package com.example.txnbatch.reject;

import org.slf4j.MDC;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Repository
public class RejectAudit {

    private final JdbcTemplate jdbc;
    public RejectAudit(JdbcTemplate jdbc) { this.jdbc = jdbc; }

    /** REQUIRES_NEW: the reject row survives even when the chunk transaction rolls back. */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void record(String phase, String txId, String reason) {
        String exec = MDC.get("jobExecutionId");   // set by JobLoggingListener (single-threaded demo)
        jdbc.update("""
                INSERT IGNORE INTO tx_rejects (job_execution_id, phase, tx_id, reason)
                VALUES (?, ?, ?, ?)
                """,
                exec == null ? null : Long.valueOf(exec), phase, txId,
                reason == null ? null : reason.substring(0, Math.min(reason.length(), 500)));
    }
}
```

### 3.9 Listeners

```java
package com.example.txnbatch.listener;

import com.example.txnbatch.domain.*;
import com.example.txnbatch.reject.RejectAudit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.listener.SkipListener;
import org.springframework.stereotype.Component;

@Component
public class RejectSkipListener implements SkipListener<TxCsv, Tx> {
    private static final Logger log = LoggerFactory.getLogger(RejectSkipListener.class);
    private final RejectAudit audit;
    public RejectSkipListener(RejectAudit audit) { this.audit = audit; }

    @Override public void onSkipInRead(Throwable t) {
        log.warn("Skipped unreadable line: {}", t.getMessage());
        audit.record("READ", null, t.getMessage());
    }
    @Override public void onSkipInProcess(TxCsv item, Throwable t) {
        log.warn("Rejected tx {}: {}", item.txId(), t.getMessage());
        audit.record("PROCESS", item.txId(), t.getMessage());
    }
    @Override public void onSkipInWrite(Tx item, Throwable t) {
        log.error("Write-skip tx {}: {}", item.txId(), t.getMessage());
        audit.record("WRITE", item.txId(), t.getMessage());
    }
}
```

```java
package com.example.txnbatch.listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.listener.JobExecutionListener;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Component
public class JobLoggingListener implements JobExecutionListener {
    private static final Logger log = LoggerFactory.getLogger(JobLoggingListener.class);

    @Override
    public void beforeJob(JobExecution je) {
        MDC.put("jobExecutionId", String.valueOf(je.getId()));
        log.info("Job {} started, executionId={}, params={}",
                je.getJobInstance().getJobName(), je.getId(), je.getJobParameters());
    }

    @Override
    public void afterJob(JobExecution je) {
        String duration = (je.getStartTime() != null && je.getEndTime() != null)
                ? Duration.between(je.getStartTime(), je.getEndTime()).toString() : "n/a";
        log.info("Job {} finished: status={} exit={} duration={}",
                je.getJobInstance().getJobName(), je.getStatus(), je.getExitStatus().getExitCode(), duration);
        je.getStepExecutions().forEach(s -> log.info(
                "  step={} status={} read={} filter={} write={} skip(r/p/w)={}/{}/{} commits={} rollbacks={}",
                s.getStepName(), s.getStatus(), s.getReadCount(), s.getFilterCount(), s.getWriteCount(),
                s.getReadSkipCount(), s.getProcessSkipCount(), s.getWriteSkipCount(),
                s.getCommitCount(), s.getRollbackCount()));
        MDC.remove("jobExecutionId");
    }
}
```

```java
package com.example.txnbatch.listener;

import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.listener.StepExecutionListener;
import org.springframework.batch.core.step.StepExecution;
import org.springframework.stereotype.Component;

/** Makes skips visible to operators: exit code "COMPLETED WITH SKIPS" (BatchStatus stays COMPLETED). */
@Component
public class StepSummaryListener implements StepExecutionListener {
    @Override
    public ExitStatus afterStep(StepExecution se) {
        long skips = se.getSkipCount();
        if (se.getStatus() == BatchStatus.COMPLETED && skips > 0) {
            return new ExitStatus("COMPLETED WITH SKIPS", skips + " item(s) skipped");
        }
        return null;   // keep the default exit status
    }
}
```

### 3.10 `ArchiveTasklet`

```java
package com.example.txnbatch.tasklet;

import com.example.txnbatch.config.BatchProps;
import org.springframework.batch.core.step.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.infrastructure.repeat.RepeatStatus;

import java.nio.file.*;

public class ArchiveTasklet implements Tasklet {
    private final Path source;
    private final Path targetDir;

    public ArchiveTasklet(BatchProps props, String inputFile) {
        this.source = props.inputDir().resolve(inputFile);
        this.targetDir = props.archiveDir();
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        Files.createDirectories(targetDir);
        Path target = targetDir.resolve(source.getFileName());
        if (Files.exists(source)) {                       // idempotent: safe if the step is re-run
            Files.move(source, target, StandardCopyOption.REPLACE_EXISTING);
        }
        return RepeatStatus.FINISHED;
    }
}
```

### 3.11 `TxnJobConfig` — the heart of the application

```java
package com.example.txnbatch.config;

import com.example.txnbatch.domain.*;
import com.example.txnbatch.listener.*;
import com.example.txnbatch.processor.TxProcessor;
import com.example.txnbatch.tasklet.ArchiveTasklet;

import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.parameters.DefaultJobParametersValidator;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.step.builder.ChunkOrientedStepBuilder;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.infrastructure.item.database.JdbcBatchItemWriter;
import org.springframework.batch.infrastructure.item.database.JdbcCursorItemReader;
import org.springframework.batch.infrastructure.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.infrastructure.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.batch.infrastructure.item.file.FlatFileItemReader;
import org.springframework.batch.infrastructure.item.file.FlatFileItemWriter;
import org.springframework.batch.infrastructure.item.file.FlatFileParseException;
import org.springframework.batch.infrastructure.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.infrastructure.item.file.builder.FlatFileItemWriterBuilder;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.time.LocalDate;

@Configuration
public class TxnJobConfig {

    // ───────────── STEP 1: import (chunk, fault tolerant) ─────────────

    @Bean
    @StepScope
    FlatFileItemReader<TxCsv> txReader(BatchProps props,
                                       @Value("#{jobParameters['inputFile']}") String inputFile) {
        return new FlatFileItemReaderBuilder<TxCsv>()
                .name("txReader")                                   // keys in the ExecutionContext
                .resource(new FileSystemResource(props.inputDir().resolve(inputFile)))
                .encoding("UTF-8")
                .linesToSkip(1)
                .delimited().delimiter(",")
                .names("txId", "accountId", "amount", "currency", "txDate")
                .fieldSetMapper(fs -> new TxCsv(
                        fs.readString("txId"),
                        fs.readString("accountId"),
                        fs.readBigDecimal("amount"),               // "abc" -> NumberFormatException
                        fs.readString("currency"),
                        LocalDate.parse(fs.readString("txDate")))) // bad date -> DateTimeParseException
                .build();                                           // both surface as FlatFileParseException
    }

    @Bean
    JdbcBatchItemWriter<Tx> txWriter(DataSource ds) {
        return new JdbcBatchItemWriterBuilder<Tx>()
                .dataSource(ds)
                .sql("""
                     INSERT INTO transactions (tx_id, account_id, amount, currency, tx_date, fee)
                     VALUES (:txId, :accountId, :amount, :currency, :txDate, :fee)
                     ON DUPLICATE KEY UPDATE
                        account_id = VALUES(account_id), amount = VALUES(amount),
                        currency = VALUES(currency), tx_date = VALUES(tx_date), fee = VALUES(fee)
                     """)
                .itemSqlParameterSourceProvider(tx -> new MapSqlParameterSource()
                        .addValue("txId", tx.txId())
                        .addValue("accountId", tx.accountId())
                        .addValue("amount", tx.amount())
                        .addValue("currency", tx.currency())
                        .addValue("txDate", tx.txDate())
                        .addValue("fee", tx.fee()))
                .build();
    }

    @Bean
    Step importStep(JobRepository jobRepository, PlatformTransactionManager tx, BatchProps props,
                    FlatFileItemReader<TxCsv> txReader, TxProcessor txProcessor, JdbcBatchItemWriter<Tx> txWriter,
                    RejectSkipListener skipListener, StepSummaryListener summaryListener) {

        return new ChunkOrientedStepBuilder<TxCsv, Tx>("importStep", jobRepository, props.chunkSize())
                .reader(txReader)
                .processor(txProcessor)
                .writer(txWriter)
                .transactionManager(tx)
                .faultTolerant()
                .skip(FlatFileParseException.class)        // unreadable lines
                .skip(InvalidTxException.class)            // validation failures
                .skipLimit(props.skipLimit())              // safety valve: too many rejects => bad file => FAIL
                .retry(TransientDataAccessException.class) // deadlocks, lock timeouts, transient connection errors
                .retryLimit(3)
                .skipListener(skipListener)
                .listener(summaryListener)
                .build();
    }

    // ───────────── STEP 2: per-account summary (cursor -> file) ─────────────

    @Bean
    @StepScope
    JdbcCursorItemReader<AccountSummary> summaryReader(DataSource ds,
                                                       @Value("#{jobParameters['runDate']}") String runDate) {
        return new JdbcCursorItemReaderBuilder<AccountSummary>()
                .name("summaryReader")
                .dataSource(ds)
                .sql("""
                     SELECT account_id, COUNT(*) AS tx_count, SUM(amount) AS total_amount, SUM(fee) AS total_fee
                     FROM transactions WHERE tx_date = ? GROUP BY account_id ORDER BY account_id
                     """)
                .queryArguments(LocalDate.parse(runDate))
                .rowMapper((rs, i) -> new AccountSummary(rs.getString("account_id"), rs.getLong("tx_count"),
                        rs.getBigDecimal("total_amount"), rs.getBigDecimal("total_fee")))
                .build();
    }

    @Bean
    @StepScope
    FlatFileItemWriter<AccountSummary> summaryWriter(BatchProps props,
                                                     @Value("#{jobParameters['runDate']}") String runDate) {
        return new FlatFileItemWriterBuilder<AccountSummary>()
                .name("summaryWriter")
                .resource(new FileSystemResource(props.reportDir().resolve("summary-" + runDate + ".csv")))
                .headerCallback(w -> w.write("accountId,txCount,totalAmount,totalFee"))
                .delimited().delimiter(",")
                .fieldExtractor(s -> new Object[] { s.accountId(), s.txCount(), s.totalAmount(), s.totalFee() })
                .build();
    }

    @Bean
    Step summaryStep(JobRepository jobRepository, PlatformTransactionManager tx,
                     JdbcCursorItemReader<AccountSummary> summaryReader,
                     FlatFileItemWriter<AccountSummary> summaryWriter) {
        return new ChunkOrientedStepBuilder<AccountSummary, AccountSummary>("summaryStep", jobRepository, 100)
                .reader(summaryReader)
                .writer(summaryWriter)                     // no processor: pass-through
                .transactionManager(tx)
                .build();
    }

    // ───────────── STEP 3: archive (tasklet) ─────────────

    @Bean
    @StepScope
    Tasklet archiveTasklet(BatchProps props, @Value("#{jobParameters['inputFile']}") String inputFile) {
        return new ArchiveTasklet(props, inputFile);
    }

    @Bean
    Step archiveStep(JobRepository jobRepository, PlatformTransactionManager tx, Tasklet archiveTasklet) {
        return new StepBuilder("archiveStep", jobRepository)
                .tasklet(archiveTasklet, tx)
                .build();
    }

    // ───────────── THE JOB ─────────────

    @Bean
    Job txnImportJob(JobRepository jobRepository, Step importStep, Step summaryStep, Step archiveStep,
                     JobLoggingListener jobLogging) {
        return new JobBuilder("txnImportJob", jobRepository)
                .validator(new DefaultJobParametersValidator(
                        new String[] { "inputFile", "runDate" },   // required
                        new String[] { }))                          // optional
                .listener(jobLogging)
                .start(importStep)
                .next(summaryStep)
                .next(archiveStep)
                .build();
    }
}
```

Points to notice:
- Every reader/writer that needs a job parameter is **`@StepScope`** and returns its **concrete type** (so `ItemStream` stays visible).
- The reader has a **unique name**.
- Only **file names**, not paths, come in as parameters; the base directories come from configuration (path-traversal protection, see the controller).
- `skip` is for **data** exceptions; `retry` is for **transient infrastructure** exceptions; both have limits.
- The writer is **idempotent** (`ON DUPLICATE KEY UPDATE`), so restart and retry cannot duplicate rows.

### 3.12 `BatchController` (REST trigger and restart)

```java
package com.example.txnbatch.web;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.nio.file.Path;
import java.util.Map;

@RestController
@RequestMapping("/batch")
public class BatchController {
    private static final Logger log = LoggerFactory.getLogger(BatchController.class);

    private final JobOperator jobOperator;
    private final JobRepository jobRepository;
    private final Job txnImportJob;
    private final AsyncTaskExecutor executor = new SimpleAsyncTaskExecutor("batch-launch-");

    public BatchController(JobOperator jobOperator, JobRepository jobRepository, Job txnImportJob) {
        this.jobOperator = jobOperator;
        this.jobRepository = jobRepository;
        this.txnImportJob = txnImportJob;
    }

    @PostMapping("/txn-import")
    public ResponseEntity<Map<String, String>> start(@RequestParam String inputFile, @RequestParam String runDate) {
        // Path-traversal guard: only a bare file name is accepted
        if (!Path.of(inputFile).getFileName().toString().equals(inputFile)) {
            return ResponseEntity.badRequest().body(Map.of("error", "inputFile must be a file name"));
        }
        JobParameters params = new JobParametersBuilder()
                .addString("inputFile", inputFile)
                .addString("runDate", runDate)
                .toJobParameters();
        executor.execute(() -> {
            try {
                JobExecution je = jobOperator.start(txnImportJob, params);
                log.info("Launched execution {} -> {}", je.getId(), je.getStatus());
            } catch (Exception e) {      // JobInstanceAlreadyComplete, AlreadyRunning, InvalidJobParameters ...
                log.error("Could not start job: {}", e.toString());
            }
        });
        return ResponseEntity.accepted().body(Map.of("message", "launch requested"));
    }

    @PostMapping("/restart/{executionId}")
    public ResponseEntity<Map<String, String>> restart(@PathVariable long executionId) {
        JobExecution failed = jobRepository.getJobExecution(executionId);
        if (failed == null) return ResponseEntity.notFound().build();
        executor.execute(() -> {
            try {
                jobOperator.restart(failed);
            } catch (Exception e) {
                log.error("Could not restart execution {}: {}", executionId, e.toString());
            }
        });
        return ResponseEntity.accepted().body(Map.of("message", "restart requested"));
    }
}
```

This controller launches on another thread so the HTTP request returns immediately. In a real deployment, protect these endpoints with Spring Security (role-based, audited), and prefer a Kubernetes `Job`/`CronJob` that runs the app in CLI mode instead of exposing a launch endpoint on a public service.

### 3.13 Unit test for the processor

```java
package com.example.txnbatch.processor;

import com.example.txnbatch.domain.*;
import org.junit.jupiter.api.Test;
import java.math.BigDecimal;
import java.time.LocalDate;

import static org.junit.jupiter.api.Assertions.*;

class TxProcessorTest {
    private final TxProcessor processor = new TxProcessor("");
    private static final LocalDate D = LocalDate.of(2026, 10, 1);

    @Test void computesFeeWithHalfUpRounding() {
        Tx tx = processor.process(new TxCsv("T1", "A1", new BigDecimal("120.50"), "USD", D));
        assertEquals(new BigDecimal("1.81"), tx.fee());          // 1.8075 -> 1.81
    }

    @Test void filtersZeroAmount() {
        assertNull(processor.process(new TxCsv("T2", "A1", BigDecimal.ZERO, "USD", D)));
    }

    @Test void rejectsUnsupportedCurrency() {
        assertThrows(InvalidTxException.class,
                () -> processor.process(new TxCsv("T3", "A1", BigDecimal.TEN, "XXX", D)));
    }
}
```

An integration test for the whole job should run against **MySQL via Testcontainers** with the batch test utilities (`@SpringBatchTest`; in v6 use `JobOperatorTestUtils` rather than the deprecated `JobLauncherTestUtils`). Assert: step counts, rows in `transactions`, rows in `tx_rejects`, and a **restart** scenario (below). I'm not including that test's code because the Boot 4 / Testcontainers 2 coordinates and annotation packages are the kind of detail I could not verify offline.

## 4. Run it

```sql
-- once
CREATE DATABASE batchdb CHARACTER SET utf8mb4;
CREATE USER 'batch'@'%' IDENTIFIED BY '...'; GRANT ALL ON batchdb.* TO 'batch'@'%';
```

`/data/in/tx-2026-10-01.csv`:

```csv
txId,accountId,amount,currency,txDate
T001,A100,120.50,USD,2026-10-01
T002,A100,75.00,USD,2026-10-01
T003,A200,300.00,EUR,2026-10-01
T004,A200,0.00,EUR,2026-10-01
T005,A300,50.00,XXX,2026-10-01
T006,A300,abc,USD,2026-10-01
T007,A100,200.00,USD,2026-10-01
T008,A400,10.00,BDT,2026-10-01
```

Deliberate traps in the data: **T004** (zero amount → filtered), **T005** (bad currency → process skip), **T006** (unparseable amount → read skip).

```bash
export DB_PASSWORD=...
./mvnw spring-boot:run
curl -X POST "localhost:8080/batch/txn-import?inputFile=tx-2026-10-01.csv&runDate=2026-10-01"
```

(Or, for a quick CLI run, set `spring.batch.job.enabled=true` and pass `inputFile=tx-2026-10-01.csv runDate=2026-10-01` as arguments.)

**Expected results** with the sample file:

| Item | Expected |
|---|---|
| `importStep` read count | 7 (T006 failed to read, so it is a read skip) |
| filter count | 1 (T004) |
| process skip count | 1 (T005) |
| read skip count | 1 (T006) |
| write count | 5 (T001, T002, T003, T007, T008) |
| `tx_rejects` rows | 2 (`READ` with null tx id, `PROCESS` for T005) |
| `summary-2026-10-01.csv` | A100: 3 tx, 395.50, fee 5.94; A200: 1, 300.00, 4.50; A400: 1, 10.00, 0.15 |
| Exit status of `importStep` | `COMPLETED WITH SKIPS` (BatchStatus `COMPLETED`) |
| Input file | moved to `/data/archive/` |
| Fees | 120.50×1.5% = 1.8075 → 1.81; 75.00 → 1.125 → 1.13; 200 → 3.00 |

`rollbackCount` and `commitCount` depend on the implementation details of the chunk loop (the classic implementation rolls back and re-processes the chunk containing the skipped item). Read them from your log line rather than assuming.

## 5. Every class explained

| Class | Role | Spring Batch concept it demonstrates |
|---|---|---|
| `TxnBatchApplication` | Boot entry point; enables `BatchProps` | Boot auto-configures `JobRepository`, `JobOperator`, schema init |
| `BatchProps` | Type-safe config (directories, chunk size, skip limit) | Tunable settings as properties, not literals |
| `TxCsv` / `Tx` / `AccountSummary` | Immutable data carriers per stage | Reader output type ≠ writer input type |
| `InvalidTxException` | Marks *data* errors | Skippable exception classification |
| `TxProcessor` | Validate → filter → enrich | `ItemProcessor`; `null` = filter; exception = skip candidate |
| `RejectAudit` | Writes rejects with `REQUIRES_NEW` | Side data that survives chunk rollback; idempotent insert |
| `RejectSkipListener` | Receives skip callbacks for read/process/write | `SkipListener` |
| `JobLoggingListener` | Boundary logging + MDC execution id | `JobExecutionListener`, observability |
| `StepSummaryListener` | Customizes `ExitStatus` when items were skipped | `BatchStatus` vs `ExitStatus` |
| `ArchiveTasklet` | Moves the processed file | Tasklet step; idempotent single action |
| `TxnJobConfig` | Builds readers, writers, steps, job | `@StepScope`, builders, `ChunkOrientedStepBuilder`, fault tolerance, validator |
| `BatchController` | Async launch + restart over REST | `JobOperator.start/restart`, `JobRepository.getJobExecution` |
| `TxProcessorTest` | Pure unit test | Processors need no Spring context |

Framework-provided pieces we rely on: `FlatFileItemReader` (+ `DefaultLineMapper`, `DelimitedLineTokenizer`), `JdbcBatchItemWriter`, `JdbcCursorItemReader`, `FlatFileItemWriter`, `ChunkOrientedStep`, `StepScope`, `JobRepository`, `JobOperator`.

## 6. Execution flow, step by step

### Phase A: application startup (no job runs)

1. Boot starts, creates the `DataSource`, runs `schema.sql`, and (with `initialize-schema=always`) creates the `BATCH_*` tables.
2. Boot's batch auto-configuration creates the **`JobRepository`** (JDBC), a **`JobOperator`**, and your `Job` bean is registered.
3. `@StepScope` beans (`txReader`, `summaryReader`, `summaryWriter`, `archiveTasklet`) are **not created yet**: Spring registers **scoped proxies** in their place, because `#{jobParameters[...]}` can't be resolved without a running step.
4. `spring.batch.job.enabled=false`, so nothing runs. The app waits for a request.

### Phase B: launch

5. `POST /batch/txn-import` → controller validates the file name, builds `JobParameters{inputFile, runDate}`, hands off to an executor thread, returns **202**.
6. `jobOperator.start(txnImportJob, params)`:
   - runs the **parameters validator** (both required keys present, else `InvalidJobParametersException` and **no** execution is created),
   - asks the repository: is there a `JobInstance` for `txnImportJob` with this identifying-params hash (`JOB_KEY`)?
     - none → inserts `BATCH_JOB_INSTANCE`, new execution,
     - one, last execution FAILED/STOPPED → new execution (restart),
     - one, COMPLETED → `JobInstanceAlreadyCompleteException`,
     - one currently running → `JobExecutionAlreadyRunningException`.
   - inserts `BATCH_JOB_EXECUTION` (STARTING), `BATCH_JOB_EXECUTION_PARAMS` rows, and an empty `BATCH_JOB_EXECUTION_CONTEXT`.
7. The job (`SimpleJob`, via the abstract job's template method) sets status **STARTED**, calls `JobLoggingListener.beforeJob` (MDC gets the execution id), and starts executing its steps **in order**, stopping if any step's `BatchStatus` is not COMPLETED.

### Phase C: `importStep` (the chunk loop)

8. A `StepExecution` is created and saved (`BATCH_STEP_EXECUTION`, STARTED) with an empty step context row.
9. **Scope activation.** The step binds its `StepExecution` to the thread (`StepSynchronizationManager`). The first call on the `txReader` proxy makes `StepScope` build the **real** `FlatFileItemReader` with `inputFile` resolved from the job parameters; it is cached for this step execution.
10. **Streams open.** The step has the reader and writer registered as `ItemStream`s. `reader.open(stepContext)`: new run → starts at line 0 (restart → reads `txReader.read.count` from the saved context and fast-forwards). The header line is skipped via `linesToSkip(1)`.
11. **Chunk loop** (chunk size 500; our file has 7 data rows, so a single chunk):
    - **Begin transaction** (`PlatformTransactionManager`).
    - **Read**: one `read()` call per item. For each line: `DelimitedLineTokenizer` → `FieldSet` → our lambda → `TxCsv`. At `T006`, `readBigDecimal("abc")` throws → wrapped in `FlatFileParseException` → matches `.skip(FlatFileParseException.class)`, under the limit → **skipped**: `readSkipCount++`, `RejectSkipListener.onSkipInRead` → `RejectAudit` inserts a row in its **own** transaction. The reader continues. After `T008`, `read()` returns **`null`** → end of input flagged.
    - **Process**: `TxProcessor` per item. `T004` → `null` → filtered (`filterCount++`). `T005` → `InvalidTxException` → skippable → the chunk is rolled back and re-processed without `T005` (classic behavior), `processSkipCount++`, `onSkipInProcess` → reject row.
    - **Write**: the remaining 5 `Tx` objects arrive as **one `Chunk`**. `JdbcBatchItemWriter` adds 5 parameter sets to one JDBC batch and executes it (with `rewriteBatchedStatements=true` Connector/J rewrites it into a multi-row insert).
    - **Update metadata**: counts go into `StepExecution`; each `ItemStream.update(ctx)` writes its state (reader: `txReader.read.count`) into the step `ExecutionContext`.
    - **Commit**: business rows and the `BATCH_STEP_EXECUTION` / `BATCH_STEP_EXECUTION_CONTEXT` updates commit **atomically**.
    - End-of-input flag was set → the loop exits.
12. **Step end.** `afterStep` listener sets exit status `COMPLETED WITH SKIPS`. Streams `close()`. Scoped beans are destroyed. The step execution is saved with final counts, and the **job execution context** is updated.

### Phase D: `summaryStep` and `archiveStep`

13. `summaryStep` creates a new `StepExecution`. `summaryReader` is built now (its `runDate` bound); `JdbcCursorItemReader` opens a cursor on the **already committed** step-1 data (that is why step 1 must commit before step 2: separate steps are separate transactions). Each row → `FlatFileItemWriter` buffers lines; **at commit the file is flushed** and the byte offset is saved in the step context.
14. `archiveStep` (tasklet): `execute` runs once in its own transaction and returns `FINISHED`; the file moves. Idempotent if re-run.

### Phase E: job end

15. Job status becomes **COMPLETED**, `END_TIME` set, `afterJob` logs summary lines. The `JobInstance` is now complete: launching the same parameters again fails with `JobInstanceAlreadyCompleteException`. That is the protection against re-importing yesterday's file.

## 7. How data moves through the framework

```
 CSV line  "T001,A100,120.50,USD,2026-10-01"
    │  FlatFileItemReader.read()
    ▼
 String line ─► DelimitedLineTokenizer ─► FieldSet{txId,accountId,amount,currency,txDate}
    │  our fieldSetMapper lambda
    ▼
 TxCsv (record)  ───────────────►  collected into the chunk's input list (in memory, ≤ chunkSize)
    │  TxProcessor.process()      (null => dropped, exception => skip/fail)
    ▼
 Tx (record, with fee)  ────────►  collected into the chunk's output list
    │  JdbcBatchItemWriter.write(Chunk<Tx>)
    ▼
 MapSqlParameterSource per item ─► one JDBC batch ─► INSERT ... ON DUPLICATE KEY UPDATE
    │  (all inside the chunk transaction)
    ▼
 MySQL row in `transactions`      +    UPDATE BATCH_STEP_EXECUTION (counts)
                                  +    UPDATE BATCH_STEP_EXECUTION_CONTEXT ({"txReader.read.count":8,...})
                                              └── committed together
```

Two flows travel side by side: the **business data** (record by record, chunk by chunk) and the **control data** (counts, positions, status) into the metadata tables. Only the **chunk** is ever held in memory (plus whatever your reader buffers), which is why the job's memory use does not depend on file size.

## 8. What happens internally: metadata after a successful run

`BATCH_JOB_INSTANCE`

| JOB_INSTANCE_ID | JOB_NAME | JOB_KEY |
|---|---|---|
| 1 | txnImportJob | (hash of inputFile + runDate) |

`BATCH_JOB_EXECUTION_PARAMS` (for execution 1): `inputFile` (String, identifying Y), `runDate` (String, identifying Y).

`BATCH_JOB_EXECUTION`: id 1, instance 1, `STATUS=COMPLETED`, `EXIT_CODE=COMPLETED`.

`BATCH_STEP_EXECUTION` (illustrative):

| STEP | STATUS | EXIT_CODE | READ | FILTER | WRITE | R/P/W skip |
|---|---|---|---|---|---|---|
| importStep | COMPLETED | COMPLETED WITH SKIPS | 7 | 1 | 5 | 1/1/0 |
| summaryStep | COMPLETED | COMPLETED | 3 | 0 | 3 | 0/0/0 |
| archiveStep | COMPLETED | COMPLETED | 0 | 0 | 0 | 0/0/0 |

`BATCH_STEP_EXECUTION_CONTEXT` for importStep holds the reader's saved count, e.g. `{"txReader.read.count": 8, ...}`.

Sanity check using the formula from Module 4: `read (7) − filter (1) − processSkip (1) = write (5)` ✔.

## 9. Restart demo: prove the mechanism

1. Clear data (`TRUNCATE transactions; TRUNCATE tx_rejects;`), put the CSV back in `/data/in`.
2. Start the app with `batch.demo.fail-on-tx-id=T007` (environment variable `BATCH_DEMO_FAIL_ON_TX_ID=T007`).
3. Launch the job. `TxProcessor` throws `IllegalStateException` at T007. It is **not skippable**, so the step **fails** and its transaction rolls back.
   - With chunk size 500, the whole 7-row chunk is rolled back: `transactions` stays empty. **This is correct, atomic behavior.**
   - To see mid-way checkpoints, run once with `batch.txn.chunk-size=3` (set in config): chunk 1 (T001-T003) commits; the failure happens while processing the next chunk; the table contains T001-T003 only; the step context says the reader committed 3 items.
4. Check: `SELECT STATUS, EXIT_CODE FROM BATCH_JOB_EXECUTION` → `FAILED`; step `importStep` `FAILED`; `summaryStep`/`archiveStep` **never started**.
5. Remove the failure setting, restart the app, and call `POST /batch/restart/{failedExecutionId}` (or relaunch with the **same** parameters).
6. Observe: a **new** `JobExecution` (id 2) under the **same** `JobInstance`. `importStep` reopens the reader with the saved count and **continues after the last committed item**; steps 2 and 3 then run. Final data equals an uninterrupted run, with no duplicates (and even if a chunk were redone, `ON DUPLICATE KEY UPDATE` makes the write idempotent).

Add this scenario as an automated test: it is the single most valuable test in a batch codebase.

## 10. Where this sample is most likely to need a fix (I could not compile it)

| Area | Why it might need adjustment |
|---|---|
| Imports under `org.springframework.batch.infrastructure.*` | Package names are from the migration guide's "all APIs of `spring-batch-infrastructure` moved"; confirm each class (e.g. `RepeatStatus`, item readers/writers/builders) with your IDE |
| `ChunkContext`, `StepScope` packages | I used `org.springframework.batch.core.scope.context.ChunkContext` and `org.springframework.batch.core.configuration.annotation.StepScope`; these may have moved |
| `ChunkOrientedStepBuilder` generics/method names | `.reader`, `.processor`, `.writer`, `.transactionManager`, `.faultTolerant`, `.skip`, `.skipLimit`, `.retry`, `.retryLimit`, `.skipListener`, `.listener` come from the 6.0.5 Javadoc and "What's new"; some generic signatures were refined in later 6.0.x releases |
| `JobOperator` bean | Boot 4 should auto-configure it; if not, define one via the batch configuration |
| `SkipListener` generics | Refined in 6.0.x; adjust the type parameters if the compiler complains |
| Boot property names | `spring.batch.jdbc.initialize-schema` may be renamed; use the properties migrator |
| `INSERT ... VALUES()` in `ON DUPLICATE KEY UPDATE` | Works in MySQL 8 but `VALUES()` is deprecated since 8.0.20; the alias form (`... AS new ON DUPLICATE KEY UPDATE x = new.x`) is the modern equivalent |
| Reject `job_execution_id` via MDC | Fine for this single-threaded demo; for multi-threaded/partitioned steps pass the id explicitly instead |

If you are on **Spring Boot 3.5 / Spring Batch 5.2** instead: drop `.infrastructure` from imports, restore old packages for `Job`, `Step`, listeners and `JobParameters`, use `new StepBuilder(..).<TxCsv, Tx>chunk(size, tx)...faultTolerant()...`, inject `JobLauncher` (and `JobExplorer` for queries) instead of `JobOperator`/`JobRepository`, use `spring-boot-starter-batch` + `spring-boot-starter-jdbc`, and use `.retry(...).retryLimit(...)` from Spring Retry.

---

# Where to go next

1. **Compile and run Part 4**, then do the restart demo by hand.
2. Re-read Modules 8, 9 and 13 with the restart and skip runs in front of you; the counters will make the internals concrete.
3. Add to the sample, one at a time: a `JdbcPagingItemReader` variant of step 2; a partitioned step; a retention job that purges old metadata; a Micrometer dashboard.
4. Read the official reference for your exact version, especially the chunk-oriented step, scalability and migration pages, and keep the 6.0 migration guide open while working through any v5 code you find online. Most blog posts still show v5 APIs.

**Sources used for version-specific facts:** Spring Batch 6.0 Migration Guide (github.com/spring-projects/spring-batch/wiki/Spring-Batch-6.0-Migration-Guide); "What's new in Spring Batch 6" (docs.spring.io/spring-batch/reference/whatsnew.html); Spring Boot 4.0 Migration Guide (github.com/spring-projects/spring-boot/wiki/Spring-Boot-4.0-Migration-Guide); Spring Batch 6.0.5 API Javadoc (`ChunkOrientedStepBuilder`, `JobOperator`, deprecated list); Spring Batch GitHub release notes and issues #5077, #5127, #5152 (fault-tolerance and parameter-exception details).
