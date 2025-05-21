# yoltq

An opinionated Datomic queue for building (more) reliable systems. 
Implements the [transactional outbox](https://microservices.io/patterns/data/transactional-outbox.html)
pattern.
Supports retries, backoff, ordering and more.
On-prem only.

## Installation

[![Clojars Project](https://img.shields.io/clojars/v/com.github.ivarref/yoltq.svg)](https://clojars.org/com.github.ivarref/yoltq)

## 1-minute example

```clojure
(require '[com.github.ivarref.yoltq :as yq])

(def conn (datomic.api/connect "..."))

; Initialize system
(yq/init! {:conn conn})

; Add a queue consumer that will intentionally fail on the first attempt
(yq/add-consumer! :q
                 (let [cnt (atom 0)]
                   (fn [payload]
                     (when (= 1 (swap! cnt inc))
                       ; A consumer throwing an exception is considered a queue job failure
                       (throw (ex-info "failed" {})))
                     ; Anything else than a throwing exception is considered a queue job success
                     ; This includes nil, false and everything else.
                     (log/info "got payload" payload))))

; Start threadpool that picks up queue jobs
(yq/start!)

; Queue a job
@(d/transact conn [(yq/put :q {:work 123})])

; On your console you will see something like this:
; 17:29:54.598 DEBUG queue item 613... for queue :q is pending status :init
; 17:29:54.602 DEBUG queue item 613... for queue :q now has status :processing
; 17:29:54.603 DEBUG queue item 613... for queue :q is now processing
; 17:29:54.605 WARN  queue-item 613... for queue :q now has status :error after 1 try in 4.8 ms
; 17:29:54.607 WARN  error message was: "failed" for queue-item 613...
; 17:29:54.615 WARN  ex-data was: {} for queue-item 613...
; The item is so far failed...

; But after approximately 10 seconds have elapsed, the item will be retried:
; 17:30:05.596 DEBUG queue item 613... for queue :q now has status :processing
; 17:30:05.597 DEBUG queue item 613... for queue :q is now processing
; 17:30:05.597 INFO  got payload {:work 123}
; 17:30:05.599 INFO  queue-item 613... for queue :q now has status :done after 2 tries in 5999.3 ms
; And then it has succeeded.
```

## Rationale

Integrating with external systems that may be unavailable can be tricky.
Imagine the following code:

```clojure
(defn post-handler [user-input]
  (let [db-item (process user-input)
        ext-ref (clj-http.client/post ext-service {:connection-timeout 3000  ; timeout in milliseconds 
                                                   :socket-timeout     10000 ; timeout in milliseconds
                                                   ...})] ; may throw exception
    @(d/transact conn [(assoc db-item :some/ext-ref ext-ref)])))
```

What if the POST request fails? Should it be retried? For how long?
Should it be allowed to fail? How do you then process failures later?

PS: If you do not set connection/socket-timeout, there is a chance that
clj-http/client will wait for all eternity in the case of a dropped TCP connection.

The queue way to solve this would be:

```clojure
(defn get-ext-ref [{:keys [id]}]
  (let [ext-ref (clj-http.client/post ext-service {:connection-timeout 3000  ; timeout in milliseconds 
                                                   :socket-timeout     10000 ; timeout in milliseconds
                                                   ...})] ; may throw exception
    @(d/transact conn [[:db/cas [:some/id id]
                        :some/ext-ref
                        nil
                        ext-ref]])))

(yq/add-consumer! :get-ext-ref get-ext-ref {:allow-cas-failure? true})

(defn post-handler [user-input]
  (let [{:some/keys [id] :as db-item} (process user-input)]
    @(d/transact conn [db-item
                       (yq/put :get-ext-ref {:id id})])))
```

Here `post-handler` will always succeed as long as the transaction commits.

`get-ext-ref` may fail multiple times if `ext-service` is down.
This is fine as long as it eventually succeeds.

There is a special case where `get-ext-ref` succeeds, but 
saving the new queue job status to the database fails.
Thus `get-ext-ref` and any queue consumer should tolerate to 
be executed successfully several times.

For `get-ext-ref` this is solved by using
the database function [:db/cas (compare-and-swap)](https://docs.datomic.com/on-prem/transactions/transaction-functions.html#dbfn-cas)
to achieve a write-once behaviour.
The yoltq system treats cas failures as job successes
when a consumer has `:allow-cas-failure?` set to `true` in its options.

## How it works

### Queue jobs

Creating queue jobs is done by `@(d/transact conn [...other data... (yq/put :q {:work 123})])`.
Inspecting `(yq/put :q {:work 123})]` you will see something like this:

```clojure
#:com.github.ivarref.yoltq{:id #uuid"614232a8-e031-45bb-8660-be146eaa32a2", ; Queue job id 
                           :queue-name :q, ; Destination queue                                 
                           :status :init, ; Status
                           :payload "{:work 123}", ; Payload persisted to the database with pr-str
                           :bindings "{}", ; Bindings that will be applied before executing consumer function
                           :lock #uuid"037d7da1-5158-4243-8f72-feb1e47e15ca", ; Lock to protect from multiple consumers
                           :tries 0, ; How many times the job has been executed
                           :init-time 4305758012289 ; Time of initialization (System/nanoTime)
                           }
```

This is the queue job as it will be stored into the database. 
You can see that the payload, i.e. the second argument of `yq/put`,
is persisted into the database. Thus the payload must be `pr-str`-able (unless you have specified
custom `:encode` and `:decode` functions that override this).


A queue job will initially have status `:init`.
It will then transition to the following statuses:

* `:processing`: When the queue job begins processing in the queue consumer function.
* `:done`: If the queue consumer function returns normally.
* `:error`: If the queue consumer function throws an exception.

### Queue consumers

Queue jobs will be consumed by queue consumers. A consumer is a function taking a single argument,
the payload. It can be added like this:

```clojure
(yq/add-consumer! 
  :q ; Queue to consume  
  (fn [payload] (println "got payload:" payload)) ; Queue consumer function
  ; An optional map of queue opts
  {:allow-cas-failure? true ; Treat [:db.cas ...] failures as success. This is one way for the
                            ; consumer function to ensure idempotence.
   :valid-payload? (fn [payload] (some? (:id payload))) ; Function that verifies payload. Should return truthy for valid payloads.
                                                        ; The default function always returns true.
   :max-retries 10})        ; Specify maximum number of times an item will be retried. Default: 10000.
                            ; If :max-retries is given as 0, the job will ~always be retried, i.e.
                            ; 9223372036854775807 times (Long/MAX_VALUE).
```

The `payload` will be deserialized from the database using `clojure.edn/read-string` before invocation, i.e.
you will get back what you put into `yq/put`.

The yoltq system treats a queue consumer function invocation as successful if it does not throw an exception.
Any return value, be it `nil`, `false`, `true`, etc. is considered a success.

### Listening for queue jobs

When `(yq/start!)` is invoked, a threadpool is started.

One thread is permanently allocated for listening to the 
[tx-report-queue](https://docs.datomic.com/on-prem/clojure/index.html#datomic.api/tx-report-queue)
and responding to changes. This means that yoltq will respond 
and process newly created queue jobs fairly quickly.
This also means that queue jobs in status `:init` will almost always be processed without
any type of backoff.

The threadpool also schedules polling jobs that will check for various statuses regularly:

* Jobs in status `:error` that have waited for at least `:error-backoff-time` (default: 5 seconds) will be retried.
* Jobs that have been in `:processing` for at least `:hung-backoff-time` (default: 30 minutes) will be considered hung and retried.
* Old `:init-backoff-time` (default: 1 minute) `:init` jobs that have not been processed. Queue jobs can be left in status `:init` during application restart/upgrade, and thus the need for this strategy.


### Retry and backoff strategy

Yoltq assumes that if a queue consumer throws an exception for one item, it
will also do the same for another item in the immediate future, 
assuming the remote system that the queue consumer represents is still down.
Thus if there are ten failures for queue `:q`, it does not make sense to
retry all of them at once.

The retry polling job that runs regularly (`:poll-delay`, default: every 10 seconds)
thus stops at the first failure.
Each queue have their own polling job, so if one queue is down, it will *not* stop
other queues from retrying.

The retry polling job will continue to eagerly process queue jobs as long as it 
encounters only successes.

While the `:error-backoff-time` of default 5 seconds may seem short, in practice
if there is a lot of failed items and the external system is still down,
the actual backoff time will be longer.


### Stuck threads and stale jobs

A single thread is dedicated to monitoring how much time a queue consumer 
spends on a single job. If this exceeds `:max-execute-time` (default: 5 minutes)
the stack trace of the offending consumer will be logged as `:ERROR`.

If a job is found stale, that is if the database spent time exceeds 
`:hung-backoff-time` (default: 30 minutes),
the job will either be retried or marked as `:error`. This case may happen if the application
is shut down abruptly during processing of queue jobs.


### Giving up

A queue job will remain in status `:error` once `:max-retries` (default: 10000) have been reached.
If `:max-retries` is given as `0`, the job will be retried 9223372036854775807 times before
giving up.
Ideally this should not happen. ¯\\\_(ツ)\_/¯

### Custom encoding and decoding

Yoltq will use `pr-str` and `clojure.edn/read-string` by default to encode and decode data.
You may specify `:encode` and `:decode` either globally or per queue to override this behaviour.
The `:encode` function must return a byte array or a string.

For example if you want to use [nippy](https://github.com/ptaoussanis/nippy):
```clojure
(require '[taoensso.nippy :as nippy])

; Globally for all queues:
(yq/init!
  {:conn conn
   :encode nippy/freeze
   :decode nippy/thaw})

; Or per queue:
(yq/add-consumer! 
  :q ; Queue to consume  
  (fn [payload] (println "got payload:" payload)) ; Queue consumer function
  {:encode nippy/freeze
   :decode nippy/thaw}) ; Queue options, here with :encode and :decode
```

### Partitions

Yoltq supports specifying which [partition](https://docs.datomic.com/on-prem/schema/schema.html#partitions)
queue entities should belong to.
The default function is:
```clojure
(defn default-partition-fn [_queue-name]
  (keyword "yoltq" (str "queue_" (.getValue (java.time.Year/now)))))
```
This is to say that there will be a single partition per year for yoltq.
Yoltq will take care of creating the partition if it does not exist.

You may override this function, either globally or per queue, with the keyword `:partition-fn`.
E.g.:
```clojure
(yq/init! {:conn conn :partition-fn (fn [_queue-name] :my-partition)})
```

### All configuration options

For an exhaustive list of all configuration options,
see [yq/default-opts](https://github.com/ivarref/yoltq/blob/main/src/com/github/ivarref/yoltq.clj#L21).


# Regular and REPL usage

For a regular system and/or REPL session you'll want to do:

```clojure
(require '[com.github.ivarref.yoltq :as yq])

(yq/init! {:conn conn})

(yq/add-consumer! :q-one ...)
(yq/add-consumer! :q-two ...)

; Start yoltq system
(yq/start!)

; Oops I need another consumer. This works fine:
(yq/add-consumer! :q-three ...)

; When the application is shutting down:
(yq/stop!)
```

You may invoke `yq/add-consumer!` and `yq/init!` on a live system as you like.

If you change `:pool-size` or `:poll-delay` you will have to `(yq/stop!)` and
`(yq/start!)` to make changes take effect.

## Queue job dependencies and ordering

It is possible to specify that one queue job must wait for another queue
job to complete before it will be executed:

```clojure
@(d/transact conn [(yq/put :a 
                           ; Payload:
                           {:id "a1"}
                           ; Job options:
                           {:id "a1"})])

@(d/transact conn [(yq/put :b 
                           ; Payload:
                           {:id "b1" :a-ref "a1"}
                           ; Jobs options:
                           {:depends-on [:a "a1"]})])

; depends-on may also be specified as a function of the payload when 
; adding the consumer:
(yq/add-consumer! :b 
                  (fn [payload] ...)
                  {:depends-on (fn [payload]
                                 [:a (:a-ref payload)])})
```

Here queue job `b1` will not execute before `a1` is `:done`.

Note that queue-name plus `:id` in job options must be an unique value.
In the example above that means `:a` plus `a1` must be unique.

When specifying `:depends-on`, the referred job must at least exist in the database,
otherwise `yq/put` will throw an exception.

Other than this there is no attempt at ordering the execution of queue jobs.
In fact the opposite is done in the poller to guard against the case that a single failing queue job
could effectively take down the entire retry polling job.

## Retrying jobs in the REPL

```clojure
(require '[com.github.ivarref.yoltq :as yq])

; List jobs that are in state error:
(yq/get-errors :q)

; This will retry a single job that is in error, regardless 
; of how many times it has been retried earlier.
; If the job fails, you will get the full stacktrace on the REPL.
(yq/retry-one-error! :q)
; Returns a map containing the new state of the job.
; Returns nil if there are no (more) jobs in state error for this queue.
```

# Testing

For testing you will probably want determinism over an extra threadpool
by using the test queue:

```clojure
...
(:require [clojure.test :refer :all]
  [com.github.ivarref.yoltq :as yq]
  [com.github.ivarref.yoltq.test-queue :as tq])

; Enables the test queue and disables the threadpool for each test.
; yq/start! and yq/stop! becomes a no-op.
(use-fixtures :each tq/call-with-virtual-queue!)

(deftest demo
         (let [conn ...]
           (yq/init! {:conn conn}) ; Setup
           (yq/add-consumer! :q identity)

           @(d/transact conn [(yq/put :q {:work 123})]) ; Add work

           ; tq/consume! consumes one job and asserts that it succeeds.
           ; It returns the return value of the consumer function
           (is (= {:work 123} (tq/consume! :q)))
           
           ; If you want to test the idempotence of your function, 
           ; you may force retry a consumer function:
           ; This may for example be useful to verify that the
           ; :db.cas logic is correct.
           (is (= {:work 123} (tq/force-retry! :q)))))
```

## Logging and capturing bindings

Yoltq can capture and restore dynamic bindings.
It will capture during `yq/put` and restore them when the consumer function
is invoked. This is specified in the `:capture-bindings` setting.
It defaults to `['#taoensso.timbre/*context*]`, 
i.e. the [timbre](https://github.com/ptaoussanis/timbre) log context,
if available, otherwise an empty vector.

These dynamic bindings will be in place when yoltq logs errors, warnings
etc. about failing consumer functions, possibly making troubleshooting
easier.

## Limitations

Datomic does not have anything like `for update skip locked`.
Thus consuming a queue should be limited to a single JVM process.
This library will take queue jobs by compare-and-swapping a lock+state,
process the item and then compare-and-swapping the lock+new-state.
It does so eagerly, thus if you have multiple JVM consumers you will
most likely get many locking conflicts. It should work, but it's far
from optimal.

## Alternatives

I did not find any alternatives for Datomic.

If I were using PostgreSQL or any other database that supports
`for update skip locked`, I'd use a queue that uses this.
For Clojure there is [proletarian](https://github.com/msolli/proletarian).

For Redis there is [carmine](https://github.com/ptaoussanis/carmine).

Note: I have not tried these libraries myself.

## Other stuff

If you liked this library, you may also like:

* [conformity](https://github.com/avescodes/conformity): A Clojure/Datomic library for idempotently transacting norms into your database – be they schema, data, or otherwise.
* [datomic-schema](https://github.com/ivarref/datomic-schema): Simplified writing of Datomic schemas (works with conformity).
* [double-trouble](https://github.com/ivarref/double-trouble):  Handle duplicate Datomic transactions with ease.
* [gen-fn](https://github.com/ivarref/gen-fn): Generate Datomic function literals from regular Clojure namespaces.
* [rewriting-history](https://github.com/ivarref/rewriting-history): A library to rewrite Datomic history.

## Change log

#### 2025-05-13 v0.2.?? [diff](https://github.com/ivarref/yoltq/compare/v0.2.64...HEAD)
Added support for specifying `tx-report-queue` as a keyword in `init!`. Yoltq will
then not grab the datomic report queue, but use the one provided: 

```clojure
(require '[com.github.ivarref.yoltq :as yq])
(yq/init! {:conn            conn
           :tx-report-queue (yq/get-tx-report-queue-multicast! conn :yoltq)
                            ; ^^ can be any `java.util.concurrent.BlockingQueue` value
                            })

(another-tx-report-consumer! (yq/get-tx-report-queue-multicast! conn :another-consumer-id))

```

Added multicast support for `datomic.api/tx-report-queue`:
```clojure
(require '[com.github.ivarref.yoltq :as yq])
(def my-q1 (yq/get-tx-report-queue-multicast! conn :q-id-1))
; ^^ consume my-q1 just like you would do `datomic.api/tx-report-queue`

(def my-q2 (yq/get-tx-report-queue-multicast! conn :q-id-2))
; Both my-q1 and my-q2 will receive everything from `datomic.api/tx-report-queue` for the given `conn`

(def my-q3 (yq/get-tx-report-queue-multicast! conn :q-id-3 true))
; my-q3 specifies the third argument, `send-end-token?`, to true, so it will receive `:end` if the queue is stopped.
; This can enable simpler consuming of queues:
(future
  (loop []
    (let [q-item (.take ^java.util.concurrent.BlockingQueue my-q3)]
      (if (= q-item :end)
        (println "Time to exit. Goodbye!")
        (do
          (println "Processing q-item" q-item)
          (recur))))))

@(d/transact conn [{:db/doc "new-data"}])

; Stop the queue:
(yq/stop-multicast-consumer-id! conn :q-id-3)
=> true
; The multicaster thread will send `:end` and the consumer thread will then print "Time to exit. Goodbye!".

; if the queue is already stopped (or never was started), `stop-multicaster...` functions will return false:
(yq/stop-multicast-consumer-id! conn :already-stopped-queue-or-typo)
=> false

; Stop all queues for all connections:
(yq/stop-all-multicasters!)
```

`yq/get-tx-report-queue-multicast!` returns, like
`datomic.api/tx-report-queue`,
`java.util.concurrent.BlockingQueue` and starts a background thread that does 
the multicasting as needed. Identical calls to `yq/get-tx-report-queue-multicast!`
returns the same `BlockingQueue`.

Changed the default for `max-retries` from `10000` to `9223372036854775807`.

Fixed reflection warnings.

#### 2023-03-20 v0.2.64 [diff](https://github.com/ivarref/yoltq/compare/v0.2.63...v0.2.64)
Added support for `max-retries` being `0`, meaning the job should be retried forever
(or at least 9223372036854775807 times).

Changed the default for `max-retries` from `100` to `10000`.

#### 2022-11-18 v0.2.63 [diff](https://github.com/ivarref/yoltq/compare/v0.2.62...v0.2.63)
Added custom `:encode` and `:decode` support.

Added support for specifying `:partifion-fn` to specify which partition a queue item should belong to.
It defaults to:
```clojure
(defn default-partition-fn [_queue-name]
  (keyword "yoltq" (str "queue_" (.getValue (Year/now)))))
```
Yoltq takes care of creating the partition if it does not exist.

#### 2022-11-15 v0.2.62 [diff](https://github.com/ivarref/yoltq/compare/v0.2.61...v0.2.62)
Added function `processing-time-stats`:

```clojure
(ns com.github.ivarref.yoltq)

(defn processing-time-stats
  "Gather processing time statistics.

  Optional keyword arguments:
  * :age-days —  last number of days to look at data from. Defaults to 30.
                 Use nil to have no limit.

  * :queue-name — only gather statistics for this queue name. Defaults to nil, meaning all queues.

  * :duration->long - Specify what unit should be used for values.
                      Must take a java.time.Duration as input and return a long.

                      Defaults to (fn [duration] (.toSeconds duration).
                      I.e. the default unit is seconds.

  Example return value:
  {:queue-a {:avg 1
             :max 10
             :min 0
             :p50 ...
             :p90 ...
             :p95 ...
             :p99 ...}}"
 [{:keys [age-days queue-name now db duration->long]
  :or   {age-days 30
         now      (ZonedDateTime/now ZoneOffset/UTC)
         duration->long (fn [duration] (.toSeconds duration))}}]
  ...)
```

#### 2022-09-07 v0.2.61 [diff](https://github.com/ivarref/yoltq/compare/v0.2.60...v0.2.61)
Added function `retry-stats`:

```clojure
(ns com.github.ivarref.yoltq)

(defn retry-stats
  "Gather retry statistics.

  Optional keyword arguments:
  * :age-days —  last number of days to look at data from. Defaults to 30.
  * :queue-name — only gather statistics for this queue name. Defaults to nil, meaning all queues.

  Example return value:
  {:queue-a {:ok 100, :retries 2, :retry-percentage 2.0}
   :queue-b {:ok 100, :retries 75, :retry-percentage 75.0}}

  From the example value above, we can see that :queue-b fails at a much higher rate than :queue-a.
  Assuming that the queue consumers are correctly implemented, this means that the service representing :queue-b
  is much more unstable than the one representing :queue-a. This again implies
  that you will probably want to fix the downstream service of :queue-b, if that is possible.
  "
  [{:keys [age-days queue-name now]
    :or   {age-days 30
           now      (ZonedDateTime/now ZoneOffset/UTC)}}]
  ...)
```

#### 2022-08-18 v0.2.60 [diff](https://github.com/ivarref/yoltq/compare/v0.2.59...v0.2.60)
Improved: Added config option `:healthy-allowed-error-time`:
```
    ; If you are dealing with a flaky downstream service, you may not want
    ; yoltq to mark itself as unhealthy on the first failure encounter with
    ; the downstream service. Change this setting to let yoltq mark itself
    ; as healthy even though a queue item has been failing for some time.
    :healthy-allowed-error-time    (Duration/ofMinutes 15)
```

#### 2022-08-15 v0.2.59 [diff](https://github.com/ivarref/yoltq/compare/v0.2.58...v0.2.59)
Fixed:
* Race condition that made the following possible: `stop!` would terminate the slow thread 
watcher, and a stuck thread could keep `stop!` from completing! 

#### 2022-06-30 v0.2.58 [diff](https://github.com/ivarref/yoltq/compare/v0.2.57...v0.2.58)
Slightly more safe EDN printing and parsing.
Recommended reading:
[Pitfalls and bumps in Clojure's Extensible Data Notation (EDN)](https://nitor.com/en/articles/pitfalls-and-bumps-clojures-extensible-data-notation-edn)

#### 2022-06-29 v0.2.57 [diff](https://github.com/ivarref/yoltq/compare/v0.2.56...v0.2.57)
Added `(get-errors qname)` and `(retry-one-error! qname)`.

Improved:
`unhealthy?` will return `false` for the first 10 minutes of the application lifetime.
This was done in order to push new code while a queue was in error in an earlier
version of the code. In this way rolling upgrades are possible regardless if there
are queue errors.
Can you tell that this issue hit me? ¯\\\_(ツ)\_/¯

#### 2022-06-22 v0.2.56 [diff](https://github.com/ivarref/yoltq/compare/v0.2.55...v0.2.56)
Added support for `:yoltq/queue-id` metadata on functions. I.e. it's possible to write
the following:
```clojure
(defn my-consumer
  {:yoltq/queue-id :some-queue}
  [payload]
  :work-work-work)

(yq/add-consumer! #'my-consumer ; <-- will resolve to :some-queue 
                  my-consumer)

@(d/transact conn [(yq/put #'my-consumer ; <-- will resolve to :some-queue
                           {:id "a"})])
```

The idea here is that it is simpler to jump to var definitions than going via keywords,
which essentially refers to a var/function anyway. 

#### 2022-03-29 v0.2.55 [diff](https://github.com/ivarref/yoltq/compare/v0.2.54...v0.2.55)
Added: `unhealthy?` function which returns `true` if there are queues in error,
or `false` otherwise.

#### 2022-03-28 v0.2.54 [diff](https://github.com/ivarref/yoltq/compare/v0.2.51...v0.2.54)
Fixed: Schedules should now be using milliseconds and not nanoseconds.

#### 2022-03-28 v0.2.51 [diff](https://github.com/ivarref/yoltq/compare/v0.2.48...v0.2.51)
* Don't OOM on migrating large amounts of data. 
* Respect `:auto-migrate? false`.

#### 2022-03-27 v0.2.48 [diff](https://github.com/ivarref/yoltq/compare/v0.2.46...v0.2.48)
* Auto migration is done in the background.
* Only poll for current version of jobs, thus no races for auto migration.

#### 2022-03-27 v0.2.46 [diff](https://github.com/ivarref/yoltq/compare/v0.2.41...v0.2.46)
* Critical bugfix that in some cases can lead to stalled jobs.
```
Started using (System/currentTimeMillis) and not (System/nanoTime)
when storing time in the database. 
```

* Bump Clojure to `1.11.0`.

#### 2022-03-27 v0.2.41 [diff](https://github.com/ivarref/yoltq/compare/v0.2.39...v0.2.41)
* Added function `healthy?` that returns:
```
  true if no errors
  false if one or more errors
  nil if error-poller is yet to be executed.
```

* Added default functions for `:on-system-error` and `:on-system-recovery`
  that simply logs that the system is in error (ERROR level) or has 
  recovered (INFO level). 
  
* Added function `queue-stats` that returns a nicely "formatted"
  vector of queue stats, for example:
```
  (queue-stats)
  =>
  [{:qname :add-message-thread, :status :done, :count 10274}
   {:qname :add-message-thread, :status :init, :count 30}
   {:qname :add-message-thread, :status :processing, :count 1}
   {:qname :send-message, :status :done, :count 21106}
   {:qname :send-message, :status :init, :count 56}]
```

#### 2021-09-27 v0.2.39 [diff](https://github.com/ivarref/yoltq/compare/v0.2.37...v0.2.39)
Added `:valid-payload?` option for queue consumers.

#### 2021-09-27 v0.2.37 [diff](https://github.com/ivarref/yoltq/compare/v0.2.33...v0.2.37) 
Improved error reporting.

#### 2021-09-24 v0.2.33
First publicly announced release.

## License

Copyright © 2021-2022 Ivar Refsdal

This program and the accompanying materials are made available under the
terms of the Eclipse Public License 2.0 which is available at
http://www.eclipse.org/legal/epl-2.0.

This Source Code may also be made available under the following Secondary
Licenses when the conditions for such availability set forth in the Eclipse
Public License, v. 2.0 are satisfied: GNU General Public License as published by
the Free Software Foundation, either version 2 of the License, or (at your
option) any later version, with the GNU Classpath Exception which is available
at https://www.gnu.org/software/classpath/license.html.
