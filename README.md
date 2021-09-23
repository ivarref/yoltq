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
        ext-ref (clj-http.client/post ext-service {...})] ; may throw exception
    @(d/transact conn [(assoc db-item :some/ext-ref ext-ref)])))
```

What if the POST request fails? Should it be retried? For how long?
Should it be allowed to fail? How do you then process failures later?

The queue way to solve this would be:

```clojure
(defn get-ext-ref [{:keys [id]}]
  (let [ext-ref (clj-http.client/post ext-service {...})] ; may throw exception
    @(d/transact conn [[:db/cas [:some/id id]
                        :some/ext-ref
                        nil
                        ext-ref]])))

(yq/add-consumer! :get-ext-ref get-ext-ref {:allow-cas-failure? true})

(defn post-handler [user-input]
  (let [{:some/keys [id] :as db-item} (process user-input)
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
is persisted into the database. Thus the payload must be `pr-str`-able.


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
   :max-retries 10})        ; Specify maximum number of times an item will be retried. Default: 100
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

A queue job will remain in status `:error` once `:max-retries` (default: 100) have been reached.
Ideally this will not happen.


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
                           {:id "b1"}
                           ; Jobs options:
                           {:depends-on [:a "a1"]})])
```

Here queue job `b1` will not execute before `a1` is `:done`.

Note that queue-name plus `:id` in job options must be an unique value.
In the example above that means `:a` plus `a1` must be unique.

When specifying `:depends-on`, the referred job must at least exist in the database,
otherwise `yq/put` will throw an exception.

Other than this there is no attempt at ordering the execution of queue jobs.
In fact the opposite is done in the poller to guard against the case that a single failing queue job
could effectively take down the entire retry polling job.

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
is invoked. This is specified in the `:capture-bindings` settings.
It defaults to `['#taoensso.timbre/*context*]`, 
i.e. the [timbre](https://github.com/ptaoussanis/timbre) log context,
if available, otherwise an empty vector.

These dynamic bindings will be in place when yoltq logs errors, warnings
etc. about failing consumer functions, possibly making troubleshooting
easier.


## License

Copyright © 2021 Ivar Refsdal

This program and the accompanying materials are made available under the
terms of the Eclipse Public License 2.0 which is available at
http://www.eclipse.org/legal/epl-2.0.

This Source Code may also be made available under the following Secondary
Licenses when the conditions for such availability set forth in the Eclipse
Public License, v. 2.0 are satisfied: GNU General Public License as published by
the Free Software Foundation, either version 2 of the License, or (at your
option) any later version, with the GNU Classpath Exception which is available
at https://www.gnu.org/software/classpath/license.html.