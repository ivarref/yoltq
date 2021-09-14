# yoltq

An opinionated Datomic queue for building (more) reliable systems. 
Implements the [transactional outbox](https://microservices.io/patterns/data/transactional-outbox.html)
pattern.
Supports retries, backoff and more.
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

; Start threadpool
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
