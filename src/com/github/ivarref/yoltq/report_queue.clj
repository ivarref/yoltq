(ns com.github.ivarref.yoltq.report-queue
  (:require [com.github.ivarref.yoltq.utils :as u]
            [com.github.ivarref.yoltq.impl :as i]
            [datomic.api :as d]
            [clojure.tools.logging :as log])
  (:import (datomic Connection Datom)
           (java.util.concurrent LinkedBlockingQueue ScheduledExecutorService BlockingQueue TimeUnit)))

; Private API, subject to change!

(defn process-poll-result! [cfg id-ident poll-result consumer]
  (let [{:keys [tx-data db-after]} poll-result]
    (when-let [new-ids (->> tx-data
                            (filter (fn [^Datom datom] (and
                                                         (= (.a datom) id-ident)
                                                         (.added datom))))
                            (mapv (fn [^Datom datom] (.v datom)))
                            (into [])
                            (not-empty))]
      (doseq [id new-ids]
        (consumer (fn []
                    (try
                      (let [{:com.github.ivarref.yoltq/keys [lock id status queue-name bindings]} (u/get-queue-item db-after id)]
                        (with-bindings (or bindings {})
                          (if (i/depends-on-waiting? cfg {:id id})
                            nil
                            (some->>
                              (u/prepare-processing db-after id queue-name lock status)
                              (i/take! cfg)
                              (i/execute! cfg)))))
                      (catch Throwable t
                        (log/error t "Unexpected error in process-poll-result!")))))))))

(defn report-queue-listener [running?
                             ready?
                             ^ScheduledExecutorService pool
                             config-atom]
  (let [cfg @config-atom
        conn (:conn cfg)
        tx-report-queue-given (contains? cfg :tx-report-queue)
        ^BlockingQueue q (if tx-report-queue-given
                           (get cfg :tx-report-queue)
                           (d/tx-report-queue conn))
        id-ident (d/q '[:find ?e .
                        :where [?e :db/ident :com.github.ivarref.yoltq/id]]
                      (d/db conn))]
    (assert (instance? BlockingQueue q))
    (log/debug "tx-report-queue-given:" tx-report-queue-given)
    (try
      (let [running-local? (atom true)]
        (while (and @running? @running-local?)
          (when-let [poll-result (.poll ^BlockingQueue q 1 TimeUnit/SECONDS)]
            (if (= poll-result :end)
              (do
                (log/debug "report-queue-listener received :end token. Exiting")
                (reset! running-local? false))
              ;(log/warn "yoltq report-queue-listener received :end token. If the rest of the system is kept running, it will result in a partially broken system."))
              (process-poll-result! @config-atom
                                    id-ident
                                    poll-result
                                    (fn [f]
                                      (when @running?
                                        (.execute ^ScheduledExecutorService pool f))))))
          (deliver ready? :ready)))
      (catch Throwable t
        (log/error t "Unexpected error in report-queue-listener:" (.getMessage t)))
      (finally
        (if tx-report-queue-given
          (log/debug "Remove tx-report-queue handled elsewhere")
          (do
            (log/debug "Remove tx-report-queue")
            (d/remove-tx-report-queue conn)))))))

; https://stackoverflow.com/a/14488425
(defn- dissoc-in
  "Dissociates an entry from a nested associative structure returning a new
  nested structure. keys is a sequence of keys. Any empty maps that result
  will not be present in the new structure."
  [m [k & ks :as keys]]
  (if ks
    (if-let [nextmap (get m k)]
      (let [newmap (dissoc-in nextmap ks)]
        (if (seq newmap)
          (assoc m k newmap)
          (dissoc m k)))
      m)
    (dissoc m k)))

(defn- queues-to-shutdown [old-state new-state]
  (assert (map? old-state))
  (assert (map? new-state))
  (doseq [x (vals new-state)]
    (assert (vector? x)))
  (doseq [x (vals old-state)]
    (assert (vector? x)))
  (let [new-qs (into #{} (mapv second (vals new-state)))]
    (reduce
      (fn [o [send-end-token? old-q]]
        ;(assert (boolean? send-end-token?))
        ;(assert (instance? BlockingQueue old-q))
        (if (contains? new-qs old-q)
          o
          (conj o [send-end-token? old-q])))
      []
      (vals old-state))))

(comment
  (queues-to-shutdown {:a [true 999] :b [false 777]}
                      {:a [true 123] :b [true 777]}))

(defn- multicast-once [conn work-item old-state new-state]
  (assert (map? old-state))
  (assert (map? new-state))
  (doseq [[send-end-token? q-to-shutdown] (queues-to-shutdown old-state new-state)]
    (if send-end-token?
      (do
        #_(log/debug "offering :end token")
        (if (.offer ^BlockingQueue q-to-shutdown :end 1 TimeUnit/MICROSECONDS)
          (log/debug "Multicaster sent :end token")
          (log/debug "Multicaster failed to send :end token")))
      (do
        (log/debug "Multicaster not sending :end token"))))
  (when (seq new-state)
    (if (some? work-item)
      (reduce-kv
        (fn [m id [send-end-token? q]]
          (let [ok-offer (.offer ^BlockingQueue q work-item 1 TimeUnit/MICROSECONDS)]
            (if (true? ok-offer)
              (assoc m id [send-end-token? q])
              (log/error "Multicaster failed to offer item for connection" conn "and queue id" id))))
        {}
        new-state)
      new-state)))

(defonce ^:private multicast-state-lock (Object.))
(defonce ^:private consumer-state-lock (Object.))
(defonce ^:private multicast-state (atom {}))
(defonce ^:private thread-count (atom 0))

(defn- multicaster-loop [init-state conn ready?]
  (assert (instance? Connection conn))
  (let [input-queue (d/tx-report-queue conn)]
    (deliver ready? true)
    (loop [old-state init-state]
      (let [work-item (.poll ^BlockingQueue input-queue 16 TimeUnit/MILLISECONDS)
            new-state (locking multicast-state-lock
                        ; writer to `multicast-state` must be protected by `multicast-state-lock`
                        ; it should block minimally / spend minimum amount of time
                        (swap! multicast-state (fn [old-state] (update-in old-state [:iter-count conn] (fnil inc 0))))
                        (if-let [new-state (multicast-once conn work-item old-state (get-in @multicast-state [:queues conn] {}))]
                          new-state
                          (do (swap! multicast-state (fn [old-state] (dissoc-in old-state [:queues conn])))
                              (swap! multicast-state (fn [old-state] (update-in old-state [:thread-count conn] dec)))
                              (d/remove-tx-report-queue conn)
                              (log/debug "Multicaster removed tx-report-queue for conn" conn)
                              nil)))]
        (if new-state
          (recur new-state)
          nil)))))

(defn- start-multicaster! [conn]
  (assert (instance? Connection conn))
  (let [ready? (promise)]
    (future
      (log/debug "Multicaster starting for conn" conn)
      (try
        (swap! thread-count inc)
        (let [new-state (swap! multicast-state (fn [old-state] (update-in old-state [:thread-count conn] (fnil inc 0))))]
          (assert (= 1 (get-in new-state [:thread-count conn])))
          ; "parent" thread holds `multicast-state-lock` and
          ; waits for `ready?` promise, so effectively this new thread also holds
          ; the lock until `ready?` is delivered. That is: it is safe
          ; for this thread to modify multicast-state regardless of what other threads are doing
          (multicaster-loop (get-in new-state [:queues conn]) conn ready?))
        (catch Throwable t
          (log/error t "Unexpected error in multicaster:" (.getMessage t))
          (log/error "Multicaster exiting for conn"))
        (finally
          (swap! thread-count dec)
          (log/debug "Multicaster exiting for conn" conn))))
    (when (= :timeout (deref ready? 30000 :timeout))
      (throw (RuntimeException. "Timed out waiting for multicaster to start")))))

(defn- wait-multicast-thread-step
  [conn state]
  ; `get-tx-report-queue-multicast!` should return only when the multicaster thread
  ; has picked up the new queue.
  ;
  ; Otherwise the following could happen:
  ; 1. multicast thread is sleeping
  ; 2: user-thread calls get-tx-report-queue-multicast! with `send-end-token?` `true`
  ; 3: user-thread (or somebody else) calls `stop-multicaster`.
  ;    The multicast-state atom is now identical as it was in step 1.
  ; ,  Step 2 and 3 happened while the multicast thread was sleeping.
  ; 4: The multicast thread is scheduled and does _not_ detect any state change.
  ;    Therefore the multicast thread does _not_ send out an :end token as one would expect.
  ;
  ; The new queue is written to memory at this point. No other thread can remove it because
  ; we are still, and have been during the modification of multicast-state, holding consumer-state-lock.
  ; This means that the multicast thread cannot exit at this point. Also, because we hold the consumer-state-lock,
  ; we can be sure that no other thread changes or has changed the state.
  ;
  ; Once [:iter-count conn] has changed, we know that the multicaster thread
  ; has seen the new queue. This means that we can be sure that the queue
  ; will receive the `:end` token if the queue is stopped.
  (let [start-ms (System/currentTimeMillis)
        iter-count (get-in state [:iter-count conn] -1)]
    (loop [spin-count 0]
      (if (not= iter-count (locking multicast-state-lock
                             (get-in @multicast-state [:iter-count conn] -1)))
        nil
        (let [spent-ms (- (System/currentTimeMillis) start-ms)]
          (if (> spent-ms 30000)
            (throw (RuntimeException. "Timed out waiting for multicaster thread"))
            (do
              (Thread/sleep 16)
              (recur (inc spin-count)))))))))

(defn get-tx-report-queue-multicast!
  "Multicast the datomic.api/tx-report-queue to different consumers.
   A multicaster thread is started on demand per connection. `conn` and `id` identifies the consumer.
   Repeated calls using the same `conn` and `id` returns the same queue.

   The optional third parameter, `send-end-token?`, if set to `true`, instructs the multicaster thread
   to send `:end` if the queue is stopped. The default value is `false`.

   A queue may be stopped using `stop-multicaster-id!`, `stop-multicaster!` or `stop-all-multicasters!`.

   Returns a `java.util.concurrent.BlockingQueue` like `datomic.api/tx-report-queue`."
  ([conn id]
   (get-tx-report-queue-multicast! conn id false))
  ([conn id send-end-token?]
   (assert (instance? Connection conn))
   (locking consumer-state-lock
     (let [[new-state the-q]
           (locking multicast-state-lock
             (assert (map? @multicast-state))
             (if-let [existing-q (get-in @multicast-state [:queues conn id])]
               (do
                 (let [new-state (swap! multicast-state
                                        (fn [old-state]
                                          (update-in old-state [:queues conn id] (fn [[end-token? q]]
                                                                                   (if (not= end-token? send-end-token?)
                                                                                     (log/debug "flipped `send-end-token?`")
                                                                                     (log/debug "identical `send-end-token?`"))
                                                                                   [send-end-token? q]))))]
                   (log/debug "Returning existing queue for id" id)
                   (assert (instance? BlockingQueue (second existing-q)))
                   [new-state (second existing-q)]))
               (let [needs-multicaster? (nil? (get-in @multicast-state [:queues conn]))
                     new-q (LinkedBlockingQueue.)
                     new-state (swap! multicast-state (fn [old-state] (assoc-in old-state [:queues conn id] [send-end-token? new-q])))]
                 (if needs-multicaster?
                   (do
                     (start-multicaster! conn)
                     (log/debug "Returning new queue for id" id "(multicaster thread started)")
                     [new-state new-q])
                   (do
                     (log/debug "Returning new queue for id" id "(multicaster thread already running)")
                     [new-state new-q])))))]
       ; wait for multicaster thread to pick up current Queue
       (wait-multicast-thread-step conn new-state)
       the-q))))

(defn- wait-multicast-threads-exit [[old-state new-state]]
  (assert (map? old-state))
  (assert (map? new-state))
  (assert (map? (get old-state :queues {})))
  (assert (map? (get new-state :queues {})))
  (assert (map? (get old-state :thread-count {})))
  (assert (map? (get new-state :thread-count {})))
  (locking consumer-state-lock
    ; No new multicast threads will be launched inside this block.
    ; The lock is already held by parent function.
    ;
    ; Why do we need to _wait_ for multicaster thread(s) to exit after
    ; removing all queue ids for a given connection?
    ; Otherwise the following could happen:
    ; 1. multicaster thread is sleeping
    ; 2. user calls stop-multicaster!
    ;    One would expect that multicaster thread would exit, but it is still sleeping
    ; 3. user calls get-tx-report-queue-multicast! with the same conn
    ;    The state is now empty, so a new multicaster thread is spawned.
    ; 4. Now there is two multicaster threads for the same connection!
    ;    ... and since the datomic report queue can be shared between threads
    ;    it will seemingly work, but when the end event is sent, it will be
    ;    sent by multiple threads.
    (let [old-conns (into #{} (keys (get old-state :queues {})))
          new-conns (into #{} (keys (get new-state :queues {})))]
      (assert (every?
                (fn [x] (instance? Connection x))
                old-conns))
      (assert (every?
                (fn [x] (instance? Connection x))
                new-conns))
      (doseq [old-conn old-conns]
        (when-not (contains? new-conns old-conn)
          (let [old-threadcount (get-in old-state [:thread-count old-conn] nil)]
            (assert (= 1 old-threadcount))
            (let [start-ms (System/currentTimeMillis)]
              (loop []
                (if (= 0 (get-in @multicast-state [:thread-count old-conn]))
                  :ok
                  (do
                    (let [spent-ms (- (System/currentTimeMillis) start-ms)]
                      (if (> spent-ms 30000)
                        (throw (RuntimeException. "Timed out waiting for multicaster thread to exit"))
                        (do
                          (Thread/sleep 16)
                          (recur))))))))))))))

(defn- all-queues [state]
  (->> (mapcat (fn [[conn qmap]]
                 (mapv (fn [q-id] [conn q-id])
                       (keys qmap)))
               (seq (get state :queues {})))
       (into #{})))

(comment
  (do
    (assert (= #{}
               (all-queues {})))
    (assert (= #{}
               (all-queues {:queues {}})))
    (assert (= #{[:conn-a :q-id]}
               (all-queues {:queues {:conn-a {:q-id 1}}})))
    (assert (= #{[:conn-a :q-id] [:conn-a :q-id-2]}
               (all-queues {:queues {:conn-a {:q-id 1 :q-id-2 2}}})))
    (assert (= #{[:conn-a :q-id-2] [:conn-b :q-id-3] [:conn-a :q-id]}
               (all-queues {:queues {:conn-a {:q-id 1 :q-id-2 2}
                                     :conn-b {:q-id-3 3}}})))))

(defn- removed-queues? [old new]
  (not= (all-queues old)
        (all-queues new)))

(defn stop-multicast-consumer-id! [conn id]
  (assert (instance? Connection conn))
  (let [did-remove? (atom nil)]
    (locking consumer-state-lock
      (wait-multicast-threads-exit
        (locking multicast-state-lock
          (let [[old new] (swap-vals! multicast-state (fn [old-state]
                                                        (let [new-state (dissoc-in old-state [:queues conn id])]
                                                          (if (= {} (get-in new-state [:queues conn]))
                                                            (dissoc-in old-state [:queues conn])
                                                            new-state))))]
              (reset! did-remove? (removed-queues? old new))
            [old new]))))
    @did-remove?))

(defn stop-multicaster! [conn]
  (assert (instance? Connection conn))
  (let [did-remove? (atom nil)]
    (locking consumer-state-lock
      (wait-multicast-threads-exit
        (locking multicast-state-lock
          (let [[old new] (swap-vals! multicast-state (fn [old-state] (dissoc-in old-state [:queues conn])))]
            (reset! did-remove? (removed-queues? old new))
            [old new]))))
    @did-remove?))

(defn stop-all-multicasters! []
  (let [did-remove? (atom nil)]
    (locking consumer-state-lock
      (wait-multicast-threads-exit
        (locking multicast-state-lock
          (let [[old new] (swap-vals! multicast-state (fn [old-state] (assoc old-state :queues {})))]
            (reset! did-remove? (removed-queues? old new))
            [old new]))))
    @did-remove?))

(comment
  (do
    (require 'com.github.ivarref.yoltq.log-init)
    (require '[datomic.api :as d])
    (com.github.ivarref.yoltq.log-init/init-logging!
      [[#{"datomic.*" "com.datomic.*" "org.apache.*"} :warn]
       [#{"com.github.ivarref.yoltq.report-queue"} :debug]
       [#{"com.github.ivarref.yoltq.poller"} :info]
       [#{"com.github.ivarref.yoltq"} :debug]
       ;[#{"ivarref.yoltq*"} :info]
       [#{"*"} :info]])
    (defonce conn (let [uri (str "datomic:mem://demo")
                        _ (d/delete-database uri)
                        _ (d/create-database uri)
                        conn (d/connect uri)]
                    conn))))

(comment
  (do
    (require 'com.github.ivarref.yoltq.log-init)
    (defn drain! [^BlockingQueue q]
      (loop [items []]
        (if-let [elem (.poll q 100 TimeUnit/MILLISECONDS)]
          (recur (conj items elem))
          items)))
    (com.github.ivarref.yoltq.log-init/init-logging!
      [[#{"datomic.*" "com.datomic.*" "org.apache.*"} :warn]
       [#{"com.github.ivarref.yoltq.report-queue"} :debug]
       [#{"com.github.ivarref.yoltq.poller"} :info]
       [#{"com.github.ivarref.yoltq"} :debug]
       ;[#{"ivarref.yoltq*"} :info]
       [#{"*"} :info]])
    (log/info "********************************")
    (defonce conn (let [uri (str "datomic:mem://demo")
                        _ (d/delete-database uri)
                        _ (d/create-database uri)
                        conn (d/connect uri)]
                    conn))
    (log/info "stop-all!")
    (stop-all-multicasters!)
    (assert (= 0 @thread-count))
    (let [q1 (get-tx-report-queue-multicast! conn :q1 false)
          q2 (get-tx-report-queue-multicast! conn :q2 false)
          _ (get-tx-report-queue-multicast! conn :q1 true)]
      @(d/transact conn [{:db/doc "demo"}])
      @(d/transact conn [{:db/doc "demo"}])
      @(d/transact conn [{:db/doc "demo"}])
      (log/info "begin drain q1")
      (stop-multicast-consumer-id! conn :q1)
      (stop-multicast-consumer-id! conn :q1)
      (println "thread count" @thread-count)
      (let [qitems-2 (drain! q2)
            qitems-1 (drain! q1)]
        (assert (= :end (last qitems-1)))
        (println "drain count q1:" (count qitems-1))
        (println "drain count q2:" (count qitems-2))))))

(comment
  (do
    (let [q (get-tx-report-queue-multicast! conn :q1 true)]
      (log/debug "stopping id :q1")
      (stop-multicaster-id! conn :q1)
      (let [drained (drain! q)]
        (println "drained:" drained)
        (assert (= [:end] drained)))
      @multicast-state)))

(comment
  (stop-all-multicasters!))

(comment
  (do
    (let [q (get-tx-report-queue-multicast! conn :q2 false)]
      (println "drain count:" (count (drain! q)))
      @multicast-state
      nil)))

(comment
  (get-tx-report-queue-multicast! conn :q1 false)
  (get-tx-report-queue-multicast! conn :q1 true))

(comment
  (do
    @(d/transact conn [{:db/doc "demo"}])
    @(d/transact conn [{:db/doc "demo"}])
    @(d/transact conn [{:db/doc "demo"}])
    :yay))