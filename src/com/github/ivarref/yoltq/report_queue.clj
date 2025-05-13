(ns com.github.ivarref.yoltq.report-queue
  (:require [com.github.ivarref.yoltq.utils :as u]
            [com.github.ivarref.yoltq.impl :as i]
            [datomic.api :as d]
            [clojure.tools.logging :as log])
  (:import (datomic Connection Datom)
           (java.util.concurrent LinkedBlockingQueue ScheduledExecutorService BlockingQueue TimeUnit)))


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
      (while @running?
        (when-let [poll-result (.poll ^BlockingQueue q 1 TimeUnit/SECONDS)]
          (process-poll-result! @config-atom
                                id-ident
                                poll-result
                                (fn [f]
                                  (when @running?
                                    (.execute ^ScheduledExecutorService pool f)))))
        (deliver ready? :ready))
      (catch Throwable t
        (log/error t "Unexpected error in report-queue-listener:" (.getMessage t)))
      (finally
        (if tx-report-queue-given
          (log/debug "Remove tx-report-queue handled elsewhere")
          (do
            (log/debug "Remove tx-report-queue")
            (d/remove-tx-report-queue conn)))))))

(defonce ^:private multicast-state-lock (Object.))

(defonce ^:private multicast-state (atom {}))

(defn- start-multicaster! [conn]
  (let [multicaster-ready? (promise)]
    (future
      (log/debug "Multicaster starting for conn" conn)
      (try
        (let [input-queue (d/tx-report-queue conn)]
          (loop []
            (when-let [mcast-state (get @multicast-state conn)]
              (when-let [dest-queues (vals mcast-state)]
                (let [element (.poll ^BlockingQueue input-queue 1 TimeUnit/SECONDS)]
                  (deliver multicaster-ready? :ready)
                  (when (some? element)
                    (doseq [q dest-queues]
                      (let [ok-offer (.offer ^BlockingQueue q element 30 TimeUnit/MINUTES)]
                        (when (false? ok-offer)
                          (log/error "Failed to offer item in multicaster for connection" conn))))))
                (recur)))))
        (catch Throwable t
          (deliver multicaster-ready? :error)
          (log/error t "Unexpected error in multicaster:" (.getMessage t)))
        (finally
          (d/remove-tx-report-queue conn)
          (log/debug "Multicaster exiting for conn" conn))))
    multicaster-ready?))

(defn get-tx-report-queue-multicast!
  "Multicast the datomic.api/tx-report-queue to different consumers.
  The multicaster is started on demand. `conn` and `id` identifies the consumer.

  Returns a `java.util.concurrent.BlockingQueue` like `datomic.api/tx-report-queue`."
  [conn id]
  (assert (instance? Connection conn))
  (assert (keyword? id))
  (locking multicast-state-lock
    (assert (map? @multicast-state))
    (if-let [existing-q (get-in @multicast-state [conn id])]
      (do
        (log/debug "returning existing queue for id" id)
        (assert (instance? BlockingQueue existing-q))
        existing-q)
      (let [needs-multicaster? (not (contains? @multicast-state conn))
            new-state (swap! multicast-state (fn [old-state] (assoc-in old-state [conn id] (LinkedBlockingQueue.))))]
        (when needs-multicaster?
          (let [multicaster-promise (start-multicaster! conn)
                multicaster-result (deref multicaster-promise (* 30 60000) :timeout)]
            (cond (= multicaster-result :timeout)
                  (do
                    (log/error "Timeout waiting for multicaster to start")
                    (throw (RuntimeException. "Timeout waiting for multicaster to start")))
                  (= multicaster-result :error)
                  (do
                    (log/error "Multicaster failed to start")
                    (throw (RuntimeException. "Multicaster failed to start")))
                  (= multicaster-result :ready)
                  (log/debug "Multicaster is ready")

                  :else
                  (do
                    (log/error "Unexpected state from multicaster:" multicaster-result)
                    (throw (RuntimeException. (str "Unexpected state from multicaster: " multicaster-result)))))))
        (let [new-q (get-in new-state [conn id])]
          (assert (instance? BlockingQueue new-q))
          new-q)))))

(defn stop-all-multicasters! []
  (reset! multicast-state {}))

(comment
  (do
    (require 'com.github.ivarref.yoltq.log-init)
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
  (defn drain! [^BlockingQueue q]
    (loop [cnt 0]
      (if (nil? (.poll q 1 TimeUnit/SECONDS))
        cnt
        (recur (inc cnt))))))

(comment
  (let [q-1 (get-tx-report-queue-multicast! conn :q1)
        q-2 (get-tx-report-queue-multicast! conn :q2)]))

(comment
  (drain! (get-tx-report-queue-multicast! conn :q1)))

(comment
  (do
    @(d/transact conn [{:db/doc "demo"}])
    :yay))