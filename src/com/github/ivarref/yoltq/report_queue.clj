(ns com.github.ivarref.yoltq.report-queue
  (:require [com.github.ivarref.yoltq.utils :as u]
            [com.github.ivarref.yoltq.impl :as i]
            [datomic.api :as d]
            [clojure.tools.logging :as log])
  (:import (datomic Datom)
           (java.util.concurrent ScheduledExecutorService BlockingQueue TimeUnit)))


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
                      (let [{:com.github.ivarref.yoltq/keys [lock id status queue-name]} (u/get-queue-item db-after id)]
                        (some->>
                          (u/prepare-processing id queue-name lock status)
                          (i/take! cfg)
                          (i/execute! cfg)))
                      (catch Throwable t
                        (log/error t "unexpected error in process-poll-result!")))))))))


(defn report-queue-listener [running?
                             ready?
                             ^ScheduledExecutorService pool
                             config-atom]
  (let [conn (:conn @config-atom)
        ^BlockingQueue q (d/tx-report-queue conn)
        id-ident (d/q '[:find ?e .
                        :where [?e :db/ident :com.github.ivarref.yoltq/id]]
                      (d/db conn))]
    (try
      (while @running?
        (when-let [poll-result (.poll ^BlockingQueue q 1 TimeUnit/SECONDS)]
          (process-poll-result! @config-atom
                                id-ident
                                poll-result
                                (fn [f]
                                  (when @running?
                                    (.execute ^ScheduledExecutorService pool f)))))
        (deliver ready? true))
      (catch Throwable t
        (log/error t "unexpected error in report-queue-listener"))
      (finally
        (log/debug "remove tx-report-queue")
        (d/remove-tx-report-queue conn)))))