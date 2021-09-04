(ns ivarref.yoltq.virtual-queue
  (:require [clojure.tools.logging :as log]
            [ivarref.yoltq.report-queue :as rq]
            [ivarref.yoltq.ext-sys :as ext]
            [ivarref.yoltq :as dq]
            [datomic.api :as d]
            [ivarref.yoltq.poller :as poller])
  (:import (java.util.concurrent BlockingQueue TimeUnit)))


(defn bootstrap-poller! [txq running? poller-exited? bootstrapped? conn]
  (let [ready? (promise)]
    (future
      (let [q (d/tx-report-queue conn)]
        (try
          (while @running?
            (when-let [poll-result (.poll ^BlockingQueue q 10 TimeUnit/MILLISECONDS)]
              (swap! txq conj poll-result))
            (deliver ready? true)
            (reset! bootstrapped? true))
          (catch Throwable t
            (log/error t "test-poller crashed: " (ex-message t)))
          (finally
            (try
              (d/remove-tx-report-queue conn)
              (catch Throwable t
                (log/warn t "remove-tx-report-queue failed:" (ex-message t))))
            (deliver poller-exited? true)))))
    @ready?))


(defmacro with-virtual-queue!
  [& body]
  `(let [txq# (atom [])
         poller-exited?# (promise)
         bootstrapped?# (atom false)
         running?# (atom true)
         config# (atom {:bootstrap-poller! (partial bootstrap-poller! txq# running?# poller-exited?# bootstrapped?#)
                        :init-backoff-time 0
                        :hung-log-level    :warn
                        :tx-queue          txq#})]
     (with-bindings {#'dq/*config*       config#
                     #'dq/*running?*     (atom false)
                     #'dq/*test-mode*    true
                     #'ext/*now-ns-atom* (atom 0)
                     #'ext/*random-atom* (atom 0)
                     #'ext/*squuid-atom* (atom 0)}
       (try
         ~@body
         (finally
           (reset! running?# false)
           (when @bootstrapped?#
             @poller-exited?#))))))


(defn call-with-virtual-queue!
  [f]
  (with-virtual-queue!
    (f)))


(defn run-report-queue! [min-items]
  (let [{:keys [tx-queue conn]} @dq/*config*
        id-ident (d/q '[:find ?e .
                        :where [?e :db/ident :ivarref.yoltq/id]]
                      (d/db conn))]
    (let [timeout (+ 3000 (System/currentTimeMillis))]
      (while (and (< (System/currentTimeMillis) timeout)
                  (< (count @tx-queue) min-items))
        (Thread/sleep 10)))
    (when (< (count @tx-queue) min-items)
      (let [msg (str "run-report-queue: timeout waiting for " min-items " items")]
        (log/error msg)
        (throw (ex-info msg {}))))
    (let [res (atom [])]
      (doseq [itm (first (swap-vals! tx-queue (constantly [])))]
        (rq/process-poll-result!
          @dq/*config*
          id-ident
          itm
          (fn [f] (swap! res conj (f)))))
      @res)))


(defn run-one-report-queue! []
  (first (run-report-queue! 1)))


(defn run-queue-once! [q status]
  (poller/poll-once! @dq/*config* q status))


(defn put! [q payload]
  @(d/transact (:conn @dq/*config*) [(dq/put q payload)]))