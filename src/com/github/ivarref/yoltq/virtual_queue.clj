(ns com.github.ivarref.yoltq.virtual-queue
  (:require [clojure.tools.logging :as log]
            [com.github.ivarref.yoltq.report-queue :as rq]
            [com.github.ivarref.yoltq.ext-sys :as ext]
            [com.github.ivarref.yoltq :as yq]
            [datomic.api :as d]
            [com.github.ivarref.yoltq.poller :as poller]
            [clojure.test :as test]
            [com.github.ivarref.yoltq.utils :as u]
            [com.github.ivarref.yoltq.impl :as i])
  (:import (java.util.concurrent BlockingQueue TimeUnit)
           (datomic Datom)))


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
     (with-bindings {#'yq/*config*       config#
                     #'yq/*running?*     (atom false)
                     #'yq/*test-mode*    true
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
  (let [{:keys [tx-queue conn]} @yq/*config*
        id-ident (d/q '[:find ?e .
                        :where [?e :db/ident :com.github.ivarref.yoltq/id]]
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
          @yq/*config*
          id-ident
          itm
          (fn [f] (swap! res conj (f)))))
      @res)))


(defn run-one-report-queue! []
  (first (run-report-queue! 1)))


(defn run-queue-once! [q status]
  (poller/poll-once! @yq/*config* q status))


(defn put! [q payload]
  @(d/transact (:conn @yq/*config*) [(yq/put q payload)]))


(defn transact-result->maps [{:keys [tx-data db-after]}]
  (let [m (->> tx-data
               (group-by (fn [^Datom d] (.e d)))
               (vals)
               (mapv (fn [datoms]
                       (reduce (fn [o ^Datom d]
                                 (if (.added d)
                                   (assoc o (d/q '[:find ?r .
                                                   :in $ ?e
                                                   :where [?e :db/ident ?r]]
                                                 db-after
                                                 (.a d))
                                            (.v d))
                                   o))
                               {}
                               datoms))))]
    m))

(defn contains-queue-job?
  [queue-id conn {::yq/keys [id queue-name status] :as m}]
  (when (and (= queue-id queue-name)
             (= status :init)
             (d/q '[:find ?e .
                    :in $ ?id
                    :where
                    [?e ::yq/id ?id]
                    [?e ::yq/status :init]]
                  (d/db conn)
                  id))
    m))


(defn get-tx-q-job [q-id]
  (let [{:keys [tx-queue conn]} @yq/*config*]
    (loop [timeout (+ 3000 (System/currentTimeMillis))]
      (if-let [job (->> @tx-queue
                        (mapcat transact-result->maps)
                        (filter (partial contains-queue-job? q-id conn))
                        (first))]
        (u/get-queue-item (d/db conn) (::yq/id job))
        (if (< (System/currentTimeMillis) timeout)
          (do (Thread/sleep 10)
              (recur timeout))
          nil)))))

(defmacro consume-expect! [queue-name expected-status]
  `(if-let [job# (get-tx-q-job ~queue-name)]
     (try
       (let [res# (some->>
                    (u/prepare-processing (:com.github.ivarref.yoltq/id job#)
                                          ~queue-name
                                          (:com.github.ivarref.yoltq/lock job#)
                                          (:com.github.ivarref.yoltq/status job#))
                    (i/take! @yq/*config*)
                    (i/execute! @yq/*config*))]
         (test/is (= ~expected-status (:com.github.ivarref.yoltq/status res#)))
         (if (:retval res#)
           (:retval res#)
           (:exception res#)))
       (catch Throwable t#
         (log/error t# "unexpected error in consume-expect:" (ex-message t#))))
     (test/is nil (str "No job found for queue " ~queue-name))))