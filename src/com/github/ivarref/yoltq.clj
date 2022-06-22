(ns com.github.ivarref.yoltq
  (:require [clojure.tools.logging :as log]
            [com.github.ivarref.yoltq.error-poller :as errpoller]
            [com.github.ivarref.yoltq.impl :as i]
            [com.github.ivarref.yoltq.migrate :as migrate]
            [com.github.ivarref.yoltq.poller :as poller]
            [com.github.ivarref.yoltq.report-queue :as rq]
            [com.github.ivarref.yoltq.slow-executor-detector :as slow-executor]
            [com.github.ivarref.yoltq.utils :as u]
            [datomic.api :as d])
  (:import (datomic Connection)
           (java.time Duration)
           (java.util.concurrent ExecutorService Executors TimeUnit)))


(defonce ^:dynamic *config* (atom nil))
(defonce threadpool (atom nil))
(defonce ^:dynamic *running?* (atom false))
(defonce ^:dynamic *test-mode* false)

(def default-opts
  (-> {; Default number of times a queue job will be retried before giving up
       ; Can be overridden on a per-consumer basis with
       ; (yq/add-consumer! :q (fn [payload] ...) {:max-retries 200})
       :max-retries                   100

       ; Minimum amount of time to wait before a failed queue job is retried
       :error-backoff-time            (Duration/ofSeconds 5)

       ; Max time a queue job can execute before an error is logged
       :max-execute-time              (Duration/ofMinutes 5)

       ; Amount of time an in progress queue job can run before it is considered failed
       ; and will be marked as such.
       :hung-backoff-time             (Duration/ofMinutes 30)

       ; Most queue jobs in init state will be consumed by the tx-report-queue listener.
       ; However, in the case where an init job was added right before the application
       ; was shut down and did not have time to be processed by the tx-report-queue listener,
       ; it will be consumer by the init poller. This init poller backs off by
       ; :init-backoff-time to avoid unnecessary compare-and-swap lock failures that could
       ; otherwise occur if competing with the tx-report-queue listener.
       :init-backoff-time             (Duration/ofSeconds 60)

       ; How frequent polling for init, error and hung jobs should be done.
       :poll-delay                    (Duration/ofSeconds 10)

       ; Specifies the number of threads available for executing queue and polling jobs.
       ; The final thread pool will be this size + 2.
       ;
       ; One thread is permanently allocated for listening to the
       ; tx-report-queue.
       ;
       ; Another thread is permanently allocated for checking :max-execute-time.
       ; This means that if all executing queue jobs are stuck and the thread pool is unavailable
       ; as such, at least an error will be logged about this. The log entry will
       ; contain the stacktrace of the stuck threads.
       :pool-size                     4

       :capture-bindings              (if-let [s (resolve (symbol "taoensso.timbre/*context*"))]
                                        [s]
                                        [])

       ; How often should the system be polled for failed queue jobs
       :system-error-poll-delay       (Duration/ofMinutes 1)

       ; How often should the system invoke
       :system-error-callback-backoff (Duration/ofHours 1)

       ; Should old, possibly stalled jobs be automatically be migrated
       ; as part of `start!`?
       :auto-migrate? true}

      u/duration->millis))


(defn init! [{:keys [conn] :as cfg}]
  (assert (instance? Connection conn) (str "Expected :conn to be of type datomic Connection. Was: " (or (some-> conn class str) "nil")))
  (locking threadpool
    @(d/transact conn i/schema)
    (let [new-cfg (swap! *config*
                         (fn [old-conf]
                           (-> (merge-with (fn [_ b] b)
                                           {:running-queues     (atom #{})
                                            :start-execute-time (atom {})
                                            :system-error       (atom {})
                                            :healthy?           (atom nil)}
                                           default-opts
                                           (if *test-mode* old-conf (select-keys old-conf [:handlers]))
                                           cfg)
                               u/duration->millis)))]
      new-cfg)))


(defn get-queue-id
  [queue-id-or-var]
  (cond (and (var? queue-id-or-var)
             (keyword? (:yoltq/queue-id (meta queue-id-or-var))))
        (:yoltq/queue-id (meta queue-id-or-var))

        (keyword? queue-id-or-var)
        queue-id-or-var

        :else
        (throw (ex-info (str "Could not get queue-id for " queue-id-or-var) {:queue-id queue-id-or-var}))))

(defn add-consumer!
  ([queue-id f]
   (add-consumer! queue-id f {}))
  ([queue-id f opts]
   (swap! *config* (fn [old-config] (assoc-in old-config [:handlers (get-queue-id queue-id)] (merge opts {:f f}))))))


(defn put
  ([queue-id payload] (put queue-id payload {}))
  ([queue-id payload opts]
   (let [{:keys [bootstrap-poller! conn] :as cfg} @*config*]
     (when (and *test-mode* bootstrap-poller!)
       (bootstrap-poller! conn))
     (i/put cfg (get-queue-id queue-id) payload opts))))


(defn- do-start! []
  (let [{:keys [poll-delay pool-size system-error-poll-delay auto-migrate?] :as cfg} @*config*]
    (when auto-migrate?
      (future (migrate/migrate! cfg)))
    (reset! threadpool (Executors/newScheduledThreadPool (+ 2 pool-size)))
    (let [pool @threadpool
          queue-listener-ready (promise)]
      (reset! *running?* true)
      (.scheduleAtFixedRate pool (fn [] (poller/poll-all-queues! *running?* *config* pool)) 0 poll-delay TimeUnit/MILLISECONDS)
      (.scheduleAtFixedRate pool (fn [] (errpoller/poll-errors *running?* *config*)) 0 system-error-poll-delay TimeUnit/MILLISECONDS)
      (.execute pool (fn [] (rq/report-queue-listener *running?* queue-listener-ready pool *config*)))
      (.execute pool (fn [] (slow-executor/show-slow-threads *running?* *config*)))
      @queue-listener-ready)))


(defn start! []
  (locking threadpool
    (cond (true? *test-mode*)
          (log/info "test mode enabled, doing nothing for start!")

          (true? @*running?*)
          nil

          (false? @*running?*)
          (do-start!))))


(defn stop! []
  (locking threadpool
    (cond (true? *test-mode*)
          (log/info "test mode enabled, doing nothing for stop!")

          (false? @*running?*)
          nil

          (true? @*running?*)
          (do
            (reset! *running?* false)
            (when-let [^ExecutorService tp @threadpool]
              (log/debug "shutting down old threadpool")
              (.shutdown tp)
              (while (not (.awaitTermination tp 1 TimeUnit/SECONDS))
                (log/debug "waiting for threadpool to stop"))
              (log/debug "stopped!")
              (reset! threadpool nil))))))


(defn healthy? []
  (some->> @*config*
           :healthy?
           (deref)))

(defn unhealthy?
  "Returns `true` if there are queues in error, otherwise `false`."
  []
  (false? (healthy?)))

(defn queue-stats []
  (let [{:keys [conn]} @*config*
        db (d/db conn)]
    (->> (d/q '[:find ?e ?qname ?status
                :in $
                :where
                [?e :com.github.ivarref.yoltq/queue-name ?qname]
                [?e :com.github.ivarref.yoltq/status ?status]]
              db)
         (mapv (partial zipmap [:e :qname :status]))
         (mapv #(select-keys % [:qname :status]))
         (mapv (fn [qitem] {qitem 1}))
         (reduce (partial merge-with +) {})
         (mapv (fn [[{:keys [qname status]} v]]
                 (array-map
                   :qname qname
                   :status status
                   :count v)))
         (sort-by (juxt :qname :status))
         (vec))))

(comment
  (do
    (require 'com.github.ivarref.yoltq.log-init)
    (com.github.ivarref.yoltq.log-init/init-logging!
      [[#{"datomic.*" "com.datomic.*" "org.apache.*"} :warn]
       [#{"ivarref.yoltq.report-queue"} :info]
       [#{"ivarref.yoltq.poller"} :info]
       [#{"ivarref.yoltq*"} :info]
       [#{"*"} :info]])
    (stop!)
    (let [received (atom [])
          uri (str "datomic:mem://demo")]
      (d/delete-database uri)
      (d/create-database uri)
      (let [ok-items (atom [])
            conn (d/connect uri)
            n 1]
        (init! {:conn               conn
                :error-backoff-time (Duration/ofSeconds 1)
                :poll-delay         (Duration/ofSeconds 1)})
        (add-consumer! :q (fn [payload]
                            #_(when (> (Math/random) 0.5)
                                (throw (ex-info "oops" {})))
                            (if (= n (count (swap! received conj (:work payload))))
                              (log/info "... and we are done!")
                              (log/info "got payload" payload "total ok:" (count @received)))))
        (start!)
        (dotimes [x n]
          @(d/transact conn [(put :q {:work 123})]))
        nil))))
