(ns com.github.ivarref.yoltq.poller
  (:require [com.github.ivarref.yoltq.utils :as u]
            [com.github.ivarref.yoltq.impl :as i]
            [clojure.tools.logging :as log]))


(defn poll-once! [cfg q status]
  (when-let [item (case status
                    :init (u/get-init cfg q)
                    :error (u/get-error cfg q)
                    :hung (u/get-hung cfg q))]
    (with-bindings (get item :bindings {})
      (if (i/depends-on-waiting? cfg item)
        nil
        (some->> item
                 (i/take! cfg)
                 (i/execute! cfg))))))


(defn poll-queue! [running?
                   {:keys [running-queues] :as cfg}
                   [queue-name status :as q]]
  (try
    (let [[old _] (swap-vals! running-queues conj q)]
      (if-not (contains? old q)
        (try
          (log/debug "polling queue" queue-name "for status" status)
          (let [start-time (u/now-ns)
                last-res (loop [prev-res nil]
                           (when @running?
                             (let [res (poll-once! cfg queue-name status)]
                               (if (and res (:success? res))
                                 (recur res)
                                 prev-res))))]
            (let [spent-ns (- (u/now-ns) start-time)]
              (log/trace "done polling queue" q "in"
                         (format "%.1f" (double (/ spent-ns 1e6)))
                         "ms"))
            last-res)
          (finally
            (swap! running-queues disj q)))
        (log/debug "queue" q "is already being polled, doing nothing...")))
    (catch Throwable t
      (log/error t "poll-queue! crashed:" (ex-message t)))
    (finally)))


(defn poll-all-queues! [running? config-atom pool]
  (try
    (when @running?
      (let [{:keys [handlers]} @config-atom]
        (doseq [q (shuffle (vec (for [q-name (keys handlers)
                                      status [:init :error :hung]]
                                  [q-name status])))]
          (.execute pool (fn [] (poll-queue! running? @config-atom q))))))
    (catch Throwable t
      (log/error t "poll-all-queues! crashed:" (ex-message t)))))