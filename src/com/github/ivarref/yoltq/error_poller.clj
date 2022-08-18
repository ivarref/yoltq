(ns com.github.ivarref.yoltq.error-poller
  (:require [clojure.tools.logging :as log]
            [com.github.ivarref.yoltq.ext-sys :as ext]
            [com.github.ivarref.yoltq.utils :as u]
            [datomic.api :as d]))


(defn get-state [v]
  (case v
    [:error :none] :recovery
    [:error :some] :error
    [:error :all] :error
    [:recovery :none] :recovery
    [:recovery :some] :recovery
    [:recovery :all] :error
    nil))


(defn handle-error-count [{:keys [errors last-notify state]
                           :or   {errors      []
                                  last-notify 0
                                  state       :recovery}}
                          {:keys [system-error-min-count system-error-callback-backoff]
                           :or   {system-error-min-count 3}}
                          now-ms
                          error-count]
  (let [new-errors (->> (conj errors error-count)
                        (take-last system-error-min-count)
                        (vec))
        classify (fn [coll]
                   (cond
                     (not= system-error-min-count (count coll))
                     :missing

                     (every? pos-int? coll)
                     :all

                     (every? zero? coll)
                     :none

                     :else
                     :some))
        old-state state]
    (merge
      {:errors      new-errors
       :last-notify last-notify}
      (when-let [new-state (get-state [old-state (classify new-errors)])]
        (merge
          {:state new-state}
          (when (and (= old-state :recovery)
                     (= new-state :error))
            {:run-callback :error
             :last-notify  now-ms})

          (when (and (= new-state :error)
                     (= old-state :error)
                     (> now-ms
                        (+ last-notify system-error-callback-backoff)))
            {:run-callback :error
             :last-notify  now-ms})

          (when (and (= new-state :recovery)
                     (= old-state :error))
            {:run-callback :recovery}))))))


(defn do-poll-errors [{:keys [conn
                              system-error
                              on-system-error
                              on-system-recovery
                              healthy?
                              healthy-allowed-error-time]
                       :or   {on-system-error    (fn []
                                                   (log/error "There are yoltq queues which have errors")
                                                   nil)
                              on-system-recovery (fn []
                                                   (log/info "Yoltq recovered"))}
                       :as   config}
                      now-ms]
  (assert (some? conn) "expected :conn to be present")
  (assert (some? system-error) "expected :system-error to be present")
  (assert (nat-int? healthy-allowed-error-time) "expected :healthy-allowed-error-time to be present")
  (let [max-init-time (- now-ms healthy-allowed-error-time)
        error-count (or (d/q '[:find (count ?e) .
                               :in $ ?status ?max-init-time
                               :where
                               [?e :com.github.ivarref.yoltq/status ?status]
                               [?e :com.github.ivarref.yoltq/init-time ?init-time]
                               [(<= ?init-time ?max-init-time)]]
                             (d/db conn)
                             u/status-error
                             max-init-time)
                        0)]
    (if (pos-int? error-count)
      (do
        (log/debug "poll-errors found" error-count "errors in system")
        (reset! healthy? false))
      (reset! healthy? true))
    (let [{:keys [run-callback] :as new-state} (swap! system-error handle-error-count config now-ms error-count)]
      (when run-callback
        (cond (= run-callback :error)
              (on-system-error)

              (= run-callback :recovery)
              (on-system-recovery)

              :else
              (log/error "unhandled callback-type" run-callback))
        (log/debug "run-callback is" run-callback))
      error-count)))


(defn poll-errors [running? config-atom]
  (try
    (when @running?
      (do-poll-errors @config-atom (ext/now-ms)))
    (catch Throwable t
      (log/error t "unexpected error in poll-errors:" (ex-message t))
      nil)))


(comment
  (do-poll-errors @com.github.ivarref.yoltq/*config* (ext/now-ms)))

