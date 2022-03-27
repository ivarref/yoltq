(ns com.github.ivarref.yoltq.error-poller
  (:require [datomic.api :as d]
            [com.github.ivarref.yoltq.utils :as u]
            [com.github.ivarref.yoltq.ext-sys :as ext]
            [clojure.tools.logging :as log]))


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
                          now-ns
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
             :last-notify  now-ns})

          (when (and (= new-state :error)
                     (= old-state :error)
                     (> now-ns
                        (+ last-notify system-error-callback-backoff)))
            {:run-callback :error
             :last-notify  now-ns})

          (when (and (= new-state :recovery)
                     (= old-state :error))
            {:run-callback :recovery}))))))


(defn do-poll-errors [{:keys [conn system-error
                              on-system-error
                              on-system-recovery
                              healthy?]
                       :or   {on-system-error    (fn []
                                                   (log/error "There are yoltq queues which have errors")
                                                   nil)
                              on-system-recovery (fn []
                                                   (log/info "Yoltq recovered"))}
                       :as   config}]
  (assert (some? conn) "expected :conn to be present")
  (assert (some? system-error) "expected :system-error to be present")
  (let [error-count (or (d/q '[:find (count ?e) .
                               :in $ ?status
                               :where
                               [?e :com.github.ivarref.yoltq/status ?status]]
                             (d/db conn)
                             u/status-error)
                        0)]
    (if (pos-int? error-count)
      (do
        (log/debug "poll-errors found" error-count "errors in system")
        (reset! healthy? false))
      (reset! healthy? true))
    (let [{:keys [run-callback] :as new-state} (swap! system-error handle-error-count config (ext/now-ns) error-count)]
      (when run-callback
        (cond (= run-callback :error)
              (on-system-error)

              (= run-callback :recovery)
              (on-system-recovery)

              :else
              (log/error "unhandled callback-type" run-callback))
        (log/debug "run-callback is" run-callback))
      new-state)))


(defn poll-errors [running? config-atom]
  (try
    (when @running?
      (do-poll-errors @config-atom))
    (catch Throwable t
      (log/error t "unexpected error in poll-errors:" (ex-message t))
      nil)))


(comment
  (do-poll-errors @com.github.ivarref.yoltq/*config*))

