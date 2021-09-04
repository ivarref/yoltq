(ns com.github.ivarref.yoltq.log-init
  (:require [clojure.term.colors :as colors]
            [taoensso.timbre :as timbre]
            [clojure.string :as str]))

(def level-colors
  {;:warn colors/red
   :error colors/red})

(def ^:dynamic *override-color* nil)

(defn min-length [n s]
  (loop [s s]
    (if (>= (count s) n)
      s
      (recur (str s " ")))))

(defn local-console-format-fn
  "A simpler log format, suitable for readable logs during development. colorized stacktraces"
  [data]
  (try
    (let [{:keys [level ?err msg_ ?ns-str ?file hostname_
                  timestamp_ ?line context]} data
          ts (force timestamp_)]
      (let [color-f (if (nil? *override-color*)
                      (get level-colors level str)
                      colors/green)
            maybe-stacktrace (when ?err
                               (str "\n" (timbre/stacktrace ?err)))]
        (cond-> (str #_(str ?ns-str ":" ?line)
                  #_(min-length (count "[yoltq:326] ")
                                (str
                                  "["
                                  (some-> ?ns-str
                                          (str/split #"\.")
                                          (last))
                                  ":" ?line))
                  ts
                  " "
                  (color-f (min-length 5 (str/upper-case (name level))))
                  " "
                  #_(.getName ^Thread (Thread/currentThread))

                  (color-f (force msg_))

                  #_maybe-stacktrace))))


    (catch Throwable t
      (println "error in local-console-format-fn:" (ex-message t))
      nil)))


(defn init-logging! [min-levels]
  (timbre/merge-config!
    {:min-level min-levels
     :timestamp-opts {:pattern "HH:mm:ss.SSS"
                      :timezone :jvm-default}
     :output-fn (fn [data] (local-console-format-fn data))
     :appenders {:println (timbre/println-appender {:stream :std-out})}}))

