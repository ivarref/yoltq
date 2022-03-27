(ns com.github.ivarref.yoltq.slow-executor-detector
  (:require [com.github.ivarref.yoltq.ext-sys :as ext]
            [clojure.tools.logging :as log]
            [clojure.string :as str]))


(defn- do-show-slow-threads [{:keys [start-execute-time
                                     max-execute-time]}]
  (doseq [[^Thread thread [start-time queue-id queue-name]] @start-execute-time]
    (when (> (ext/now-ms) (+ start-time max-execute-time))
      (log/error "thread" (.getName thread) "spent too much time on"
                 "queue item" (str queue-id)
                 "for queue" queue-name
                 "stacktrace: \n"
                 (str/join "\n" (mapv str (seq (.getStackTrace thread))))))))


(defn show-slow-threads [running? config-atom]
  (try
    (while @running?
      (try
        (do-show-slow-threads @config-atom)
        (catch Throwable t
          (log/error t "do-show-slow-threads crashed:" (ex-message t))))
      (dotimes [_ 3]
        (when @running? (Thread/sleep 1000))))
    (catch Throwable t
      (log/error t "reap! crashed:" (ex-message t)))))
