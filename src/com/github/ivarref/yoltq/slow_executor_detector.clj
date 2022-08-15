(ns com.github.ivarref.yoltq.slow-executor-detector
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]
            [com.github.ivarref.yoltq.ext-sys :as ext])
  (:import (java.util.concurrent ExecutorService)))

(defn- do-show-slow-threads [{:keys [start-execute-time
                                     max-execute-time
                                     slow?
                                     slow-thread-show-stacktrace?]
                              :or {slow-thread-show-stacktrace? true}}]
  (let [new-slow-val (atom false)]
    (doseq [[^Thread thread [start-time queue-id queue-name]] @start-execute-time]
      (when (> (ext/now-ms) (+ start-time max-execute-time))
        (reset! new-slow-val true)
        (log/error "thread" (.getName thread) "spent too much time on"
                   "queue item" (str queue-id)
                   "for queue" queue-name
                   (if slow-thread-show-stacktrace?
                       (str "stacktrace: \n" (str/join "\n" (mapv str (seq (.getStackTrace thread)))))
                       ""))))
    (reset! slow? @new-slow-val)))

(defn show-slow-threads [^ExecutorService pool config-atom]
  (try
    (while (not (.isTerminated pool))
      (try
        (do-show-slow-threads @config-atom)
        (catch Throwable t
          (log/error t "do-show-slow-threads crashed:" (ex-message t))))
      (dotimes [_ 3]
        (when (not (.isTerminated pool))
          (Thread/sleep 1000))))
    (log/debug "show-slow-threads exiting")
    (catch Throwable t
      (log/error t "reap! crashed:" (ex-message t)))))
