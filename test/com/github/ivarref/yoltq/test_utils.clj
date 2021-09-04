(ns com.github.ivarref.yoltq.test-utils
  (:require [com.github.ivarref.yoltq.log-init :as logconfig]
            [clojure.tools.logging :as log]
            [com.github.ivarref.yoltq.utils :as u]
            [com.github.ivarref.yoltq :as dq]
            [datomic.api :as d]
            [clojure.string :as str]
            [com.github.ivarref.yoltq.impl :as i]
            [clojure.edn :as edn]
            [com.github.ivarref.yoltq.ext-sys :as ext])
  (:import (java.util UUID)))


(logconfig/init-logging!
  [[#{"datomic.*" "com.datomic.*" "org.apache.*"} :warn]
   [#{"*"} (edn/read-string
             (System/getProperty "TAOENSSO_TIMBRE_MIN_LEVEL_EDN" ":info"))]])


(defn empty-conn []
  (let [uri (str "datomic:mem://hello-world-" (UUID/randomUUID))]
    (d/delete-database uri)
    (d/create-database uri)
    (d/connect uri)))


(defn break []
  (log/info (str/join "*" (repeat 60 ""))))


(defn clear []
  (.print System/out "\033[H\033[2J")
  (.flush System/out)
  (break))


(defn put-transact! [id payload]
  @(d/transact (:conn @dq/*config*) [(i/put @dq/*config* id payload)]))


(defn advance! [tp]
  (assert (some? ext/*now-ns-atom*) "Expected to be running in test-mode!")
  (swap! ext/*now-ns-atom* + (if (number? tp)
                               tp
                               (.toNanos tp))))


(defn done-count []
  (d/q '[:find (count ?e) .
         :where
         [?e :com.github.ivarref.yoltq/id _]
         [?e :com.github.ivarref.yoltq/status :done]]
       (d/db (:conn @dq/*config*))))


(defn get-init [& args]
  (apply u/get-init @dq/*config* args))


(defn get-error [& args]
  (apply u/get-error @dq/*config* args))


(defn get-hung [& args]
  (apply u/get-hung @dq/*config* args))


(defn take! [& args]
  (apply i/take! @dq/*config* args))


(defn execute! [& args]
  (apply i/execute! @dq/*config* args))

