(ns com.github.ivarref.yoltq.test-utils
  (:require [com.github.ivarref.yoltq.log-init :as logconfig]
            [clojure.tools.logging :as log]
            [com.github.ivarref.yoltq.utils :as u]
            [com.github.ivarref.yoltq :as yq]
            [datomic.api :as d]
            [clojure.string :as str]
            [com.github.ivarref.yoltq.impl :as i]
            [clojure.edn :as edn]
            [com.github.ivarref.yoltq.ext-sys :as ext]
            [clojure.pprint :as pp])
  (:import (java.util UUID)
           (java.time Duration)))


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
  @(d/transact (:conn @yq/*config*) [(i/put @yq/*config* id payload {})]))


(defn advance! [tp]
  (assert (some? ext/*now-ms-atom*) "Expected to be running in test-mode!")
  (swap! ext/*now-ms-atom* + (if (number? tp)
                               tp
                               (.toMillis ^Duration tp))))


(defn done-count []
  (d/q '[:find (count ?e) .
         :where
         [?e :com.github.ivarref.yoltq/id _]
         [?e :com.github.ivarref.yoltq/status :done]]
       (d/db (:conn @yq/*config*))))


(defn pp [x]
  (pp/pprint x)
  x)

(defn get-init [& args]
  (apply u/get-init @yq/*config* args))


(defn get-error [& args]
  (apply u/get-error @yq/*config* args))


(defn get-hung [& args]
  (apply u/get-hung @yq/*config* args))


(defn take! [& args]
  (apply i/take! @yq/*config* args))


(defn execute! [& args]
  (apply i/execute! @yq/*config* args))

