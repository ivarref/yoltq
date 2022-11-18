(ns com.github.ivarref.yoltq.utils
  (:require [datomic.api :as d]
            [clojure.edn :as edn]
            [com.github.ivarref.yoltq.ext-sys :as ext]
            [clojure.tools.logging :as log])
  (:refer-clojure :exclude [random-uuid])
  (:import (datomic Connection)
           (java.time Duration)))


(def status-init :init)
(def status-processing :processing)
(def status-done :done)
(def status-error :error)

(def current-version "2")

(defn duration->millis [m]
  (reduce-kv (fn [o k v]
               (if (instance? Duration v)
                 (assoc o k (.toMillis v))
                 (assoc o k v)))
             {}
             m))


(defn squuid []
  (ext/squuid))


(defn random-uuid []
  (ext/random-uuid))


(defn now-ms []
  (ext/now-ms))


(defn root-cause [e]
  (if-let [root (ex-cause e)]
    (root-cause root)
    e))


(defn db-error-map [^Throwable t]
  (loop [e t]
    (cond (nil? e) nil

          (and (map? (ex-data e))
               (contains? (ex-data e) :db/error))
          (ex-data e)

          :else
          (recur (ex-cause e)))))


(defn get-queue-item [db id]
  (-> (d/pull db '[:*] [:com.github.ivarref.yoltq/id id])
      (dissoc :db/id)
      (update :com.github.ivarref.yoltq/opts (fn [s] (or (when s (edn/read-string s)) {})))
      (update :com.github.ivarref.yoltq/bindings
              (fn [s]
                (when s
                  (->> s
                       (edn/read-string)
                       (reduce-kv (fn [o k v]
                                    (assoc o (resolve k) v))
                                  {})))))))


(defn prepare-processing [db id queue-name old-lock old-status]
  (let [new-lock (random-uuid)]
    {:id         id
     :lock       new-lock
     :queue-name queue-name
     :bindings   (get (get-queue-item db id) :com.github.ivarref.yoltq/bindings {})
     :tx         [[:db/cas [:com.github.ivarref.yoltq/id id] :com.github.ivarref.yoltq/lock old-lock new-lock]
                  [:db/cas [:com.github.ivarref.yoltq/id id] :com.github.ivarref.yoltq/status old-status status-processing]
                  {:db/id [:com.github.ivarref.yoltq/id id] :com.github.ivarref.yoltq/processing-time (now-ms)}]}))


(defn get-init [{:keys [conn db init-backoff-time] :as cfg} queue-name]
  (assert (instance? Connection conn) (str "Expected conn to be of type datomic.Connection. Was: "
                                           (str (if (nil? conn) "nil" conn))
                                           "\nConfig was: " (str cfg)))
  (let [db (or db (d/db conn))]
    (if-let [ids (->> (d/q '[:find ?id ?lock
                             :in $ ?queue-name ?backoff ?current-version
                             :where
                             [?e :com.github.ivarref.yoltq/status :init]
                             [?e :com.github.ivarref.yoltq/queue-name ?queue-name]
                             [?e :com.github.ivarref.yoltq/init-time ?init-time]
                             [(>= ?backoff ?init-time)]
                             [?e :com.github.ivarref.yoltq/id ?id]
                             [?e :com.github.ivarref.yoltq/lock ?lock]
                             [?e :com.github.ivarref.yoltq/version ?current-version]]
                           db
                           queue-name
                           (- (now-ms) init-backoff-time)
                           current-version)
                      (not-empty))]
      (let [[id old-lock] (rand-nth (into [] ids))]
        (prepare-processing db id queue-name old-lock :init))
      (log/debug "no new-items in :init status for queue" queue-name))))


(defn get-error [{:keys [conn db error-backoff-time max-retries] :as cfg} queue-name]
  (assert (instance? Connection conn) (str "Expected conn to be of type datomic.Connection. Was: "
                                           (str (if (nil? conn) "nil" conn))
                                           "\nConfig was: " (str cfg)))
  (let [db (or db (d/db conn))
        max-retries (get-in cfg [:handlers queue-name :max-retries] max-retries)]
    (when-let [ids (->> (d/q '[:find ?id ?lock
                               :in $ ?queue-name ?backoff ?max-tries ?current-version
                               :where
                               [?e :com.github.ivarref.yoltq/status :error]
                               [?e :com.github.ivarref.yoltq/queue-name ?queue-name]
                               [?e :com.github.ivarref.yoltq/error-time ?time]
                               [(>= ?backoff ?time)]
                               [?e :com.github.ivarref.yoltq/tries ?tries]
                               [(> ?max-tries ?tries)]
                               [?e :com.github.ivarref.yoltq/id ?id]
                               [?e :com.github.ivarref.yoltq/lock ?lock]
                               [?e :com.github.ivarref.yoltq/version ?current-version]]
                             db
                             queue-name
                             (- (now-ms) error-backoff-time)
                             (inc max-retries)
                             current-version)
                        (not-empty))]
      (let [[id old-lock] (rand-nth (into [] ids))]
        (prepare-processing db id queue-name old-lock :error)))))


(defn get-hung [{:keys [conn db now hung-backoff-time max-retries] :as cfg} queue-name]
  (assert (instance? Connection conn) (str "Expected conn to be of type datomic.Connection. Was: "
                                           (str (if (nil? conn) "nil" conn))
                                           "\nConfig was: " (str cfg)))
  (let [now (or now (now-ms))
        max-retries (get-in cfg [:handlers queue-name :max-retries] max-retries)
        db (or db (d/db conn))]
    (when-let [ids (->> (d/q '[:find ?id ?lock ?tries
                               :in $ ?qname ?backoff ?current-version
                               :where
                               [?e :com.github.ivarref.yoltq/status :processing]
                               [?e :com.github.ivarref.yoltq/queue-name ?qname]
                               [?e :com.github.ivarref.yoltq/processing-time ?time]
                               [(>= ?backoff ?time)]
                               [?e :com.github.ivarref.yoltq/tries ?tries]
                               [?e :com.github.ivarref.yoltq/id ?id]
                               [?e :com.github.ivarref.yoltq/lock ?lock]
                               [?e :com.github.ivarref.yoltq/version ?current-version]]
                             db
                             queue-name
                             (- now hung-backoff-time)
                             current-version)
                        (not-empty))]
      (let [new-lock (random-uuid)
            [id old-lock tries _t] (rand-nth (into [] ids))
            to-error? (>= tries max-retries)]
        {:id         id
         :lock       new-lock
         :queue-name queue-name
         :was-hung?  true
         :to-error?  to-error?
         :bindings   (get (get-queue-item db id) :com.github.ivarref.yoltq/bindings {})
         :tx         (if (not to-error?)
                       [[:db/cas [:com.github.ivarref.yoltq/id id] :com.github.ivarref.yoltq/lock old-lock new-lock]
                        [:db/cas [:com.github.ivarref.yoltq/id id] :com.github.ivarref.yoltq/tries tries (inc tries)]
                        {:db/id [:com.github.ivarref.yoltq/id id] :com.github.ivarref.yoltq/error-time now}]
                       [[:db/cas [:com.github.ivarref.yoltq/id id] :com.github.ivarref.yoltq/lock old-lock new-lock]
                        [:db/cas [:com.github.ivarref.yoltq/id id] :com.github.ivarref.yoltq/tries tries (inc tries)]
                        [:db/cas [:com.github.ivarref.yoltq/id id] :com.github.ivarref.yoltq/status status-processing status-error]
                        {:db/id [:com.github.ivarref.yoltq/id id] :com.github.ivarref.yoltq/error-time now}])}))))
