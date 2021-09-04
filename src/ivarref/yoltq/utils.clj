(ns ivarref.yoltq.utils
  (:require [datomic.api :as d]
            [clojure.edn :as edn]
            [ivarref.yoltq.ext-sys :as ext]
            [clojure.tools.logging :as log])
  (:import (datomic Connection)
           (java.time Duration)))


(def status-init :init)
(def status-processing :processing)
(def status-done :done)
(def status-error :error)


(defn duration->nanos [m]
  (reduce-kv (fn [o k v]
               (if (instance? Duration v)
                 (assoc o k (.toNanos v))
                 (assoc o k v)))
             {}
             m))


(defn squuid []
  (ext/squuid))


(defn random-uuid []
  (ext/random-uuid))


(defn now-ns []
  (ext/now-ns))


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
  (-> (d/pull db '[:*] [:ivarref.yoltq/id id])
      (dissoc :db/id)
      (update :ivarref.yoltq/payload edn/read-string)
      (update :ivarref.yoltq/bindings edn/read-string)))


(defn prepare-processing [id queue-name old-lock old-status]
  (let [new-lock (random-uuid)]
    {:id         id
     :lock       new-lock
     :queue-name queue-name
     :tx         [[:db/cas [:ivarref.yoltq/id id] :ivarref.yoltq/lock old-lock new-lock]
                  [:db/cas [:ivarref.yoltq/id id] :ivarref.yoltq/status old-status status-processing]
                  {:db/id [:ivarref.yoltq/id id] :ivarref.yoltq/processing-time (now-ns)}]}))


(defn get-init [{:keys [conn db init-backoff-time] :as cfg} queue-name]
  (assert (instance? Connection conn) (str "Expected conn to be of type datomic.Connection. Was: "
                                           (str (if (nil? conn) "nil" conn))
                                           "\nConfig was: " (str cfg)))
  (if-let [ids (->> (d/q '[:find ?id ?lock
                           :in $ ?queue-name ?backoff
                           :where
                           [?e :ivarref.yoltq/status :init]
                           [?e :ivarref.yoltq/queue-name ?queue-name]
                           [?e :ivarref.yoltq/init-time ?init-time]
                           [(>= ?backoff ?init-time)]
                           [?e :ivarref.yoltq/id ?id]
                           [?e :ivarref.yoltq/lock ?lock]]
                         (or db (d/db conn))
                         queue-name
                         (- (now-ns) init-backoff-time))
                    (not-empty))]
    (let [[id old-lock] (rand-nth (into [] ids))]
      (prepare-processing id queue-name old-lock :init))
    (log/trace "no new-items in :init status for queue" queue-name)))


(defn get-error [{:keys [conn db error-backoff-time max-retries] :as cfg} queue-name]
  (assert (instance? Connection conn) (str "Expected conn to be of type datomic.Connection. Was: "
                                           (str (if (nil? conn) "nil" conn))
                                           "\nConfig was: " (str cfg)))
  (let [max-retries (get-in cfg [:handlers queue-name :max-retries] max-retries)]
    (when-let [ids (->> (d/q '[:find ?id ?lock
                               :in $ ?queue-name ?backoff ?max-tries
                               :where
                               [?e :ivarref.yoltq/status :error]
                               [?e :ivarref.yoltq/queue-name ?queue-name]
                               [?e :ivarref.yoltq/error-time ?time]
                               [(>= ?backoff ?time)]
                               [?e :ivarref.yoltq/tries ?tries]
                               [(> ?max-tries ?tries)]
                               [?e :ivarref.yoltq/id ?id]
                               [?e :ivarref.yoltq/lock ?lock]]
                             (or db (d/db conn))
                             queue-name
                             (- (now-ns) error-backoff-time)
                             (inc max-retries))
                        (not-empty))]
      (let [[id old-lock] (rand-nth (into [] ids))]
        (prepare-processing id queue-name old-lock :error)))))


(defn get-hung [{:keys [conn db now hung-backoff-time max-retries] :as cfg} queue-name]
  (assert (instance? Connection conn) (str "Expected conn to be of type datomic.Connection. Was: "
                                           (str (if (nil? conn) "nil" conn))
                                           "\nConfig was: " (str cfg)))
  (let [now (or now (now-ns))
        max-retries (get-in cfg [:handlers queue-name :max-retries] max-retries)]
    (when-let [ids (->> (d/q '[:find ?id ?lock ?tries
                               :in $ ?qname ?backoff
                               :where
                               [?e :ivarref.yoltq/status :processing]
                               [?e :ivarref.yoltq/queue-name ?qname]
                               [?e :ivarref.yoltq/processing-time ?time]
                               [(>= ?backoff ?time)]
                               [?e :ivarref.yoltq/tries ?tries]
                               [?e :ivarref.yoltq/id ?id]
                               [?e :ivarref.yoltq/lock ?lock]]
                             (or db (d/db conn))
                             queue-name
                             (- now hung-backoff-time))
                        (not-empty))]
      (let [new-lock (random-uuid)
            [id old-lock tries _t] (rand-nth (into [] ids))
            to-error? (>= tries max-retries)]
        {:id         id
         :lock       new-lock
         :queue-name queue-name
         :was-hung?  true
         :to-error?  to-error?
         :tx         (if (not to-error?)
                       [[:db/cas [:ivarref.yoltq/id id] :ivarref.yoltq/lock old-lock new-lock]
                        [:db/cas [:ivarref.yoltq/id id] :ivarref.yoltq/tries tries (inc tries)]
                        {:db/id [:ivarref.yoltq/id id] :ivarref.yoltq/error-time now}]
                       [[:db/cas [:ivarref.yoltq/id id] :ivarref.yoltq/lock old-lock new-lock]
                        [:db/cas [:ivarref.yoltq/id id] :ivarref.yoltq/tries tries (inc tries)]
                        [:db/cas [:ivarref.yoltq/id id] :ivarref.yoltq/status status-processing status-error]
                        {:db/id [:ivarref.yoltq/id id] :ivarref.yoltq/error-time now}])}))))
