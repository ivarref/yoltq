(ns com.github.ivarref.yoltq.migrate
  (:require [datomic.api :as d]
            [clojure.tools.logging :as log]))

(defn to->v2-ent [{:keys [conn]} now-ms id]
  (log/info "Migrating id" id)
  (let [attr-val (fn [attr]
                   (when-let [old (d/q '[:find ?time .
                                         :in $ ?e ?a
                                         :where
                                         [?e ?a ?time]]
                                       (d/db conn)
                                       [:com.github.ivarref.yoltq/id id]
                                       attr)]
                     (let [now-ms (or now-ms
                                      (.getTime (d/q '[:find (max ?txinst) .
                                                       :in $ ?e ?a
                                                       :where
                                                       [?e ?a _ ?tx true]
                                                       [?tx :db/txInstant ?txinst]]
                                                     (d/history (d/db conn))
                                                     [:com.github.ivarref.yoltq/id id]
                                                     attr)))]
                       (log/info "Updating" id attr "to" now-ms)
                       [[:db/cas [:com.github.ivarref.yoltq/id id]
                         attr old now-ms]])))]
    (vec (concat [[:db/cas [:com.github.ivarref.yoltq/id id]
                   :com.github.ivarref.yoltq/version nil "2"]]
                 (mapcat attr-val [:com.github.ivarref.yoltq/init-time
                                   :com.github.ivarref.yoltq/processing-time
                                   :com.github.ivarref.yoltq/done-time
                                   :com.github.ivarref.yoltq/error-time])))))

(defn to->v2 [{:keys [conn loop? now-ms]
               :or   {loop? true}
               :as   cfg}]
  (loop [tx-vec []]
    (if-let [id (some->> (d/q '[:find [?id ...]
                                :in $
                                :where
                                [?e :com.github.ivarref.yoltq/id ?id]
                                [(missing? $ ?e :com.github.ivarref.yoltq/version)]]
                              (d/db conn))
                         (sort)
                         (not-empty)
                         (first))]
      (let [tx (to->v2-ent cfg now-ms id)]
        @(d/transact conn tx)
        (if loop?
          (recur (vec (take 10 (conj tx-vec tx))))
          tx))
      (do
        (log/info "No items left to migrate")
        tx-vec))))


(defn migrate! [cfg]
  (to->v2 cfg))

(comment
  (migrate! @com.github.ivarref.yoltq/*config*))
