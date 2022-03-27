(ns com.github.ivarref.yoltq.migrate-test
  (:require [clojure.test :refer [deftest is]]
            [com.github.ivarref.yoltq.ext-sys :as ext]
            [com.github.ivarref.yoltq.migrate :as m]
            [com.github.ivarref.yoltq.impl :as impl]
            [com.github.ivarref.yoltq.test-utils :as tu]
            [com.github.ivarref.yoltq.utils :as u]
            [datomic.api :as d]))


(deftest to-v2-migration
  (with-bindings {#'ext/*squuid-atom* (atom 0)}
    (let [conn (tu/empty-conn)]
      @(d/transact conn impl/schema)
      @(d/transact conn [{:com.github.ivarref.yoltq/id              (u/squuid)
                          :com.github.ivarref.yoltq/queue-name      :dummy
                          :com.github.ivarref.yoltq/status          u/status-processing
                          :com.github.ivarref.yoltq/init-time       1
                          :com.github.ivarref.yoltq/processing-time 2}])
      @(d/transact conn [{:com.github.ivarref.yoltq/id         (u/squuid)
                          :com.github.ivarref.yoltq/queue-name :dummy
                          :com.github.ivarref.yoltq/status     u/status-init
                          :com.github.ivarref.yoltq/init-time  3}])
      (is (= [[[:db/cas
                [:com.github.ivarref.yoltq/id
                 #uuid "00000000-0000-0000-0000-000000000001"]
                :com.github.ivarref.yoltq/version
                nil
                "2"]
               [:db/cas
                [:com.github.ivarref.yoltq/id
                 #uuid "00000000-0000-0000-0000-000000000001"]
                :com.github.ivarref.yoltq/init-time
                1
                1000]
               [:db/cas
                [:com.github.ivarref.yoltq/id
                 #uuid "00000000-0000-0000-0000-000000000001"]
                :com.github.ivarref.yoltq/processing-time
                2
                1000]]
              [[:db/cas
                [:com.github.ivarref.yoltq/id
                 #uuid "00000000-0000-0000-0000-000000000002"]
                :com.github.ivarref.yoltq/version
                nil
                "2"]
               [:db/cas
                [:com.github.ivarref.yoltq/id
                 #uuid "00000000-0000-0000-0000-000000000002"]
                :com.github.ivarref.yoltq/init-time
                3
                1000]]]
             (m/migrate! {:conn   conn
                          :now-ms 1000
                          :loop?  true})))
      (is (= []
             (m/migrate! {:conn   conn
                          :now-ms 1000
                          :loop?  true}))))))


(deftest to-v2-migration-real-time
  (with-bindings {#'ext/*squuid-atom* (atom 0)}
    (let [conn (tu/empty-conn)
          id (u/squuid)]
      @(d/transact conn impl/schema)
      @(d/transact conn [{:com.github.ivarref.yoltq/id         id
                          :com.github.ivarref.yoltq/queue-name :dummy
                          :com.github.ivarref.yoltq/status     u/status-init
                          :com.github.ivarref.yoltq/init-time  1}])
      (Thread/sleep 100)
      @(d/transact conn [{:com.github.ivarref.yoltq/id        id
                          :com.github.ivarref.yoltq/init-time 2}])
      (let [tx-times (->> (d/q '[:find [?txinst ...]
                                 :in $ ?e
                                 :where
                                 [?e :com.github.ivarref.yoltq/init-time _ ?tx true]
                                 [?tx :db/txInstant ?txinst]]
                               (d/history (d/db conn))
                               [:com.github.ivarref.yoltq/id id])
                          (sort)
                          (vec))]
        (is (= 2 (count tx-times)))
        (m/migrate! {:conn conn})
        (is (= (.getTime (last tx-times))
               (d/q '[:find ?init-time .
                      :in $ ?e
                      :where
                      [?e :com.github.ivarref.yoltq/init-time ?init-time]]
                    (d/db conn)
                    [:com.github.ivarref.yoltq/id id])))))))
