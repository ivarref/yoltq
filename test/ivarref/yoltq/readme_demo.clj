(ns ivarref.yoltq.readme-demo
  (:require [clojure.tools.logging :as log]
            [datomic.api :as d]
            [ivarref.yoltq :as yq])
  (:import (java.util UUID)))

(defonce conn
  (let [uri (str "datomic:mem://hello-world-" (UUID/randomUUID))]
    (d/delete-database uri)
    (d/create-database uri)
    (d/connect uri)))

(ivarref.yoltq.log-init/init-logging!
  [[#{"datomic.*" "com.datomic.*" "org.apache.*"} :warn]
   [#{"ivarref.yoltq.report-queue"} :debug]
   [#{"ivarref.yoltq.poller"} :info]
   [#{"ivarref.yoltq*"} :debug]
   [#{"*"} :info]])

(yq/stop!)

(yq/init! {:conn conn})


(yq/add-consumer! :q
                  (let [cnt (atom 0)]
                    (fn [payload]
                      (when (= 1 (swap! cnt inc))
                        (throw (ex-info "failed" {})))
                      (log/info "got payload" payload))))

(yq/start!)

@(d/transact conn [(yq/put :q {:work 123})])

(comment
  (yq/add-consumer! :q (fn [_] (throw (ex-info "always fail" {})))))

(comment
  @(d/transact conn [(yq/put :q {:work 123})]))

(comment
  (do
    (yq/add-consumer! :q (fn [_] :ok))
    nil))
#_(comment
    (do
      (require 'ivarref.yoltq.log-init)
      (require '[datomic.api :as d])
      (require '[ivarref.yoltq :as yq])

      (yq/stop!)
      (let [uri (str "datomic:mem://hello-world-" (UUID/randomUUID))]
        (d/delete-database uri)
        (d/create-database uri)
        (let [conn (d/connect uri)]
          (yq/init! {:conn conn})
          (yq/add-consumer! :q
                            (let [cnt (atom 0)]
                              (fn [payload]
                                (when (= 1 (swap! cnt inc))
                                  (throw (ex-info "failed" {})))
                                (log/info "got payload" payload))))
          (yq/start!)
          @(d/transact conn [(yq/put :q {:work 123})])
          nil))))