(ns ivarref.yoltq.http-hang-demo
  (:require [datomic.api :as d]
            [ivarref.yoltq :as yq]
            [ivarref.yoltq.log-init])
  (:import (java.net.http HttpClient HttpRequest HttpResponse$BodyHandlers)))

(comment
  (do
    (ivarref.yoltq.log-init/init-logging!
      [[#{"datomic.*" "com.datomic.*" "org.apache.*"} :warn]
       [#{"ivarref.yoltq.report-queue"} :debug]
       [#{"ivarref.yoltq.poller"} :info]
       [#{"ivarref.yoltq*"} :debug]
       [#{"*"} :info]])
    (yq/stop!)
    (let [received (atom [])
          uri (str "datomic:mem://hello-world-" (java.util.UUID/randomUUID))]
      (d/delete-database uri)
      (d/create-database uri)
      (let [conn (d/connect uri)]
        (init! {:conn                          conn
                :error-backoff-time            (Duration/ofSeconds 5)
                :poll-delay                    5
                :system-error-poll-interval    [1 TimeUnit/MINUTES]
                :system-error-callback-backoff (Duration/ofHours 1)
                :max-execute-time              (Duration/ofSeconds 3)
                :on-system-error               (fn [] (log/error "system in error state"))
                :on-system-recovery            (fn [] (log/info "system recovered"))})
        (yq/add-consumer! :slow (fn [_payload]
                                  (log/info "start slow handler...")
                                  ; sudo tc qdisc del dev wlp3s0 root netem
                                  ; sudo tc qdisc add dev wlp3s0 root netem delay 10000ms
                                  ; https://jvns.ca/blog/2017/04/01/slow-down-your-internet-with-tc/
                                  (let [request (-> (HttpRequest/newBuilder)
                                                    (.uri (java.net.URI. "https://postman-echo.com/get"))
                                                    (.timeout (Duration/ofSeconds 10))
                                                    (.GET)
                                                    (.build))]
                                    (log/info "body is:" (-> (.send (HttpClient/newHttpClient) request (HttpResponse$BodyHandlers/ofString))
                                                             (.body))))
                                  (log/info "slow handler is done!")))
        (yq/start!)
        @(d/transact conn [(put :slow {:work 123})])
        #_(dotimes [x 1] @(d/transact conn [(put :q {:work x})]))
        nil))))