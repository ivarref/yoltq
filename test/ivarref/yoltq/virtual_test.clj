(ns ivarref.yoltq.virtual-test
  (:require [datomic-schema.core]
            [clojure.test :refer :all]
            [ivarref.yoltq.virtual-queue :as vq]
            [ivarref.yoltq :as dq]
            [ivarref.yoltq.test-utils :as u]
            [datomic.api :as d]
            [ivarref.yoltq.utils :as uu]
            [clojure.tools.logging :as log]
            [ivarref.yoltq.impl :as i]
            [ivarref.yoltq :as yq]))


(use-fixtures :each vq/call-with-virtual-queue!)


(deftest happy-case-1
  (let [conn (u/empty-conn)]
    (dq/init! {:conn conn})
    (dq/add-consumer! :q identity)
    @(d/transact conn [(dq/put :q {:work 123})])
    (is (= {:work 123} (:retval (vq/run-queue-once! :q :init))))))


(deftest happy-case-tx-report-q
  (let [conn (u/empty-conn)]
    (dq/init! {:conn conn})
    (dq/add-consumer! :q identity)
    @(d/transact conn [(dq/put :q {:work 123})])
    (is (= {:work 123} (:retval (vq/run-one-report-queue!))))
    (is (= 1 (u/done-count)))))


(deftest happy-case-poller
  (let [conn (u/empty-conn)]
    (dq/init! {:conn     conn
               :handlers {:q {:f (fn [payload] payload)}}})
    (u/put-transact! :q {:work 123})
    (u/advance! (:init-backoff-time yq/default-opts))
    (is (= {:work 123} (some->> (u/get-init :q)
                                (u/take!)
                                (u/execute!)
                                :retval)))))


(deftest happy-case-queue-fn-allow-cas-failure
  (let [conn (u/empty-conn)
        invoke-count (atom 0)]
    (dq/init! {:conn conn})
    (dq/add-consumer! :q
                      (fn [{:keys [id]}]
                        (swap! invoke-count inc)
                        @(d/transact conn [[:db/cas [:e/id id] :e/val nil "janei"]]))
                      {:allow-cas-failure? #{:e/val}})
    @(d/transact conn #d/schema [[:e/id :one :string :id]
                                 [:e/val :one :string]])
    @(d/transact conn [{:e/id "demo"}
                       (dq/put :q {:id "demo"})])
    (u/advance! (:init-backoff-time yq/default-opts))
    (swap! dq/*config* assoc :mark-status-fn! (fn [_ _ new-status]
                                                (log/info "mark-status! doing nothing for new status" new-status)))
    (is (nil? (some->> (u/get-init :q)
                       (u/take!)
                       (u/execute!))))
    (swap! dq/*config* dissoc :mark-status-fn!)

    ; (mark-status! :done) failed but f succeeded.
    (is (nil? (u/get-hung :q)))
    (u/advance! (:hung-backoff-time @yq/*config*))
    (is (some? (u/get-hung :q)))
    (is (true? (some->> (u/get-hung :q)
                        (u/take!)
                        (u/execute!)
                        :allow-cas-failure?)))
    (is (= 2 @invoke-count))))


(deftest backoff-test
  (let [conn (u/empty-conn)]
    (dq/init! {:conn              conn
               :init-backoff-time (:init-backoff-time yq/default-opts)
               :handlers          {:q {:f (fn [_] (throw (ex-info "janei" {})))}}})
    (u/put-transact! :q {:work 123})
    (is (nil? (u/get-init :q)))

    (u/advance! (dec (:init-backoff-time yq/default-opts)))
    (is (nil? (u/get-init :q)))
    (u/advance! 1)
    (is (some? (u/get-init :q)))

    (is (some? (some->> (u/get-init :q)
                        (u/take!)
                        (u/execute!)
                        :exception)))

    (u/advance! (dec (:error-backoff-time @yq/*config*)))
    (is (nil? (u/get-error :q)))
    (u/advance! 1)
    (is (some? (u/get-error :q)))))


(deftest get-hung-test
  (let [conn (u/empty-conn)]
    (dq/init! {:conn              conn
               :init-backoff-time (:init-backoff-time yq/default-opts)
               :handlers          {:q {:f (fn [_] (throw (ex-info "janei" {})))}}})
    (u/put-transact! :q {:work 123})
    (u/advance! (:init-backoff-time yq/default-opts))
    (is (some? (u/get-init :q)))

    (is (= :processing (some->> (u/get-init :q)
                                (u/take!)
                                :ivarref.yoltq/status)))

    (is (nil? (u/get-hung :q)))
    (u/advance! (dec (:hung-backoff-time yq/default-opts)))
    (is (nil? (u/get-hung :q)))
    (u/advance! 1)
    (is (some? (u/get-hung :q)))))


(deftest basic-locking
  (let [conn (u/empty-conn)]
    (dq/init! {:conn              conn
               :init-backoff-time (:init-backoff-time yq/default-opts)
               :cas-failures      (atom 0)
               :handlers          {:q {:f (fn [_] (throw (ex-info "janei" {})))}}})
    (u/put-transact! :q {:work 123})
    (u/advance! (:init-backoff-time yq/default-opts))
    (is (some? (u/get-init :q)))

    (let [job (u/get-init :q)]
      (is (= :processing (some->> job (u/take!) :ivarref.yoltq/status)))
      (u/take! job)
      (is (= 1 @(:cas-failures @dq/*config*))))))


(deftest retry-test
  (let [conn (u/empty-conn)]
    (dq/init! {:conn              conn
               :init-backoff-time (:init-backoff-time yq/default-opts)
               :handlers          {:q {:f
                                       (let [c (atom 0)]
                                         (fn [_]
                                           (if (<= (swap! c inc) 2)
                                             (throw (ex-info "janei" {}))
                                             ::ok)))}}})
    (u/put-transact! :q {:work 123})

    (u/advance! (:init-backoff-time yq/default-opts))
    (is (some? (some->> (u/get-init :q) (u/take!) (u/execute!) :exception)))

    (u/advance! (:error-backoff-time @yq/*config*))
    (is (some? (some->> (u/get-error :q) (u/take!) (u/execute!) :exception)))

    (u/advance! (:error-backoff-time @yq/*config*))
    (is (nil? (some->> (u/get-error :q) (u/take!) (u/execute!) :exception)))))


(deftest max-retries-test
  (let [conn (u/empty-conn)
        call-count (atom 0)]
    (dq/init! {:conn               conn
               :error-backoff-time 0})
    (dq/add-consumer! :q (fn [_]
                           (swap! call-count inc)
                           (throw (ex-info "janei" {})))
                      {:max-retries 1})
    (vq/put! :q {:work 123})
    (is (some? (:exception (vq/run-one-report-queue!))))

    (dotimes [_ 10]
      (vq/run-queue-once! :q :error))
    (is (= 2 @call-count))))


(deftest max-retries-test-two
  (let [conn (u/empty-conn)
        call-count (atom 0)]
    (dq/init! {:conn               conn
               :error-backoff-time 0})
    (dq/add-consumer! :q (fn [_]
                           (swap! call-count inc)
                           (throw (ex-info "janei" {})))
                      {:max-retries 3})
    (vq/put! :q {:work 123})
    (is (some? (:exception (vq/run-one-report-queue!))))

    (dotimes [_ 20]
      (vq/run-queue-once! :q :error))
    (is (= 4 @call-count))))


(deftest hung-to-error
  (let [conn (u/empty-conn)
        call-count (atom 0)
        missed-mark-status (atom nil)]
    (dq/init! {:conn conn})
    (dq/add-consumer! :q
                      (fn [_]
                        (if (= 1 (swap! call-count inc))
                          (throw (ex-info "error" {}))
                          (log/info "return OK"))))
    (vq/put! :q {:id "demo"})
    (vq/run-one-report-queue!)                              ; now in status :error


    (swap! dq/*config* assoc :mark-status-fn! (fn [_ _ new-status]
                                                (reset! missed-mark-status new-status)
                                                (log/info "mark-status! doing nothing for new status" new-status)))
    (u/advance! (:error-backoff-time @yq/*config*))
    (vq/run-queue-once! :q :error)
    (swap! dq/*config* dissoc :mark-status-fn!)
    (is (= :done @missed-mark-status))

    (is (nil? (uu/get-hung @dq/*config* :q)))
    (u/advance! (:hung-backoff-time @yq/*config*))

    (is (some? (uu/get-hung @dq/*config* :q)))

    (is (= 2 @call-count))

    (is (true? (some->> (uu/get-hung (assoc-in @dq/*config* [:handlers :q :max-retries] 1) :q)
                        (i/take! @dq/*config*)
                        (i/execute! @dq/*config*)
                        :failed?)))

    (u/advance! (:error-backoff-time @yq/*config*))
    (is (some? (uu/get-error @dq/*config* :q)))
    (is (nil? (uu/get-error (assoc-in @dq/*config* [:handlers :q :max-retries] 1) :q)))))


