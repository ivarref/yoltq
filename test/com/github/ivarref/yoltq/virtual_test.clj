(ns com.github.ivarref.yoltq.virtual-test
  (:require [datomic-schema.core]
            [clojure.test :refer :all]
            [com.github.ivarref.yoltq.test-queue :as tq]
            [com.github.ivarref.yoltq.test-utils :as u]
            [datomic.api :as d]
            [com.github.ivarref.yoltq.utils :as uu]
            [clojure.tools.logging :as log]
            [com.github.ivarref.yoltq.impl :as i]
            [com.github.ivarref.yoltq :as yq]
            [taoensso.timbre :as timbre]))


(use-fixtures :each tq/call-with-virtual-queue!)


(deftest happy-case-1
  (let [conn (u/empty-conn)]
    (yq/init! {:conn conn})
    (yq/add-consumer! :q identity)
    @(d/transact conn [(yq/put :q {:work 123})])
    (is (= {:work 123} (tq/consume! :q)))))


(deftest happy-case-tx-report-q
  (let [conn (u/empty-conn)]
    (yq/init! {:conn conn})
    (yq/add-consumer! :q identity)
    @(d/transact conn [(yq/put :q {:work 123})])
    (is (= {:work 123} (:retval (tq/run-one-report-queue!))))
    (is (= 1 (u/done-count)))))


(deftest happy-case-poller
  (let [conn (u/empty-conn)]
    (yq/init! {:conn     conn
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
    (yq/init! {:conn conn})
    (yq/add-consumer! :q
                      (fn [{:keys [id]}]
                        (swap! invoke-count inc)
                        @(d/transact conn [[:db/cas [:e/id id] :e/val nil "janei"]]))
                      {:allow-cas-failure? #{:e/val}})
    @(d/transact conn #d/schema [[:e/id :one :string :id]
                                 [:e/val :one :string]])
    @(d/transact conn [{:e/id "demo"}
                       (yq/put :q {:id "demo"})])
    (u/advance! (:init-backoff-time yq/default-opts))
    (swap! yq/*config* assoc :mark-status-fn! (fn [_ _ new-status]
                                                (log/info "mark-status! doing nothing for new status" new-status)))
    (is (nil? (some->> (u/get-init :q)
                       (u/take!)
                       (u/execute!))))
    (swap! yq/*config* dissoc :mark-status-fn!)

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
    (yq/init! {:conn              conn
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
    (yq/init! {:conn              conn
               :init-backoff-time (:init-backoff-time yq/default-opts)
               :handlers          {:q {:f (fn [_] (throw (ex-info "janei" {})))}}})
    (u/put-transact! :q {:work 123})
    (u/advance! (:init-backoff-time yq/default-opts))
    (is (some? (u/get-init :q)))

    (is (= :processing (some->> (u/get-init :q)
                                (u/take!)
                                :com.github.ivarref.yoltq/status)))

    (is (nil? (u/get-hung :q)))
    (u/advance! (dec (:hung-backoff-time yq/default-opts)))
    (is (nil? (u/get-hung :q)))
    (u/advance! 1)
    (is (some? (u/get-hung :q)))))


(deftest basic-locking
  (let [conn (u/empty-conn)]
    (yq/init! {:conn              conn
               :init-backoff-time (:init-backoff-time yq/default-opts)
               :cas-failures      (atom 0)
               :handlers          {:q {:f (fn [_] (throw (ex-info "janei" {})))}}})
    (u/put-transact! :q {:work 123})
    (u/advance! (:init-backoff-time yq/default-opts))
    (is (some? (u/get-init :q)))

    (let [job (u/get-init :q)]
      (is (= :processing (some->> job (u/take!) :com.github.ivarref.yoltq/status)))
      (u/take! job)
      (is (= 1 @(:cas-failures @yq/*config*))))))


(deftest retry-test
  (let [conn (u/empty-conn)]
    (yq/init! {:conn              conn
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
    (yq/init! {:conn               conn
               :error-backoff-time 0})
    (yq/add-consumer! :q (fn [_]
                           (swap! call-count inc)
                           (throw (ex-info "janei" {})))
                      {:max-retries 1})
    (tq/put! :q {:work 123})
    (is (some? (:exception (tq/run-one-report-queue!))))

    (dotimes [_ 10]
      (tq/run-queue-once! :q :error))
    (is (= 2 @call-count))))


(deftest max-retries-test-two
  (let [conn (u/empty-conn)
        call-count (atom 0)]
    (yq/init! {:conn               conn
               :error-backoff-time 0})
    (yq/add-consumer! :q (fn [_]
                           (swap! call-count inc)
                           (throw (ex-info "janei" {})))
                      {:max-retries 3})
    (tq/put! :q {:work 123})
    (is (some? (:exception (tq/run-one-report-queue!))))

    (dotimes [_ 20]
      (tq/run-queue-once! :q :error))
    (is (= 4 @call-count))))


(deftest hung-to-error
  (let [conn (u/empty-conn)
        call-count (atom 0)
        missed-mark-status (atom nil)]
    (yq/init! {:conn conn})
    (yq/add-consumer! :q
                      (fn [_]
                        (if (= 1 (swap! call-count inc))
                          (throw (ex-info "error" {}))
                          (log/info "return OK"))))
    (tq/put! :q {:id "demo"})
    (tq/run-one-report-queue!)                              ; now in status :error


    (swap! yq/*config* assoc :mark-status-fn! (fn [_ _ new-status]
                                                (reset! missed-mark-status new-status)
                                                (log/info "mark-status! doing nothing for new status" new-status)))
    (u/advance! (:error-backoff-time @yq/*config*))
    (tq/run-queue-once! :q :error)
    (swap! yq/*config* dissoc :mark-status-fn!)
    (is (= :done @missed-mark-status))

    (is (nil? (uu/get-hung @yq/*config* :q)))
    (u/advance! (:hung-backoff-time @yq/*config*))

    (is (some? (uu/get-hung @yq/*config* :q)))

    (is (= 2 @call-count))

    (is (true? (some->> (uu/get-hung (assoc-in @yq/*config* [:handlers :q :max-retries] 1) :q)
                        (i/take! @yq/*config*)
                        (i/execute! @yq/*config*)
                        :failed?)))

    (u/advance! (:error-backoff-time @yq/*config*))
    (is (some? (uu/get-error @yq/*config* :q)))
    (is (nil? (uu/get-error (assoc-in @yq/*config* [:handlers :q :max-retries] 1) :q)))))


(deftest consume-expect-test
  (let [conn (u/empty-conn)
        seen (atom #{})]
    (yq/init! {:conn conn})
    (yq/add-consumer! :q (fn [payload]
                           (when (= #{1 2} (swap! seen conj payload))
                             (throw (ex-info "oops" {})))
                           payload))

    @(d/transact conn [(yq/put :q 1)])
    @(d/transact conn [(yq/put :q 2)])

    (is (= 1 (tq/consume-expect! :q :done)))
    (tq/consume-expect! :q :error)))


(def ^:dynamic *some-binding* nil)


(deftest binding-test
  (let [conn (u/empty-conn)]
    (yq/init! {:conn             conn
               :capture-bindings [#'*some-binding* #'timbre/*context*]})
    (yq/add-consumer! :q (fn [_] *some-binding*))
    (binding [timbre/*context* {:x-request-id "wooho"}]
      (binding [*some-binding* 1]
        @(d/transact conn [(yq/put :q nil)]))
      (binding [*some-binding* 2]
        @(d/transact conn [(yq/put :q nil)]))
      @(d/transact conn [(yq/put :q nil)]))

    (is (= 1 (tq/consume-expect! :q :done)))
    (is (= 2 (tq/consume-expect! :q :done)))
    (is (nil? (tq/consume-expect! :q :done)))))


(deftest default-binding-test
  (let [conn (u/empty-conn)]
    (yq/init! {:conn conn})
    (yq/add-consumer! :q (fn [_] (:x-request-id timbre/*context*)))
    (binding [timbre/*context* {:x-request-id "123"}]
      @(d/transact conn [(yq/put :q nil)]))
    (is (= "123" (tq/consume-expect! :q :done)))))


(deftest force-retry-test
  (let [conn (u/empty-conn)]
    (yq/init! {:conn conn})
    (yq/add-consumer! :q (let [cnt (atom 0)]
                           (fn [_] (swap! cnt inc))))
    @(d/transact conn [(yq/put :q nil)])
    (is (= 1 (tq/consume! :q)))
    (is (= 2 (tq/force-retry! :q)))
    (is (= 3 (tq/force-retry! :q)))))


(deftest ext-id-no-duplicates
  (let [conn (u/empty-conn)]
    (yq/init! {:conn conn})
    (yq/add-consumer! :q identity)
    @(d/transact conn [(yq/put :q nil {:id "123"})])
    (is (thrown? Exception @(d/transact conn [(yq/put :q nil {:id "123"})])))))


(deftest depends-on
  (let [conn (u/empty-conn)]
    (yq/init! {:conn conn})
    (yq/add-consumer! :a identity)
    (yq/add-consumer! :b identity)
    @(d/transact conn [(yq/put :a "a" {:id "1"})])
    (is (thrown? Exception @(d/transact conn [(yq/put :b "b" {:depends-on [:a "0"]})])))
    @(d/transact conn [(yq/put :b "b" {:depends-on [:a "1"]})])

    ; can't consume :b yet:
    (is (= {:depends-on [:a "1"]} (tq/consume! :b)))
    (is (= {:depends-on [:a "1"]} (tq/consume! :b)))

    (is (= "a" (tq/consume! :a)))
    (is (= "b" (tq/consume! :b)))
    (is (= "b" (tq/force-retry! :b)))))

