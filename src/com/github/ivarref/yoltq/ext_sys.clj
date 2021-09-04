(ns com.github.ivarref.yoltq.ext-sys
  (:require [datomic.api :as d])
  (:import (java.util UUID)))


(def ^:dynamic *now-ns-atom* nil)
(def ^:dynamic *squuid-atom* nil)
(def ^:dynamic *random-atom* nil)


(defn now-ns []
  (if *now-ns-atom*
    @*now-ns-atom*
    (System/nanoTime)))


(defn squuid []
  (if *squuid-atom*
    (UUID/fromString (str "00000000-0000-0000-0000-" (format "%012d" (swap! *squuid-atom* inc))))
    (d/squuid)))


(defn random-uuid []
  (if *random-atom*
    (UUID/fromString (str "00000000-0000-0000-0000-" (format "%012d" (swap! *random-atom* inc))))
    (UUID/randomUUID)))