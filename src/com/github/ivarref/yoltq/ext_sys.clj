(ns com.github.ivarref.yoltq.ext-sys
  (:require [datomic.api :as d])
  (:refer-clojure :exclude [random-uuid])
  (:import (java.util UUID)))


(def ^:dynamic *now-ms-atom* nil)
(def ^:dynamic *squuid-atom* nil)
(def ^:dynamic *random-atom* nil)


(defn now-ms []
  (if *now-ms-atom*
    @*now-ms-atom*
    (System/currentTimeMillis)))


(defn squuid []
  (if *squuid-atom*
    (UUID/fromString (str "00000000-0000-0000-0000-" (format "%012d" (swap! *squuid-atom* inc))))
    (d/squuid)))


(defn random-uuid []
  (if *random-atom*
    (UUID/fromString (str "00000000-0000-0000-0000-" (format "%012d" (swap! *random-atom* inc))))
    (UUID/randomUUID)))
