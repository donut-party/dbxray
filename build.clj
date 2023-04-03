(ns build
  "dbxray's build script. inspired by:
  * https://github.com/seancorfield/honeysql/blob/develop/build.clj
  * https://github.com/seancorfield/build-clj

  Run tests:
  clojure -X:test
  clojure -X:test-cljs
  For more information, run:
  clojure -A:deps -T:build help/doc"
  (:refer-clojure :exclude [test])
  (:require [clojure.tools.build.api :as b]
            [org.corfield.build :as bb]))


(def lib 'ai.motiva/dbxray)
(def version "0.1.0")


(defn deploy
  "Deploy the JAR to Clojars"
  [opts]
  (-> opts
      (assoc :lib lib :version version)
      (bb/deploy)))


(defn jar
  "Builds a jar"
  [opts]
  (-> opts
      (assoc :lib lib :version version)
      (bb/clean)
      (bb/jar)))


(defn install
  "Install the JAR locally."
  [opts]
  (-> opts
      (assoc :lib lib :version version)
      (bb/install)))
