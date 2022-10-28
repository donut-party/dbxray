(ns donut.dbxray.generate
  (:require [clojure.string :as str]))

(defn singularize
  [k]
  (-> k name (str/replace #"s$" "")))
