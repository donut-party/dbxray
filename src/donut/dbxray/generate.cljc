(ns donut.dbxray.generate
  (:require
   [clojure.string :as str]
   [weavejester.dependency :as dep]))

(defn table-deps
  [xray]
  (for [[table-name table-xray] xray
        [_ column-xray]         (:columns table-xray)
        :let                    [refers-to (first (:refers-to column-xray))]
        :when                   refers-to]
    [table-name refers-to]))

(defn column-deps
  [xray]
  (for [[table-name table-xray]   xray
        [column-name column-xray] (:columns table-xray)
        :let                      [refers-to (:refers-to column-xray)]
        :when                     refers-to]
    [[table-name column-name] refers-to]))

(defn table-order
  [xray]
  (->> (table-deps xray)
       (reduce (fn [g [table-name dep]]
                 (dep/depend g table-name dep))
               (dep/graph))
       (dep/topo-sort)))

(defn singularize
  [k]
  (-> k name (str/replace #"s$" "")))
