(ns donut.dbdna.generate
  (:require
   [clojure.string :as str]
   [weavejester.dependency :as dep]))

(defn table-deps
  [dna]
  (for [[table-name table-dna]   dna
        [_ column-dna] (:columns table-dna)
        :let                     [refers-to (first (:refers-to column-dna))]
        :when                    refers-to]
    [table-name refers-to]))

(defn column-deps
  [dna]
  (for [[table-name table-dna]   dna
        [column-name column-dna] (:columns table-dna)
        :let                     [refers-to (:refers-to column-dna)]
        :when                    refers-to]
    [[table-name column-name] refers-to]))

(defn table-order
  [dna]
  (->> (table-deps dna)
       (reduce (fn [g [table-name dep]]
                 (dep/depend g table-name dep))
               (dep/graph))
       (dep/topo-sort)))

(defn singularize
  [k]
  (-> k name (str/replace #"s$" "")))
