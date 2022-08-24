(ns donut.dbdna.generate.datapotato
  (:require
   [camel-snake-kebab.core :as csk]
   [donut.dbdna.generate :as ddg]
   [meander.epsilon :as m]
   [clojure.string :as str]))

(defn generate
  [dna])

(require '[flatland.ordered.map :as omap])
(def todo-list-dna
  {:users      {:columns (omap/ordered-map
                          :id       {:column-type  :integer
                                     :primary-key? true}
                          :username {:column-type :varchar})}
   :todo_lists {:columns (omap/ordered-map
                          :id            {:column-type  :integer
                                          :primary-key? true}
                          :created_by_id {:column-type :integer
                                          :nullable?   true
                                          :refers-to   [:users :id]})}})

;; problems
;; 1. include :relations iff a column has :refers-to
;; 2. change map values

(defn prefix
  [table-name]
  (->> (str/split (csk/->snake_case_string table-name) #"_")
       (map first)
       (apply str)
       (keyword)))

(m/rewrite todo-list-dna

  ;; match on table
  (m/seqable [(m/and !table-names-1
                     !table-names-2)
              {:columns {& (m/seqable [!column-names !column-dnas] ..!column-counts)}}]
             ...)
  (m/app merge {!table-names-1 (m/app merge
                                      {:prefix :f}
                                      (m/cata [!column-names !column-dnas]) ..!column-counts)} ...)

  ;; columns with a refers-to
  [?column-name {:refers-to (m/pred identity ?reference)}]
  {:relations {?column-name ?reference}}

  ;; columns without a refers-to
  [?column-name {:refers-to (m/pred not)}]
  nil)
