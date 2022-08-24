(ns donut.dbdna.generate.datapotato
  (:require
   [camel-snake-kebab.core :as csk]
   [meander.epsilon :as m]
   [clojure.string :as str]))

(defn prefix
  [table-name]
  (->> (str/split (csk/->snake_case_string table-name) #"_")
       (map first)
       (apply str)
       (keyword)))

(defn column-spec-name
  [table-name column-name]
  (keyword (name table-name) (name column-name)))

(defn relations-path
  [[table-name column-name]]
  [table-name (column-spec-name table-name column-name)])

(defn generate
  [dna]
  ;; trying to get column names to be namespaced with table name
  (m/rewrite dna

    ;; match on table
    (m/seqable [(m/and !outer-table-names !inner-table-names (m/app prefix !prefixes))
                {:columns !columns}]
               ...)
    (m/app merge {!outer-table-names (m/app merge
                                            {:prefix !prefixes}
                                            (m/cata [!inner-table-names !columns]))} ...)

    ;; table-name / columns
    [?table-name (m/seqable [!column-name !column-dna] ...)]
    (m/app (partial merge-with merge) (m/cata [?table-name !column-name !column-dna]) ...)

    ;; columns with a refers-to
    [?table-name ?column-name {:refers-to (m/pred identity ?reference)}]
    {:relations {(m/app column-spec-name ?table-name ?column-name)
                 (m/app relations-path ?reference)}}

    ;; columns without a refers-to
    [?table-name ?column-name {:refers-to (m/pred not)}]
    nil))
