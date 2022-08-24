(ns donut.dbdna.generate.malli
  (:require
   [camel-snake-kebab.core :as csk]
   [donut.dbdna.generate :as ddg]
   [meander.epsilon :as m]))

(defn table-spec-name
  [table-name]
  (-> table-name
      ddg/singularize
      (csk/->PascalCaseSymbol table-name)))

(defn column-predicate
  [dna table-name column-name]
  (m/rewrite {:dna         dna
              :table-name  table-name
              :column-name column-name}

    ;; when there's a refers-to, recur using reference
    {:dna         {?table-name {:columns {?column-name {:refers-to [?ref-table-name ?ref-column-name]}}}
                   :as ?dna}
     :table-name  ?table-name
     :column-name ?column-name}
    (m/cata {:dna         ?dna
             :table-name  ?ref-table-name
             :column-name ?ref-column-name})

    ;; get the column-dna
    {:dna         {?table-name {:columns {?column-name ?column-dna}}}
     :table-name  ?table-name
     :column-name ?column-name}
    (m/cata ?column-dna)

    ;; all the different types
    {:column-type :integer :primary-key? true} nat-int?
    {:column-type :integer} int?
    {:column-type :text} string?
    {:column-type :varchar} string?
    {:column-type :timestamp} inst?))

(defn generate
  [dna]
  (let [column-predicate (partial column-predicate dna)]
    (m/rewrite dna

      ;; columns specs
      [?table-name
       (m/and (m/seqable !col-names-1 ...)
              (m/seqable !col-names-2 ...))
       (m/seqable !col-dnas ...)]
      [:map .
       [!col-names-1
        {:optional? (m/app (comp boolean :nullable?) !col-dnas)}
        (m/app column-predicate ?table-name !col-names-2)] ...]

      ;; table spec
      (m/and {} (m/gather [(m/and !table-names-1 !table-names-2) {:columns (m/and (m/app keys !col-names)
                                                                                  (m/app vals !col-dnas))}]))
      [(def (m/app table-spec-name !table-names-1)
         (m/cata [!table-names-2 !col-names !col-dnas])) ...])))
