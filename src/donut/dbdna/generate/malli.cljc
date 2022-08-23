(ns donut.dbdna.generate.malli
  (:require
   [camel-snake-kebab.core :as csk]
   [donut.dbdna.generate :as ddg]
   [meander.epsilon :as m]))

(def column-types
  {:integer    'int?
   :integer-pk 'nat-int?
   :text       'string?
   :varchar    'string?
   :timestamp  'inst?})

(defn table-spec-name
  [table-name]
  (-> table-name
      ddg/singularize
      (csk/->PascalCaseSymbol table-name)))

(defn column-predicate
  [dna table-name column-name]
  (let [{:keys [column-type refers-to primary-key?]} (get-in dna [table-name :columns column-name])]
    (cond
      refers-to
      (column-predicate dna (first refers-to) (second refers-to))

      (and (= :integer column-type) primary-key?)
      (:integer-pk column-types)

      :else
      (column-type column-types))))

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
