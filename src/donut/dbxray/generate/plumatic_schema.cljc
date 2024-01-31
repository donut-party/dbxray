(ns donut.dbxray.generate.plumatic-schema
  (:require
   [donut.dbxray.generate.format :as fmt]))

(def column-types
  {:integer    's/Int
   :integer-pk 's/Int
   :clob       's/Str
   :text       's/Str
   :varchar    's/Str
   :string     's/Str
   :timestamp  's/Inst
   :date       's/Inst})

(defn- column-spec
  [xray table-name column-name fmt-column-name]
  (let [{:keys [column-type primary-key? nullable? refers-to]} (get-in xray [table-name :columns column-name])]
    [(cond->> (fmt-column-name table-name column-name)
       (not nullable?) (list 's/required-key))

     (cond
       refers-to
       (last (column-spec xray (first refers-to) (second refers-to) fmt-column-name))

       (and (= :integer column-type) primary-key?)
       (:integer-pk column-types)

       :else
       (column-type column-types [:TODO/column-type-not-recognized column-type]))]))

(defn generate
  [{:keys [tables table-order]} & [{:keys [fmt-table-name fmt-column-name]
                                    :or   {fmt-table-name  fmt/->var-name
                                           fmt-column-name fmt/->full-column-schema-name}}]]
  (->> table-order
       (mapv (fn [table-name]
               (let [{:keys [column-order]} (table-name tables)]
                 (list 's/defschema
                       (fmt-table-name table-name)
                       (->> column-order
                            (map #(column-spec tables table-name % fmt-column-name))
                            (into {}))))))))
