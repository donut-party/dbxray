(ns donut.dbxray.generate.malli
  (:require
   [donut.dbxray.generate.format :as fmt]))

(def column-types
  {:integer    'int?
   :integer-pk 'pos-int?
   :clob       'string?
   :text       'string?
   :varchar    'string?
   :string     'string?
   :timestamp  'inst?
   :date       'inst?})

(defn- column-spec
  [xray table-name column-name fmt-column-name]
  (let [{:keys [column-type primary-key? nullable? refers-to]} (get-in xray [table-name :columns column-name])]
    [(fmt-column-name table-name column-name)
     {:optional? (boolean nullable?)}

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
                 (list 'def
                       (fmt-table-name table-name)
                       (->> column-order
                            (map #(column-spec tables table-name % fmt-column-name))
                            (into [:map]))))))))
