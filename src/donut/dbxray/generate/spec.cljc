(ns donut.dbxray.generate.spec
  (:require
   [donut.dbxray.generate.format :as fmt]
   [inflections.core :as inflections]))

(def column-types
  {:integer    'int?
   :integer-pk 'pos-int?
   :clob       'string?
   :text       'string?
   :varchar    'string?
   :string     'string?
   :timestamp  'inst?
   :date       'inst?})

(defn- table-spec-name
  [table-name]
  (keyword "record" (inflections/singular (name table-name))))

(defn- column-spec
  [tables table-name column-name fmt-column-name]
  (let [{:keys [column-type primary-key? refers-to]} (get-in tables [table-name :columns column-name])]
    (list 's/def
          (fmt-column-name table-name column-name)

          (cond
            refers-to
            (last (column-spec tables (first refers-to) (second refers-to) fmt-column-name))

            (and (= :integer column-type) primary-key?)
            (:integer-pk column-types)

            :else
            (column-type column-types [:TODO/column-type-not-recognized column-type])))))

(defn- column-specs
  [tables table-name fmt-column-name]
  (mapv #(column-spec tables table-name % fmt-column-name)
        (get-in tables [table-name :column-order])))

(defn- skeys-columns
  [table-name columns req? fmt-column-name]
  (->> columns
       (filter (fn [[_ {:keys [nullable?]}]] (if (= req? :req) (not nullable?) nullable?)))
       (map first)
       (mapv #(fmt-column-name table-name %))))

(defn- table-spec
  [tables table-name fmt-table-name fmt-column-name]
  (let [columns     (get-in tables [table-name :columns])
        req-columns (skeys-columns table-name columns :req fmt-column-name)
        opt-columns (skeys-columns table-name columns :opt fmt-column-name)]
    (list 's/def
          (fmt-table-name table-name)
          (cond-> '[s/keys]
            (seq req-columns) (into [:req req-columns])
            (seq opt-columns) (into [:opt opt-columns])
            true              seq))))

(defn generate
  [{:keys [tables table-order]} & [{:keys [fmt-table-name fmt-column-name]
                                    :or   {fmt-table-name  table-spec-name
                                           fmt-column-name fmt/->full-column-schema-name}}]]
  (->> table-order
       (mapcat (fn [table-name]
                 (conj (column-specs tables table-name fmt-column-name)
                       (table-spec tables table-name fmt-table-name fmt-column-name))))
       vec))
