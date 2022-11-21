(ns donut.dbxray.generate.spec
  (:require
   [inflections.core :as inflections]))

(def column-types
  {:integer    'int?
   :integer-pk 'pos-int?
   :clob       'string?
   :text       'string?
   :varchar    'string?
   :timestamp  'inst?})

(defn- table-spec-name
  [table-name]
  (keyword "record" (inflections/singular (name table-name))))

(defn- column-spec-name
  [table-name column-name]
  (keyword (name table-name) (name column-name)))

(defn- column-spec
  [tables table-name column-name]
  (let [{:keys [column-type primary-key? refers-to]} (get-in tables [table-name :columns column-name])]
    (list 's/def
          (column-spec-name table-name column-name)

          (cond
            refers-to
            (last (column-spec tables (first refers-to) (second refers-to)))

            (and (= :integer column-type) primary-key?)
            (:integer-pk column-types)

            :else
            (column-type column-types [:TODO/column-type-not-recognized column-type])))))

(defn- column-specs
  [tables table-name]
  (mapv #(column-spec tables table-name %)
        (get-in tables [table-name :column-order])))

(defn- skeys-columns
  [table-name columns req?]
  (->> columns
       (filter (fn [[_ {:keys [nullable?]}]] (if (= req? :req) (not nullable?) nullable?)))
       (map first)
       (mapv #(column-spec-name table-name %))))

(defn- table-spec
  [tables table-name]
  (let [columns     (get-in tables [table-name :columns])
        req-columns (skeys-columns table-name columns :req)
        opt-columns (skeys-columns table-name columns :opt)]
    (list 's/def
          (table-spec-name table-name)
          (cond-> '[s/keys]
            (seq req-columns) (into [:req req-columns])
            (seq opt-columns) (into [:opt opt-columns])
            true              seq))))

(defn generate
  [{:keys [tables table-order]}]
  (->> table-order
       (mapcat (fn [table-name]
                 (conj (column-specs tables table-name)
                       (table-spec tables table-name))))
       vec))
