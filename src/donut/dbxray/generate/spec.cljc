(ns donut.dbxray.generate.spec
  (:require
   [donut.dbxray.generate :as ddg]))

(def column-types
  {:integer    'int?
   :integer-pk 'pos-int?
   :clob       'string?
   :text       'string?
   :varchar    'string?
   :timestamp  'inst?})

(defn- table-spec-name
  [table-name]
  (keyword "record" (ddg/singularize table-name)))

(defn- column-spec-name
  [table-name column-name]
  (keyword (name table-name) (name column-name)))

(defn- column-spec
  [xray table-name column-name]
  (let [{:keys [column-type primary-key? refers-to]} (get-in xray [table-name :columns column-name])]
    (list 's/def
          (column-spec-name table-name column-name)

          (cond
            refers-to
            (last (column-spec xray (first refers-to) (second refers-to)))

            (and (= :integer column-type) primary-key?)
            (:integer-pk column-types)

            :else
            (column-type column-types [:TODO/column-type-not-recognized column-type])))))

(defn- column-specs
  [xray table-name]
  (mapv #(column-spec xray table-name %)
        (keys (get-in xray [table-name :columns]))))

(defn- skeys-columns
  [table-name columns req?]
  (->> columns
       (filter (fn [[_ {:keys [nullable?]}]] (if (= req? :req) (not nullable?) nullable?)))
       (map first)
       (mapv #(column-spec-name table-name %))))

(defn- table-spec
  [xray table-name]
  (let [columns     (get-in xray [table-name :columns])
        req-columns (skeys-columns table-name columns :req)
        opt-columns (skeys-columns table-name columns :opt)]
    (list 's/def
          (table-spec-name table-name)
          (cond-> '[s/keys]
            (seq req-columns) (into [:req req-columns])
            (seq opt-columns) (into [:opt opt-columns])
            true              seq))))

(defn generate
  [xray]
  (->> xray
       keys
       (mapcat (fn [table-name]
                 (conj (column-specs xray table-name)
                       (table-spec xray table-name))))
       vec))
