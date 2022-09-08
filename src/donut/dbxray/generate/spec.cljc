(ns donut.dbxray.generate.spec
  (:require
   [camel-snake-kebab.core :as csk]
   [donut.dbxray.generate :as ddg]))

(def column-types
  {:integer    'int?
   :integer-pk 'pos-int?
   :text       'string?
   :varchar    'string?
   :timestamp  'inst?})

(defn format-table-name
  [table-name]
  (-> table-name
      ddg/singularize
      csk/->kebab-case-string))

(defn column-spec
  [xray table-name column-name]
  (let [{:keys [column-type primary-key? refers-to]} (get-in xray [table-name :columns column-name])]

    (or (list 's/def
              (keyword (format-table-name table-name) (name column-name))

              (cond
                refers-to
                (last (column-spec xray (first refers-to) (second refers-to)))

                (and (= :integer column-type) primary-key?)
                (:integer-pk column-types)

                :else
                (column-type column-types)))
        (throw (ex-info "unknown column-type" {:column-type column-type})))))

(defn generate
  [xray]
  (reduce (fn [specs table-name]
            (let [{:keys [columns]} (table-name xray)
                  req-columns       (->> columns
                                         (filter (fn [[_ {:keys [nullable?]}]] (not nullable?)))
                                         (map first)
                                         (map #(keyword (format-table-name table-name) (name %))))
                  opt-columns       (->> columns
                                         (filter (fn [[_ {:keys [nullable?]}]] nullable?))
                                         (map first)
                                         (map #(keyword (format-table-name table-name) (name %))))]
              (-> (reduce (fn [specs column-name]
                            (conj specs (column-spec xray table-name column-name)))
                          specs
                          (keys columns))
                  (conj (list 's/def
                              (keyword "record" (format-table-name table-name))
                              (cond-> '[s/keys]
                                (seq req-columns) (into [:req req-columns])
                                (seq opt-columns) (into [:opt opt-columns])
                                true              seq))))))
          []
          (ddg/table-order xray)))
