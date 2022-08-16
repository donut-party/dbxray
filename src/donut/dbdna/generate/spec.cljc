(ns donut.dbdna.generate.spec
  (:require
   [camel-snake-kebab.core :as csk]
   [clojure.string :as str]
   [donut.dbdna.generate :as ddg]))

(def column-types
  {:integer    'pos-int?
   :integer-pk 'nat-int?
   :text       'string?
   :varchar    'string?})

(defn format-table-name
  [table-name]
  (-> table-name
      ddg/singularize
      csk/->kebab-case-string))

(defn column-spec
  [dna table-name column-name]
  (let [{:keys [column-type primary-key? refers-to]} (get-in dna [table-name :columns column-name])]

    (or (list 's/def
              (keyword (format-table-name table-name) (name column-name))

              (cond
                refers-to
                (last (column-spec  dna (first refers-to) (second refers-to)))

                (and (= :integer column-type) primary-key?)
                (:integer-pk column-types)

                :else
                (column-type column-types)))
        (throw (ex-info "unknown column-type" {:column-type column-type})))))

(defn generate
  [dna]
  (reduce (fn [specs table-name]
            (let [{:keys [columns column-order]} (table-name dna)
                  req-columns                    (->> column-order
                                                      (filter (->> columns
                                                                   (filter (fn [[_ {:keys [nullable?]}]] (not nullable?)))
                                                                   (map first)
                                                                   set))
                                                      (mapv #(keyword (format-table-name table-name) (name %))))
                  opt-columns                    (->> column-order
                                                      (filter (->> columns
                                                                   (filter (fn [[_ {:keys [nullable?]}]] nullable?))
                                                                   (map first)
                                                                   set))
                                                      (mapv #(keyword (format-table-name table-name) (name %))))]
              (-> (reduce (fn [specs column-name]
                            (conj specs (column-spec dna table-name column-name)))
                          specs
                          column-order)
                  (conj (list 's/def
                              (keyword "record" (format-table-name table-name))
                              (cond-> '[s/keys]
                                (seq req-columns) (into [:req req-columns])
                                (seq opt-columns) (into [:opt opt-columns])
                                true              seq))))))
          []
          (ddg/table-order dna)))
