(ns donut.dbdna.generate.spec
  (:require
   [camel-snake-kebab.core :as csk]
   [donut.dbdna.generate :as ddg]))

(def column-types
  {:integer    'pos-int?
   :integer-pk 'nat-int?
   :text       'string?
   :varchar    'string?
   :timestamp  'inst?})

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
            (let [{:keys [columns]} (table-name dna)
                  req-columns       (->> columns
                                         (filter (fn [[_ {:keys [nullable?]}]] (not nullable?)))
                                         (map first)
                                         (map #(keyword (format-table-name table-name) (name %))))
                  opt-columns       (->> columns
                                         (filter (fn [[_ {:keys [nullable?]}]] nullable?))
                                         (map first)
                                         (map #(keyword (format-table-name table-name) (name %))))]
              (-> (reduce (fn [specs column-name]
                            (conj specs (column-spec dna table-name column-name)))
                          specs
                          (keys columns))
                  (conj (list 's/def
                              (keyword "record" (format-table-name table-name))
                              (cond-> '[s/keys]
                                (seq req-columns) (into [:req req-columns])
                                (seq opt-columns) (into [:opt opt-columns])
                                true              seq))))))
          []
          (ddg/table-order dna)))

(comment
  (require '[meander.epsilon :as m])
  (defn favorite-food-info [foods-by-name user]
    (m/match {:user          user
              :foods-by-name foods-by-name}

      {:user          {:name          ?name
                       :favorite-food {:name ?food}}
       :foods-by-name {?food {:popularity ?popularity
                              :calories   ?calories}}}
      {:name     ?name
       :favorite {:food       ?food
                  :popularity ?popularity
                  :calories   ?calories}}))

  (def foods-by-name
    {:nachos   {:popularity :high
                :calories   :lots}
     :smoothie {:popularity :high
                :calories   :less}})

  (favorite-food-info foods-by-name
                      {:name          :alice
                       :favorite-food {:name :nachos}}))
