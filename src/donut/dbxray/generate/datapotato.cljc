(ns donut.dbxray.generate.datapotato
  (:require
   [clojure.string :as string]
   [donut.dbxray.generate.format :as fmt]
   [flatland.ordered.map :as omap]))

(defn- table-prefix
  [table-name]
  (let [words (string/split (name table-name) #"_")
        initials (map first words)]
    {:prefix (keyword (apply str initials))}))

(defn- table-relations
  [table-name columns fmt-column-name]
  (some->> (not-empty
            (into {}
                  (keep (fn [[column-name column-data]]
                          (when-let [refers-to (:refers-to column-data)]
                            (let [relations-key (fmt-column-name table-name column-name)
                                  [ref-table ref-col] refers-to]
                              {relations-key [ref-table (fmt-column-name ref-table ref-col)]})))
                        columns)))
           (assoc {} :relations)))

(defn- table-potato-schema
  [table-name table-data fmt-table-name fmt-column-name]
  (let [{:keys [columns]} table-data
        prefix (table-prefix table-name)]
    {(fmt-table-name table-name) (merge prefix
                                        (table-relations table-name columns fmt-column-name))}))

(defn generate
  [{:keys [tables table-order]} & [{:keys [fmt-table-name fmt-column-name]
                                    :or   {fmt-table-name  identity
                                           fmt-column-name fmt/->full-column-schema-name}}]]
  (reduce (fn [generated table-name]
            (merge generated (table-potato-schema table-name
                                                  (table-name tables)
                                                  fmt-table-name
                                                  fmt-column-name)))
          (omap/ordered-map)
          table-order))
