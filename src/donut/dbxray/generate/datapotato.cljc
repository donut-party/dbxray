(ns donut.dbxray.generate.datapotato
  (:require
   [clojure.string :as string]
   [donut.dbxray.generate :as ddg]))

(defn- table-prefix
  [table-name]
  (let [words (string/split (name table-name) #"_")
        initials (map first words)]
    {:prefix (keyword (apply str initials))}))

(defn- full-column-name
  [table-name col-name]
  (let [table (name table-name)]
    (keyword (str table "/" (name col-name)))))

(defn- gen-relations-for-column
  [table-name column-name column-data]
  (when-let [refers-to (:refers-to column-data)]
    (let [relations-key (full-column-name table-name column-name)
          [ref-table ref-col] refers-to
          relations-path [ref-table (full-column-name ref-table ref-col)]]
      {:relations {relations-key relations-path}})))

(defn- table-potato-schema
  [table-name table-data]
  (let [{:keys [columns]} table-data
        prefix (gen-table-prefix table-name)]
    {table-name (merge prefix
                       (reduce (fn [relations [column-name column-data]]
                                 (merge-with into
                                             relations
                                             (gen-relations-for-column table-name column-name column-data)))
                               {}
                               columns))}))


(defn generate
  [xray]
  (reduce (fn [generated [table-name table-data]]
            (merge generated (gen-potato-for-table table-name table-data)))
          {}
          xray))
