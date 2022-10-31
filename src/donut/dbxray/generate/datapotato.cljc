(ns donut.dbxray.generate.datapotato
  (:require
   [camel-snake-kebab.core :as csk]
   [clojure.string :as string]
   [flatland.ordered.map :as omap]
   [donut.dbxray.generate :as ddg]))

(defn gen-table-prefix
  [table-name]
  (let [words (string/split (name table-name) #"_") 
        initials (map (comp str first) words)]
    {:prefix (keyword (apply str initials))}))

(defn add-table-prefix-to-col-name
  [table-name col-name]
  (let [table (name table-name)]
    (keyword (str table "/" (name col-name)))))

(defn munge-refers-to
  [[refers-to-table refers-to-col]]
  [refers-to-table (add-table-prefix-to-col-name refers-to-table refers-to-col)]) 

(defn gen-relations-for-column
  [table-name column-name column-data]
  (when-let [ref (:refers-to column-data)]
    (let [relations-key (add-table-prefix-to-col-name table-name column-name)
          relations-path (munge-refers-to ref)]
      {:relations {relations-key relations-path}})))

(defn gen-potato-for-table
  [table-name table-data]
  (let [{:keys [columns]} table-data
        prefix (gen-table-prefix table-name)]
    {table-name (merge prefix
                       (reduce (fn [relations [column-name column-data]]
                                 (merge-with into
                                             relations
                                             (gen-relations-for-column table-name column-name column-data)))
                               {} columns))}))


(defn generate
  [xray]
  (reduce (fn [generated [table-name table-data]]
            (merge-with into generated (gen-potato-for-table table-name table-data)))
          {}
          xray))
