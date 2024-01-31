(ns donut.dbxray.generate.plumatic-schema
  (:require
   [camel-snake-kebab.core :as csk]
   [inflections.core :as inflections]))

(def column-types
  {:integer    's/Int
   :integer-pk 's/Int
   :clob       's/Str
   :text       's/Str
   :varchar    's/Str
   :string     's/Str
   :timestamp  's/Inst
   :date       's/Inst})

(defn- table-spec-name
  [table-name]
  (-> table-name
      inflections/singular
      csk/->PascalCaseSymbol))

(defn- column-spec-name
  [table-name column-name]
  (keyword (name table-name) (name column-name)))

(defn- column-spec
  [xray table-name column-name]
  (let [{:keys [column-type primary-key? nullable? refers-to]} (get-in xray [table-name :columns column-name])]
    [(cond->> (column-spec-name table-name column-name)
       (not nullable?) (list 's/required-key))

     (cond
       refers-to
       (last (column-spec xray (first refers-to) (second refers-to)))

       (and (= :integer column-type) primary-key?)
       (:integer-pk column-types)

       :else
       (column-type column-types [:TODO/column-type-not-recognized column-type]))]))

(defn generate
  [{:keys [tables table-order]}]
  (->> table-order
       (mapv (fn [table-name]
               (let [{:keys [column-order]} (table-name tables)]
                 (list 's/defschema
                       (table-spec-name table-name)
                       (->> column-order
                            (map #(column-spec tables table-name %))
                            (into {}))))))))
