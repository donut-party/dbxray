(ns donut.dbdna.generate.malli
  (:require
   [camel-snake-kebab.core :as csk]
   [clojure.string :as str]
   [donut.dbdna.generate :as ddg]))

(def column-types
  {:integer    'pos-int?
   :integer-pk 'nat-int?
   :text       'string?})

(defn column-spec
  [column-name table-name dna]
  (let [{:keys [column-type primary-key? nullable? refers-to]} (get-in dna [table-name :columns column-name])]
    [column-name
     {:optional? (boolean nullable?)}

     (cond
       refers-to
       (last (column-spec (second refers-to) (first refers-to) dna))

       (and (= :integer column-type) primary-key?)
       (:integer-pk column-types)

       :else
       (column-type column-types))]))

(defn generate
  [dna]
  (reduce (fn [specs table-name]
            (let [table-dna (table-name dna)]
              (->> (reduce (fn [spec column-name]
                             (conj spec (column-spec column-name table-name dna)))
                           [:map]
                           (:column-order table-dna))
                   (conj ['def (-> table-name
                                   name
                                   (str/replace #"s$" "")
                                   csk/->PascalCaseSymbol)])
                   seq
                   (conj specs))))
          []
          (ddg/table-order dna)))
