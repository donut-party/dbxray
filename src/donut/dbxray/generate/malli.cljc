(ns donut.dbxray.generate.malli
  (:require
   [camel-snake-kebab.core :as csk]
   [donut.dbxray.generate :as ddg]))

(def column-types
  {:integer    'int?
   :integer-pk 'pos-int?
   :clob       'string?
   :text       'string?
   :varchar    'string?
   :timestamp  'inst?})

(defn table-spec-name
  [table-name]
  (-> table-name
      ddg/singularize
      (csk/->PascalCaseSymbol table-name)))

(defn column-spec-name
  [table-name column-name]
  (keyword (name table-name) (name column-name)))

(defn column-spec
  [xray table-name column-name]
  (let [{:keys [column-type primary-key? nullable? refers-to]} (get-in xray [table-name :columns column-name])]
    (or [(column-spec-name table-name column-name)
         {:optional? (boolean nullable?)}

         (cond
           refers-to
           (last (column-spec xray (first refers-to) (second refers-to)))

           (and (= :integer column-type) primary-key?)
           (:integer-pk column-types)

           :else
           (column-type column-types))]
        (throw (ex-info "unknown column-type" {:column-type column-type})))))

(defn generate
  [xray]
  (mapv (fn [table-name]
          (let [table-xray (table-name xray)]
            (list 'def
                  (table-spec-name table-name)
                  (into [:map] (map #(column-spec xray table-name %)
                                    (keys (:columns table-xray)))))))
        (ddg/table-order xray)))
