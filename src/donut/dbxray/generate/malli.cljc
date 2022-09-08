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

(defn column-spec
  [xray table-name column-name]
  (let [{:keys [column-type primary-key? nullable? refers-to]} (get-in xray [table-name :columns column-name])]
    (or [column-name
         {:optional? (boolean nullable?)}

         (cond
           refers-to
           (last (column-spec xray (first refers-to) (second refers-to)))

           (and (= :integer column-type) primary-key?)
           (:integer-pk column-types)

           :else
           (column-type column-types))]
        (throw (ex-info "unknown column-type" {:column-type column-type})))))

(defn table-name->spec-name
  [table-name]
  (-> table-name
      name
      ddg/singularize
      csk/->PascalCaseSymbol))

(defn generate
  [xray]
  (mapv (fn [table-name]
          (let [table-xray (table-name xray)]
            (list 'def
                  (table-name->spec-name table-name)
                  (into [:map] (map #(column-spec xray table-name %)
                                    (keys (:columns table-xray)))))))
        (ddg/table-order xray)))
