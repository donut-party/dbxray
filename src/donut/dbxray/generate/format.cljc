(ns donut.dbxray.generate.format
  "Format table and column names"
  (:require
   [camel-snake-kebab.core :as csk]
   [inflections.core :as inflections]))

(defn inflected-case
  [s & [{:keys [inflection ->case]
         :or {inflection inflections/singular
              ->case csk/->PascalCaseSymbol}}]]
  (-> s inflection ->case))


(defn ->case-keyword
  [ns kw & [{:keys [->ns-case ->kw-case]
             :or   {->ns-case identity
                    ->kw-case identity}}]]
  (keyword (name (->ns-case ns))
           (name (->kw-case kw))))

(defn ->var-name
  [s]
  (inflected-case s))

(defn ->full-column-schema-name
  [table-name column-name]
  (->case-keyword table-name column-name))

(defn ->column-schema-name
  [_ column-name]
  (keyword (name column-name)))

(defn column-name
  [_ column-name]
  column-name)
