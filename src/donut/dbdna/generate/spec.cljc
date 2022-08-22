(ns donut.dbdna.generate.spec
  (:require
   [camel-snake-kebab.core :as csk]
   [donut.dbdna.generate :as ddg]
   [meander.epsilon :as m]))

(def column-types
  {:integer    'int?
   :integer-pk 'nat-int?
   :text       'string?
   :varchar    'string?
   :timestamp  'inst?})

(defn column-predicate
  [dna table-name column-name]
  (let [{:keys [column-type refers-to primary-key?]} (get-in dna [table-name :columns column-name])]
    (cond
      refers-to
      (column-predicate dna (first refers-to) (second refers-to))

      (and (= :integer column-type) primary-key?)
      (:integer-pk column-types)

      :else
      (column-type column-types))))

(defn table-spec-name
  [table-name]
  (keyword "record" (-> table-name
                        ddg/singularize
                        csk/->kebab-case-string)))

(defn column-spec-name
  [table-name column-name]
  (keyword (name table-name) (name column-name)))

(defn column-specs
  [dna table-dna]
  (vec
   (m/search table-dna
     [?table-name {:columns {?column-name _}}]

     (list 's/def
           (column-spec-name ?table-name ?column-name)
           (column-predicate dna ?table-name ?column-name)))))

(defn table-spec
  [table-dna]
  (m/rewrite table-dna
    [?table-name {:columns {& (m/seqable (m/or [!reqcn (m/pred (complement :nullable?))]
                                               [!optcn (m/pred :nullable?)])
                                         ...)}}]

    ;; use cata to recur and remove empty :opt or :req
    (m/cata
     (s/def (m/app table-spec-name ?table-name)
       (s/keys
        ;; wrap :req and :opt in vectors to make empty checking easier
        [:req [(m/app column-spec-name ?table-name !reqcn) ...]]
        [:opt [(m/app column-spec-name ?table-name !optcn) ...]])))

    ;; remove empty :opt or :req
    (s/def ?spec-name
      (s/keys & (m/gather [!req (m/pred seq !specs)])))
    (s/def ?spec-name
      (s/keys . !req !specs ...))))

(defn generate
  [dna]
  (mapcat (fn [table-name]
            (let [table-dna [table-name (table-name dna)]]
              (conj (column-specs dna table-dna)
                    (table-spec table-dna))))
          (ddg/table-order dna)))


(comment
  ;; keeper
  (m/rewrite {:a {:m {}
                  :n {}}
              :b {:m {}
                  :o {}}}

    [?k (m/and (m/seqable !v ...) ?v2)]
    [[?k !v] ... [?k & ?v2]]

    (m/and {} (m/gather [!k (m/app keys !v)]))
    (m/app into (m/cata [!k !v]) ...)))
