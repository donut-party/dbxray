(ns donut.dbdna.generate.spec
  (:require
   [camel-snake-kebab.core :as csk]
   [donut.dbdna.generate :as ddg]
   [meander.epsilon :as m]))

(defn column-predicate
  [dna table-name column-name]
  (m/rewrite {:dna         dna
              :table-name  table-name
              :column-name column-name}

    ;; when there's a refers-to, recur using reference
    {:dna         {?table-name {:columns {?column-name {:refers-to [?ref-table-name ?ref-column-name]}}}
                   :as ?dna}
     :table-name  ?table-name
     :column-name ?column-name}
    (m/cata {:dna         ?dna
             :table-name  ?ref-table-name
             :column-name ?ref-column-name})

    ;; get the column-dna
    {:dna         {?table-name {:columns {?column-name ?column-dna}}}
     :table-name  ?table-name
     :column-name ?column-name}
    (m/cata ?column-dna)

    ;; all the different types
    {:column-type :integer :primary-key? true} nat-int?
    {:column-type :integer} int?
    {:column-type :text} string?
    {:column-type :varchar} string?
    {:column-type :timestamp} inst?))

(defn table-spec-name
  [table-name]
  (keyword "record" (ddg/singularize table-name)))

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
