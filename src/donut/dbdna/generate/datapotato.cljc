(ns donut.dbdna.generate.datapotato
  (:require
   [camel-snake-kebab.core :as csk]
   [donut.dbdna.generate :as ddg]
   [meander.epsilon :as m]))

(defn generate
  [dna])

(require '[flatland.ordered.map :as omap])
(def todo-list-dna
  {:users      {:columns (omap/ordered-map
                          :id       {:column-type  :integer
                                     :primary-key? true
                                     :unique?      true}
                          :username {:column-type :varchar
                                     :unique?     true})}
   :todo_lists {:columns (omap/ordered-map
                          :id            {:column-type  :integer
                                          :primary-key? true
                                          :unique?      true}
                          :created_by_id {:column-type :integer
                                          :nullable?   true
                                          :refers-to   [:users :id]})}})

(m/rewrite todo-list-dna
  [?table-name
   (m/and (m/seqable !col-names-1 ...)
          (m/seqable !col-names-2 ...))
   (m/seqable !col-dnas ...)]
  {?table-name {:prefix :TODO
                :relations {}}}

  (m/and {} (m/gather [!table-name {:columns {!column-names {:refers-to !references}}}]))
  (m/app merge (m/cata !table-name !column-names !references)))


(m/rewrite {:foo  {:refers-to [:users :id]}
            :nope {}}
  (m/gather [!column-name !x])
  [!column-name !x ...])
