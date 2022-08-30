(ns donut.dbxray.fixtures
  (:require [flatland.ordered.map :as omap]))

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
                                          :refers-to   [:users :id]})}
   :todos      {:columns (omap/ordered-map
                          :id            {:column-type  :integer
                                          :primary-key? true
                                          :unique?      true}
                          :todo_list_id  {:column-type :integer
                                          :nullable?   true
                                          :refers-to   [:todo_lists :id]}
                          :todo_title    {:column-type :varchar}
                          :notes         {:column-type :text
                                          :nullable?   true}
                          :created_by_id {:column-type :integer
                                          :nullable?   true
                                          :refers-to   [:users :id]}
                          :created_at    {:column-type :timestamp
                                          :nullable?   true})}})
