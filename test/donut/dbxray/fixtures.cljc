(ns donut.dbxray.fixtures)

(def todo-list-xray
  {:tables
   {:users
    {:columns
     {:id       {:column-type  :integer
                 :primary-key? true
                 :unique?      true}
      :username {:column-type :varchar
                 :unique?     true}}
     :column-order
     [:id :username]}

    :todo_lists
    {:columns
     {:id            {:column-type  :integer
                      :primary-key? true
                      :unique?      true}
      :created_by_id {:column-type :integer
                      :nullable?   true
                      :refers-to   [:users :id]}}
     :column-order
     [:id :created_by_id]}

    :todos
    {:columns
     {:id            {:column-type  :integer
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
                      :nullable?   true}}
     :column-order
     [:id :todo_list_id :todo_title :notes :created_by_id :created_at]}}

   :table-order
   [:users :todo_lists :todos]})
