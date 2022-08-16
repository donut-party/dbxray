(ns donut.dbdna.generate.malli-test
  (:require
   [clojure.test :refer [deftest is]]
   [donut.dbdna.generate.malli :as ddgm]))

(deftest generates-malli-specs
  (is (= '[(def User
             [:map
              [:id {:optional? false} nat-int?]
              [:username {:optional? false} string?]])
           (def TodoList
             [:map
              [:id {:optional? false} nat-int?]
              [:created_by_id {:optional? true} nat-int?]])
           (def Todo
             [:map
              [:id {:optional? false} nat-int?]
              [:todo_list_id {:optional? true} nat-int?]
              [:todo_title {:optional? false} string?]
              [:notes {:optional? true} string?]
              [:created_by_id {:optional? true} nat-int?]])]
         (ddgm/generate
          {:users      {:columns      {:id       {:column-type  :integer
                                                  :primary-key? true
                                                  :unique?      true}
                                       :username {:column-type :varchar
                                                  :unique?     true}}
                        :column-order [:id :username]}
           :todo_lists {:columns      {:id            {:column-type  :integer
                                                       :primary-key? true
                                                       :unique?      true}
                                       :created_by_id {:column-type :integer
                                                       :nullable?   true
                                                       :refers-to   [:users :id]}}
                        :column-order [:id :created_by_id]}
           :todos      {:columns      {:id            {:column-type  :integer
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
                                                       :refers-to   [:users :id]}}
                        :column-order [:id :todo_list_id :todo_title :notes :created_by_id]}}))))
