(ns donut.dbdna.generate.spec-test
  (:require
   [clojure.test :refer [deftest is]]
   [donut.dbdna.generate.spec :as ddgs]))

(deftest generates-spec-specs
  (is (= '[(s/def :user/id nat-int?)
           (s/def :user/username string?)
           (s/def :record/user (s/keys :req [:user/id :user/username]))
           (s/def :todo-list/id nat-int?)
           (s/def :todo-list/created_by_id nat-int?)
           (s/def :record/todo-list
             (s/keys :req [:todo-list/id]
                     :opt [:todo-list/created_by_id]))
           (s/def :todo/id nat-int?)
           (s/def :todo/todo_list_id nat-int?)
           (s/def :todo/todo_title string?)
           (s/def :todo/notes string?)
           (s/def :todo/created_by_id nat-int?)
           (s/def :todo/created_at inst?)
           (s/def :record/todo
             (s/keys :req [:todo/id :todo/todo_title]
                     :opt [:todo/todo_list_id :todo/notes :todo/created_by_id :todo/created_at]))]
         (ddgs/generate
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
                                                       :refers-to   [:users :id]}
                                       :created_at    {:column-type :timestamp
                                                       :nullable?   true}}
                        :column-order [:id :todo_list_id :todo_title :notes :created_by_id :created_at]}}))))
