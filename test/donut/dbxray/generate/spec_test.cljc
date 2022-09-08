(ns donut.dbxray.generate.spec-test
  (:require
   [clojure.test :refer [deftest is]]
   [donut.dbxray.fixtures :as ddf]
   [donut.dbxray.generate.spec :as ddgs]))

(deftest generates-spec-specs
  (is (= '[(s/def :users/id pos-int?)
           (s/def :users/username string?)
           (s/def :record/user (s/keys :req [:users/id :users/username]))
           (s/def :todo_lists/id pos-int?)
           (s/def :todo_lists/created_by_id pos-int?)
           (s/def :record/todo_list
             (s/keys :req [:todo_lists/id]
                     :opt [:todo_lists/created_by_id]))
           (s/def :todos/id pos-int?)
           (s/def :todos/todo_list_id pos-int?)
           (s/def :todos/todo_title string?)
           (s/def :todos/notes string?)
           (s/def :todos/created_by_id pos-int?)
           (s/def :todos/created_at inst?)
           (s/def :record/todo
             (s/keys :req [:todos/id :todos/todo_title]
                     :opt [:todos/todo_list_id :todos/notes :todos/created_by_id :todos/created_at]))]
         (ddgs/generate ddf/todo-list-xray))))
