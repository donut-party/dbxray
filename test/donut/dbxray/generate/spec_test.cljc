(ns donut.dbxray.generate.spec-test
  (:require
   [clojure.test :refer [deftest is]]
   [donut.dbxray.fixtures :as ddf]
   [donut.dbxray.generate.spec :as ddgs]))

(deftest generates-spec-specs
  (is (= '[(s/def :user/id pos-int?)
           (s/def :user/username string?)
           (s/def :record/user (s/keys :req [:user/id :user/username]))
           (s/def :todo-list/id pos-int?)
           (s/def :todo-list/created_by_id pos-int?)
           (s/def :record/todo-list
             (s/keys :req [:todo-list/id]
                     :opt [:todo-list/created_by_id]))
           (s/def :todo/id pos-int?)
           (s/def :todo/todo_list_id pos-int?)
           (s/def :todo/todo_title string?)
           (s/def :todo/notes string?)
           (s/def :todo/created_by_id pos-int?)
           (s/def :todo/created_at inst?)
           (s/def :record/todo
             (s/keys :req [:todo/id :todo/todo_title]
                     :opt [:todo/todo_list_id :todo/notes :todo/created_by_id :todo/created_at]))]
         (ddgs/generate ddf/todo-list-xray))))
