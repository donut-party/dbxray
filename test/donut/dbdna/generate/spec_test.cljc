(ns donut.dbdna.generate.spec-test
  (:require
   [clojure.test :refer [deftest is]]
   [donut.dbdna.fixtures :as ddf]
   [donut.dbdna.generate.spec :as ddgs]))

(deftest generates-spec-specs
  (is (= '[(s/def :users/id nat-int?)
           (s/def :users/username string?)
           (s/def :record/user (s/keys :req [:users/id :users/username]))
           (s/def :todo_lists/id nat-int?)
           (s/def :todo_lists/created_by_id nat-int?)
           (s/def :record/todo-list
             (s/keys :req [:todo_lists/id]
                     :opt [:todo_lists/created_by_id]))
           (s/def :todos/id nat-int?)
           (s/def :todos/todo_list_id nat-int?)
           (s/def :todos/todo_title string?)
           (s/def :todos/notes string?)
           (s/def :todos/created_by_id nat-int?)
           (s/def :todos/created_at inst?)
           (s/def :record/todo
             (s/keys :req [:todos/id :todos/todo_title]
                     :opt [:todos/todo_list_id :todos/notes :todos/created_by_id :todos/created_at]))]
         (ddgs/generate ddf/todo-list-dna))))
