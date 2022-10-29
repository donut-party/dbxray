(ns donut.dbxray.generate.plumatic-schema-test
  (:require
   [clojure.test :refer [deftest is]]
   [donut.dbxray.generate.plumatic-schema :as ddgps]
   [donut.dbxray.fixtures :as ddf]))

(deftest generates-plumatic-schema-specs
  (is (= '[(s/defschema User
             {(s/required-key :users/id)       s/Int
              (s/required-key :users/username) s/Str})
           (s/defschema TodoList
             {(s/required-key :todo_lists/id) s/Int
              :todo_lists/created_by_id       s/Int})
           (s/defschema Todo
             {(s/required-key :todos/id)         s/Int
              :todos/todo_list_id                s/Int
              (s/required-key :todos/todo_title) s/Str
              :todos/notes                       s/Str
              :todos/created_by_id               s/Int
              :todos/created_at                  s/Inst})]
         (ddgps/generate ddf/todo-list-xray))))
