(ns donut.dbdna.generate.malli-test
  (:require
   [clojure.test :refer [deftest is]]
   [donut.dbdna.generate.malli :as ddgm]
   [donut.dbdna.fixtures :as ddf]))

(deftest generates-malli-specs
  (is (= '[(def User
             [:map
              [:users/id {:optional? false} nat-int?]
              [:users/username {:optional? false} string?]])
           (def TodoList
             [:map
              [:todo_lists/id {:optional? false} nat-int?]
              [:todo_lists/created_by_id {:optional? true} nat-int?]])
           (def Todo
             [:map
              [:todos/id {:optional? false} nat-int?]
              [:todos/todo_list_id {:optional? true} nat-int?]
              [:todos/todo_title {:optional? false} string?]
              [:todos/notes {:optional? true} string?]
              [:todos/created_by_id {:optional? true} nat-int?]
              [:todos/created_at {:optional? true} inst?]])]
         (ddgm/generate ddf/todo-list-dna))))
