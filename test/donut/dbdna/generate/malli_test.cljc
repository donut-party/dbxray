(ns donut.dbdna.generate.malli-test
  (:require
   [clojure.test :refer [deftest is]]
   [donut.dbdna.generate.malli :as ddgm]
   [donut.dbdna.fixtures :as ddf]))

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
              [:created_by_id {:optional? true} nat-int?]
              [:created_at {:optional? true} inst?]])]
         (ddgm/generate ddf/todo-list-dna))))
