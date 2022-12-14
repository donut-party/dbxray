(ns donut.dbxray.generate.datapotato-test
  (:require
   [clojure.test :refer [deftest is]]
   [donut.dbxray.generate.datapotato :as ddgd]
   [donut.dbxray.fixtures :as ddf]
   [flatland.ordered.map :as omap]))

(deftest generates-datapotato-schema
  (is (= (into []
               (omap/ordered-map :users      {:prefix :u}
                                 :todo_lists {:prefix    :tl
                                              :relations {:todo_lists/created_by_id [:users :users/id]}}
                                 :todos      {:prefix    :t
                                              :relations {:todos/todo_list_id  [:todo_lists :todo_lists/id]
                                                          :todos/created_by_id [:users :users/id]}}))
         (into [] (ddgd/generate ddf/todo-list-xray)))))
