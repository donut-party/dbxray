dbxray provides metadata about JDBC databases and uses that data to generate
specs/schemas that you can copy into your application. It can save you some time
if you're just getting started with [spec](https://clojure.org/guides/spec) or
[malli](https://github.com/metosin/malli).

Here's the kind of metadata it produces:

``` clojure
(ns donut.dbxray.todo-list
  (:require
   [donut.dbxray :as dbx]
   [next.jdbc :as jdbc])
  (:import
   (io.zonky.test.db.postgres.embedded EmbeddedPostgres)))

(defonce embedded-pg (delay (EmbeddedPostgres/start)))
(defonce conn (delay (jdbc/get-connection (.getPostgresDatabase ^EmbeddedPostgres @embedded-pg))))

;; Given these tables:
;; (NOTE: actual table creation is not shown in this example)
(def create-users
  (str
   "CREATE TABLE users ("
   "  id       serial PRIMARY KEY, "
   "  username varchar(256) NOT NULL UNIQUE "
   ")"))

(def create-todo-lists
  (str
   "CREATE TABLE todo_lists ("
   "  id      serial PRIMARY KEY, "
   "  user_id integer NOT NULL, "
   "  name    varchar(256) NOT NULL, "
   "  FOREIGN KEY(user_id)"
   "    REFERENCES users(id)"
   ")"))

(def create-todos
  (str
   "CREATE TABLE todos ("
   "  id           serial PRIMARY KEY, "
   "  todo_list_id integer NOT NULL, "
   "  description  varchar(256) NOT NULL, "
   "  FOREIGN KEY(todo_list_id)"
   "    REFERENCES todo_lists(id)"
   ")"))

;; dbxray produces this metadata:
(dbxray/xray @conn) ;; =>

{:users
 {:columns {:id       {:column-type    :integer
                       :raw            {...}
                       :primary-key?   true
                       :unique?        true
                       :autoincrement? true}
            :username {:column-type :varchar
                       :raw         {...}
                       :unique?     true}}}

 :todo_lists
 {:columns {:id      {:column-type    :integer
                      :raw            {...}
                      :primary-key?   true
                      :unique?        true
                      :autoincrement? true}
            :user_id {:column-type :integer
                      :raw         {...}
                      :refers-to   [:users :id]}
            :name    {:column-type :varchar
                      :raw         {...}}}}
 :todos
 {:columns {:id           {:column-type    :integer
                           :raw            {...}
                           :primary-key?   true
                           :unique?        true
                           :autoincrement? true}
            :todo_list_id {:column-type :integer
                           :raw         {...}
                           :refers-to   [:todo_lists :id]}
            :description  {:column-type :varchar
                           :raw         {...}}}}}
```

Note that `:raw` contains more metadata but it's been elided to keep the example
focused.

You can generate basic specs from this metadata:

``` clojure
(require '[donut.dbxray.generate.malli :as ddgm])
(ddgm/generate (dbxray/xray connection))
[(def User
   [:map
    [:users/id {:optional? false} pos-int?]
    [:users/username {:optional? false} string?]])
 (def TodoList
   [:map
    [:todo_lists/id {:optional? false} pos-int?]
    [:todo_lists/user_id {:optional? false} pos-int?]
    [:todo_lists/name {:optional? false} string?]])
 (def Todo
   [:map
    [:todos/id {:optional? false} pos-int?]
    [:todos/todo_list_id {:optional? false} pos-int?]
    [:todos/description {:optional? false} string?]])]
```

If you run this in a REPL you can just copy and paste it into your source code.
