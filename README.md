[![Clojars Project](https://img.shields.io/clojars/v/party.donut/dbxray.svg)](https://clojars.org/party.donut/dbxray)


dbxray provides metadata about JDBC databases and uses that data to generate
specs/schemas that you can copy into your application. It can save you some time
if you're just getting started with [spec](https://clojure.org/guides/spec),
[malli](https://github.com/metosin/malli), or [plumatic
schema](https://github.com/plumatic/schema).

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
(dbx/xray @conn {:include-raw? true}) ;; =>

{:tables
 {:users
  {:columns      {:id       {:column-type    :integer
                             :raw            {...}
                             :primary-key?   true
                             :unique?        true
                             :autoincrement? true}
                  :username {:column-type :varchar
                             :raw         {...}
                             :unique?     true}}
   :column-order [:id :username]}

  :todo_lists
  {:columns      {:id      {:column-type    :integer
                            :raw            {...}
                            :primary-key?   true
                            :unique?        true
                            :autoincrement? true}
                  :user_id {:column-type :integer
                            :raw         {...}
                            :refers-to   [:users :id]}
                  :name    {:column-type :varchar
                            :raw         {...}}}
   :column-order [:id :user_id :name]}
  :todos
  {:columns      {:id           {:column-type    :integer
                                 :raw            {...}
                                 :primary-key?   true
                                 :unique?        true
                                 :autoincrement? true}
                  :todo_list_id {:column-type :integer
                                 :raw         {...}
                                 :refers-to   [:todo_lists :id]}
                  :description  {:column-type :varchar
                                 :raw         {...}}}
   :column-order [:id :todo_list_id :description]}}

 :table-order
 [:users :todo_lists :todos]}
```

Note that you must pass in `{:include-raw? true}` as the second argument to
`xray` to include raw metata. `:raw` has been been elided
to keep the example focused.

You can generate basic specs or schemas from this metadata:

``` clojure
(require '[donut.dbxray :as dbx])
(def xray (dbx/xray connection))

(dbx/malli-schema xray)
;; =>
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
    
(dbx/clojure-spec xray)
;; =>
[(s/def :users/id pos-int?)
 (s/def :users/username string?)
 (s/def :record/user (s/keys :req [:users/id :users/username]))
 (s/def :todo_lists/id pos-int?)
 (s/def :todo_lists/user_id pos-int?)
 (s/def :todo_lists/name string?)
 (s/def :record/todo_list
   (s/keys :req [:todo_lists/id :todo_lists/user_id :todo_lists/name]))
 (s/def :todos/id pos-int?)
 (s/def :todos/todo_list_id pos-int?)
 (s/def :todos/description string?)
 (s/def :record/todo
   (s/keys :req [:todos/id :todos/todo_list_id :todos/description]))]
   
(dbx/plumatic-schema xray)
;; =>
[(s/defschema User
   {(s/required-key :users/id)       s/Int
    (s/required-key :users/username) s/Str})
 (s/defschema TodoList
   {(s/required-key :todo_lists/id)      s/Int
    (s/required-key :todo_lists/user_id) s/Int
    (s/required-key :todo_lists/name)    s/Str})
 (s/defschema Todo
   {(s/required-key :todos/id)           s/Int
    (s/required-key :todos/todo_list_id) s/Int
    (s/required-key :todos/description)  s/Str})]

(dbx/datapotato-schema xray)
;; =>
{:users      {:prefix :u}
 :todo_lists {:prefix    :tl
              :relations #:todo_lists{:user_id [:users :users/id]}}
 :todos      {:prefix    :t
              :relations #:todos{:todo_list_id [:todo_lists :todo_lists/id]}}}
```

If you run this in a REPL you can just copy and paste the results into your
source code.

## Help wanted!

The library has some utility in its current state but has a lot of room for
improvement. I'd love feedback / collaboration on:

* How to represent the metadata. I'm not a database expert and am unsure whether
  the approach taken might lead to problems.
* Adapter implementations. Every vendor has different internal representations
  for their metadata, and dbxray attempts to provide a consistent Clojure
  representation. The translation between vendor and Clojure happens via the
  `adapter*` multimethod, and I've only implemented a couple so far.

Join the [#donut channel in Clojurians
slack](https://clojurians.slack.com/archives/C030C4Z2W0Y) for discussion

## Thanks

Thanks to [Sean Corfield](https://github.com/seancorfield/) for his work on
[next-jdbc](https://github.com/seancorfield/next-jdbc), which this relies on
which helped me understand how to interact with JDBC.

## Contributors

* [Marcela Poffald](https://github.com/mpoffald)
