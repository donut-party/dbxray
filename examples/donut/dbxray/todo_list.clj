(ns donut.dbxray.todo-list
  (:require
   [donut.dbxray :as dbx]
   [next.jdbc :as jdbc])
  (:import
   (io.zonky.test.db.postgres.embedded EmbeddedPostgres)))

(defonce embedded-pg (delay (EmbeddedPostgres/start)))
(defonce conn (delay (jdbc/get-connection (.getPostgresDatabase ^EmbeddedPostgres @embedded-pg))))

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

(defn execute-many!
  [conn stmts]
  (doseq [stmt stmts]
    (jdbc/execute! conn [stmt])))

(defn create-tables
  [conn create-table-statements]
  (doseq [table-name ["users" "todo_lists" "todos"]]
    (try
      (jdbc/execute! conn [(str "DROP TABLE " table-name)])
      (catch Exception _)))
  (execute-many! conn create-table-statements))


(comment
  (create-tables (jdbc/get-connection (.getPostgresDatabase ^EmbeddedPostgres @embedded-pg))
                 [create-users create-todo-lists create-todos])

  (dbx/xray @conn)

  )
