(ns donut.dbxray-test
  (:require
   [clojure.test :refer [deftest is use-fixtures]]
   [donut.dbxray :as dbx]
   [next.jdbc :as jdbc])
  (:import
   (io.zonky.test.db.postgres.embedded EmbeddedPostgres)))

(def test-dbspec (atom nil))
(defonce embedded-pg (future (EmbeddedPostgres/start)))

(def ^:private test-postgres {:dbtype "embedded-postgres" :dbname "clojure_test"})
(def ^:private test-sqlite-mem {:dbtype "sqlite" :connection-uri "jdbc:sqlite::memory:"})
(def ^:private test-sqlite-fs {:dbtype "sqlite" :dbname "sqlite.db"})

(def test-dbspecs
  [test-postgres
   #_test-sqlite-mem
   test-sqlite-fs])

(defn create-tables
  [conn]
  (doseq [table-name ["users" "todo_lists" "todos"]]
    (try
      (jdbc/execute! conn [(str "DROP TABLE " table-name)])
      (catch Exception _)))

  (jdbc/execute!
   conn
   ["CREATE TABLE users (
       id integer PRIMARY KEY,
       username text NOT NULL
    )"])

  (jdbc/execute!
   conn
   ["CREATE TABLE todo_lists (
       id integer PRIMARY KEY,
       created_by_id INTEGER,
       updated_by_id INTEGER
    )"])

  (jdbc/execute!
   conn
   ["CREATE TABLE todos (
       id integer PRIMARY KEY,
       todo_list_id INTEGER,
       todo_title text NOT NULL,
       created_by_id INTEGER,
       updated_by_id INTEGER
       CONSTRAINT fk_todo_list
         FOREIGN KEY(todo_list_id)
           REFERENCES todo_lists(id)
    )"])
  )

(defn with-test-db
  [t]
  (doseq [db test-dbspecs]
    (if (= "embedded-postgres" (:dbtype db))
      (reset! test-dbspec
              (.getPostgresDatabase ^EmbeddedPostgres @embedded-pg))
      (reset! test-dbspec db))
    (create-tables @test-dbspec)
    (t)))

(use-fixtures :each with-test-db)

(deftest returns-tables
  (is (= {:users      {:columns {:id       {:type         :integer
                                            :not-null?    true
                                            :primary-key? true}
                                 :username {:type         :text
                                            :not-null?    true
                                            :primary-key? false}}}
          :todos      {:columns {:id            {:type         :integer
                                                 :not-null?    true
                                                 :primary-key? true}
                                 :todo_list_id  {:type         :integer
                                                 :not-null?    false
                                                 :primary-key? false}
                                 :todo_title    {:type         :text
                                                 :not-null?    true
                                                 :primary-key? false}
                                 :created_by_id {:type         :integer
                                                 :not-null?    false
                                                 :primary-key? false}
                                 :updated_by_id {:type         :integer
                                                 :not-null?    false
                                                 :primary-key? false}}}
          :todo_lists {:columns {:id            {:type         :integer
                                                 :primary-key? true
                                                 :not-null?    true}
                                 :created_by_id {:type         :integer
                                                 :not-null?    false
                                                 :primary-key? false}
                                 :updated_by_id {:type         :integer
                                                 :not-null?    false
                                                 :primary-key? false}}}}
         #_(dbxray/tables {:dbtype     :postgres
                           :connection @test-dbspec}))))

(comment
  (require '[next.jdbc.sql :as sql])
  (require '[clojure.datafy :as df])
  (def pg-conn (.getPostgresDatabase ^EmbeddedPostgres @embedded-pg))
  (create-tables pg-conn)



  (-> pg-conn
      jdbc/get-connection
      .getMetaData
      (.getTables nil nil "users" nil)
      clojure.data/datafy
      )

  ;; pg all public tables, includes indexes
  (-> pg-conn
      jdbc/get-connection
      .getMetaData
      (.getTables nil "public" nil nil)
      clojure.data/datafy
      )

  ;; columns
  (-> pg-conn
      jdbc/get-connection
      .getMetaData
      (.getColumns nil "public" nil nil)
      clojure.data/datafy
      :rows
      )

  (with-open [conn (jdbc/get-connection test-sqlite-mem)]
    (create-tables conn)
    (-> conn
        .getMetaData
        (.getTables nil nil nil nil)
        clojure.datafy/datafy))
  )
