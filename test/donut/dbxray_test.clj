(ns donut.dbxray-test
  (:require
   [clojure.test :refer [deftest is use-fixtures]]
   [donut.dbxray :as dbxray]
   [next.jdbc :as jdbc])
  (:import
   (io.zonky.test.db.postgres.embedded EmbeddedPostgres)))

(def test-dbspec (atom nil))
(defonce embedded-pg (future (EmbeddedPostgres/start)))

(def ^:private test-postgres {:dbtype "embedded-postgres" :dbname "clojure_test"})
(def ^:private test-sqlite-mem {:dbtype "sqlite" :connection-uri "jdbc:sqlite::memory:"})

(def test-dbspecs
  [test-postgres
   #_test-sqlite-mem])

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
                                            :primary-key? true}
                                 :username {:type      :text
                                            :not-null? true}}}
          :todos      {:columns {:id            {:type         :integer
                                                 :primary-key? true}
                                 :todo_list_id  {:type :integer}
                                 :todo_title    {:type      :next
                                                 :not-null? true}
                                 :created_by_id {:type :integer}
                                 :updated_by_id {:type :integer}}}
          :todo_lists {:columns {:id            {:type         :integer
                                                 :primary-key? true}
                                 :created_by_id {:type :integer}
                                 :updated_by_id {:type :integer}}}}
         (dbxray/tables {:dbtype     :postgres
                         :connection @test-dbspec}))))
