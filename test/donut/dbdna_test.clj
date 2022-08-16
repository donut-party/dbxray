(ns donut.dbdna-test
  (:require
   [clojure.test :refer [deftest is use-fixtures testing]]
   [donut.dbdna :as dbd]
   [next.jdbc :as jdbc]
   [clojure.string :as str])
  (:import
   (io.zonky.test.db.postgres.embedded EmbeddedPostgres)))

(def test-dbconn (atom nil))
(defonce embedded-pg (future (EmbeddedPostgres/start)))

(def ^:private test-postgres {:dbtype "embedded-postgres" :dbname "dbdna_test"})
(def ^:private test-sqlite-mem {:dbtype "sqlite" :connection-uri "jdbc:sqlite::memory:"})
(def ^:private test-sqlite-fs {:dbtype "sqlite" :dbname "sqlite.db"})
(def ^:private test-mysql {:dbtype "mysql" :dbname "dbdna_test" :user "root"})


(def test-dbspecs
  [test-postgres
   test-sqlite-mem
   test-sqlite-fs
   test-mysql])

(defn build-stmt
  "helper to vary sql by db type"
  [conn stmt-parts]
  (let [dbtype (dbd/database-product-name (.getMetaData conn))]
    [(->> stmt-parts
          (filter (fn [part]
                    (or (string? part)
                        (let [[rule dbtypes] part]
                          (or (and (= :exclude rule) (not (dbtypes dbtype)))
                              (and (= :include rule) (dbtypes dbtype)))))))
          (map (fn [part] (if (string? part) part (last part))))
          (str/join ""))]))

(defn execute-many!
  [conn stmts]
  (doseq [stmt stmts]
    (jdbc/execute! conn (build-stmt conn stmt))))

(defn create-tables
  [conn]
  (doseq [table-name ["todos" "todo_lists" "users" #_#_"t2" "t1"]]
    (try
      (jdbc/execute! conn [(str "DROP TABLE " table-name)])
      (catch Exception _)))

  (execute-many!
   conn
   [["CREATE TABLE users ("
     "  id integer PRIMARY KEY NOT NULL UNIQUE,"
     "  username varchar(256) NOT NULL UNIQUE"
     ")"]
    ["CREATE TABLE todo_lists ("
     "  id integer PRIMARY KEY NOT NULL UNIQUE,"
     "  created_by_id INTEGER,"
     "  FOREIGN KEY(created_by_id)"
     "    REFERENCES users(id)"
     ")"]
    ["CREATE TABLE todos ("
     "   id integer PRIMARY KEY NOT NULL UNIQUE,"
     "   todo_list_id INTEGER,"
     "   todo_title varchar(256) NOT NULL,"
     "   notes text,"
     "   created_by_id INTEGER,"
     "   FOREIGN KEY(todo_list_id)"
     "     REFERENCES todo_lists(id),"
     "   FOREIGN KEY(created_by_id)"
     "     REFERENCES users(id)"
     ")"]]))

(defn create-multi-fk-tables
  [conn]
  (doseq [table-name ["t2" "t1"]]
    (try
      (jdbc/execute! conn [(str "DROP TABLE " table-name)])
      (catch Exception _)))
  (execute-many!
   conn
   [["CREATE TABLE t1 ("
     "   a INTEGER,"
     "   b INTEGER,"
     "   c INTEGER,"
     "   PRIMARY KEY (a, b)"
     ")"]
    ["CREATE TABLE t2 ("
     "   x INTEGER PRIMARY KEY,"
     "   y INTEGER,"
     "   z INTEGER,"
     "   CONSTRAINT \"foo\" FOREIGN KEY (x, y) REFERENCES t1 (a, b)"
     ")"]]))

(defn with-test-db
  [t]
  (doseq [db test-dbspecs]
    (if (= "embedded-postgres" (:dbtype db))
      (reset! test-dbconn (jdbc/get-connection (.getPostgresDatabase ^EmbeddedPostgres @embedded-pg)))
      (reset! test-dbconn (jdbc/get-connection db)))
    (try
      (create-tables @test-dbconn)
      (testing (str "db: " db)
        (t))
      (finally (.close @test-dbconn)))))

(use-fixtures :each with-test-db)

(deftest returns-tables
  (is (= {:users      {:columns      {:id       {:column-type  :integer
                                                 :primary-key? true
                                                 :unique?      true}
                                      :username {:column-type :varchar
                                                 :unique?     true}}
                       :column-order [:id :username]}
          :todo_lists {:columns      {:id            {:column-type  :integer
                                                      :primary-key? true
                                                      :unique?      true}
                                      :created_by_id {:column-type :integer
                                                      :nullable?   true
                                                      :refers-to   [:users :id]}}
                       :column-order [:id :created_by_id]}

          :todos {:columns      {:id            {:column-type  :integer
                                                 :primary-key? true
                                                 :unique?      true}
                                 :todo_list_id  {:column-type :integer
                                                 :nullable?   true
                                                 :refers-to   [:todo_lists :id]}
                                 :todo_title    {:column-type :varchar}
                                 :notes         {:column-type :text
                                                 :nullable?   true}
                                 :created_by_id {:column-type :integer
                                                 :nullable?   true
                                                 :refers-to   [:users :id]}}
                  :column-order [:id :todo_list_id :todo_title :notes :created_by_id]}}
         (dbd/dna @test-dbconn))))

(comment
  (do
    (require '[clojure.datafy :as df])
    (require '[next.jdbc.result-set :as njrs])
    (def epg-conn (jdbc/get-connection (.getPostgresDatabase ^EmbeddedPostgres @embedded-pg)))
    (create-tables epg-conn)
    (def pg-conn (jdbc/get-connection {:dbtype "postgresql" :dbname "daniel" :user "daniel" :password ""}))
    (def sl-conn (jdbc/get-connection {:dbtype "sqlite" :dbname "sqlite.db"}))
    )

  (with-open [conn (jdbc/get-connection {:dbtype "sqlite" :dbname "sqlite.db"})]
    (create-tables conn)
    (dbd/get-columns (dbd/prep conn) "todos"))

  (dbd/get-columns (dbd/prep epg-conn) "todos")

  (dbd/explore [md pg-conn]
               (.getImportedKeys md nil nil "todos"))
  )
