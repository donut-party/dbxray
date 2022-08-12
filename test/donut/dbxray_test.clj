(ns donut.dbxray-test
  (:require
   [clojure.test :refer [deftest is use-fixtures testing]]
   [donut.dbxray :as dbx]
   [next.jdbc :as jdbc])
  (:import
   (io.zonky.test.db.postgres.embedded EmbeddedPostgres)))

(def test-dbconn (atom nil))
(defonce embedded-pg (future (EmbeddedPostgres/start)))

(def ^:private test-postgres {:dbtype "embedded-postgres" :dbname "clojure_test"})
(def ^:private test-sqlite-mem {:dbtype "sqlite" :connection-uri "jdbc:sqlite::memory:"})
(def ^:private test-sqlite-fs {:dbtype "sqlite" :dbname "sqlite.db"})

(def test-dbspecs
  [test-postgres
   test-sqlite-mem
   test-sqlite-fs])

(defn build-stmt
  "helper to vary sql by db type"
  [conn stmt-parts]
  (let [dbtype (dbx/database-product-name (.getMetaData conn))]
    [(reduce (fn [s part]
               (cond
                 (string? part)                   (str s part)
                 ((:exclude (first part)) dbtype) s
                 :else                            (str s (second part))))
             ""
             stmt-parts)]))

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
     "  id integer PRIMARY KEY NOT NULL,"
     "  username text NOT NULL"
     ")"]
    ["CREATE TABLE todo_lists ("
     "  id integer PRIMARY KEY NOT NULL,"
     "  created_by_id INTEGER,"
     "  FOREIGN KEY(created_by_id)"
     "    REFERENCES users(id)"
     ")"]
    ["CREATE TABLE todos ("
     "   id integer PRIMARY KEY NOT NULL,"
     "   todo_list_id INTEGER,"
     "   todo_title text NOT NULL,"
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

(deftest parse-foreign-keys
  (is (= {[:x :y] [:t1 :a :b]}
         (#'dbx/parse-foreign-keys [{:fkcolumn_name "x",
                                     :fktable_schem "public",
                                     :pk_name "t1_pkey",
                                     :fktable_cat nil,
                                     :pktable_name "t1",
                                     :delete_rule 3,
                                     :deferrability 7,
                                     :key_seq 1,
                                     :pkcolumn_name "a",
                                     :fktable_name "t2",
                                     :pktable_cat nil,
                                     :update_rule 3,
                                     :pktable_schem "public",
                                     :fk_name "t2_x_y_fkey"}
                                    {:fkcolumn_name "y",
                                     :fktable_schem "public",
                                     :pk_name "t1_pkey",
                                     :fktable_cat nil,
                                     :pktable_name "t1",
                                     :delete_rule 3,
                                     :deferrability 7,
                                     :key_seq 2,
                                     :pkcolumn_name "b",
                                     :fktable_name "t2",
                                     :pktable_cat nil,
                                     :update_rule 3,
                                     :pktable_schem "public",
                                     :fk_name "t2_x_y_fkey"}]))))

(deftest returns-tables
  (is (= {:users      {:columns      {:id       {:type         :integer
                                                 :nullable?    false
                                                 :primary-key? true}
                                      :username {:type         :text
                                                 :nullable?    false
                                                 :primary-key? false}}
                       :foreign-keys {}}
          :todos      {:columns      {:id            {:type         :integer
                                                      :nullable?    false
                                                      :primary-key? true}
                                      :todo_list_id  {:type         :integer
                                                      :nullable?    true
                                                      :primary-key? false}
                                      :todo_title    {:type         :text
                                                      :nullable?    false
                                                      :primary-key? false}
                                      :created_by_id {:type         :integer
                                                      :nullable?    true
                                                      :primary-key? false}}
                       :foreign-keys {[:todo_list_id]  [:todo_lists :id]
                                      [:created_by_id] [:users :id]}}
          :todo_lists {:columns      {:id            {:type         :integer
                                                      :primary-key? true
                                                      :nullable?    false}
                                      :created_by_id {:type         :integer
                                                      :nullable?    true
                                                      :primary-key? false}}
                       :foreign-keys {[:created_by_id] [:users :id]}}}
         (dbx/xray @test-dbconn))))

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
    (dbx/get-columns (dbx/prep conn) "todos"))

  (dbx/get-columns (dbx/prep epg-conn) "todos")

  (dbx/explore [md pg-conn]
               (df/datafy (.getImportedKeys md nil nil "todos")))
  )
