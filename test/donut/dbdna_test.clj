(ns donut.dbdna-test
  (:require
   [clojure.string :as str]
   [clojure.test :refer [deftest is testing use-fixtures]]
   [donut.dbdna :as dbd]
   [matcher-combinators.test]
   [next.jdbc :as jdbc])
  (:import
   (io.zonky.test.db.postgres.embedded EmbeddedPostgres)))

(def test-dbconn (atom nil))
(defonce embedded-pg (future (EmbeddedPostgres/start)))

(def ^:private test-postgres {:dbtype "embedded-postgres" :dbname "dbdna_test"})
(def ^:private test-sqlite-mem {:dbtype "sqlite" :connection-uri "jdbc:sqlite::memory:"})
(def ^:private test-sqlite-fs {:dbtype "sqlite" :dbname "sqlite.db"})
(def ^:private test-mysql {:dbtype "mysql" :dbname "dbdna_test" :user "root"})

(def ^:dynamic *dbtype*)

(def test-dbspecs
  [test-postgres
   test-sqlite-mem
   test-sqlite-fs
   test-mysql])

(defn build-stmt
  "helper to vary sql by db type"
  [stmt-parts]
  [(->> stmt-parts
        (filter (fn [part]
                  (or (string? part)
                      (let [[rule dbtypes] part]
                        (or (and (= :exclude rule) (not (dbtypes *dbtype*)))
                            (and (= :include rule) (dbtypes *dbtype*)))))))
        (map (fn [part] (if (string? part) part (last part))))
        (str/join ""))])

(defn execute-many!
  [conn stmts]
  (doseq [stmt stmts]
    (jdbc/execute! conn (build-stmt stmt))))

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
     "   completed_at TIMESTAMP NULL,"
     "   FOREIGN KEY(todo_list_id)"
     "     REFERENCES todo_lists(id)"
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
    (binding [*dbtype* (dbd/database-product-name (.getMetaData @test-dbconn))]
      (try
        (create-tables @test-dbconn)
        (testing (str "db: " db)
          (t))
        (finally (.close @test-dbconn))))))

(use-fixtures :each with-test-db)

(defn match-pair
  [result-name [dbtype match]]
  `(when (= *dbtype* ~dbtype)
     (is (~'match? ~match ~result-name))))

(defmacro match-when-dbtype
  [result & pairs]
  (let [result-name (gensym result)]
    `(let [~result-name ~result]
       ~@(map #(match-pair result-name %) pairs))))

(deftest returns-tables
  (let [dna (dbd/dna @test-dbconn)]
    (is (match? {:users      {:columns {:id       {:column-type  :integer
                                                   :primary-key? true
                                                   :unique?      true}
                                        :username {:column-type :varchar
                                                   :unique?     true}}}
                 :todo_lists {:columns {:id            {:column-type  :integer
                                                        :primary-key? true
                                                        :unique?      true}
                                        :created_by_id {:column-type :integer
                                                        :nullable?   true
                                                        :refers-to   [:users :id]}}}
                 :todos      {:columns {:id           {:column-type  :integer
                                                       :primary-key? true
                                                       :unique?      true}
                                        :todo_list_id {:column-type :integer
                                                       :nullable?   true
                                                       :refers-to   [:todo_lists :id]}
                                        :todo_title   {:column-type :varchar}
                                        :notes        {:column-type :text
                                                       :nullable?   true}
                                        :completed_at {:column-type :timestamp
                                                       :nullable?   true}}}}
                dna))

    (match-when-dbtype
     dna
     [:postgresql {:users      {:columns {:id       {:raw-column-type "int4"}
                                          :username {:raw-column-type "varchar"}}}
                   :todo_lists {:columns {:id            {:raw-column-type "int4"}
                                          :created_by_id {:raw-column-type "int4"}}}
                   :todos      {:columns {:id           {:raw-column-type "int4"}
                                          :todo_list_id {:raw-column-type "int4"}
                                          :todo_title   {:raw-column-type "varchar"}
                                          :notes        {:raw-column-type "text"}
                                          :completed_at {:raw-column-type "timestamp"}}}}]

     [:mysql {:users      {:columns {:id       {:raw-column-type "INT"}
                                     :username {:raw-column-type "VARCHAR"}}}
              :todo_lists {:columns {:id            {:raw-column-type "INT"}
                                     :created_by_id {:raw-column-type "INT"}}}
              :todos      {:columns {:id           {:raw-column-type "INT"}
                                     :todo_list_id {:raw-column-type "INT"}
                                     :todo_title   {:raw-column-type "VARCHAR"}
                                     :notes        {:raw-column-type "TEXT"}
                                     :completed_at {:raw-column-type "TIMESTAMP"}}}}]

     [:sqlite {:users      {:columns {:id       {:raw-column-type "INTEGER"}
                                      :username {:raw-column-type "VARCHAR(256)"}}}
               :todo_lists {:columns {:id            {:raw-column-type "INTEGER"}
                                      :created_by_id {:raw-column-type "INTEGER"}}}
               :todos      {:columns {:id           {:raw-column-type "INTEGER"}
                                      :todo_list_id {:raw-column-type "INTEGER"}
                                      :todo_title   {:raw-column-type "VARCHAR(256)"}
                                      :notes        {:raw-column-type "TEXT"}
                                      :completed_at {:raw-column-type "TIMESTAMP"}}}}])))

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
