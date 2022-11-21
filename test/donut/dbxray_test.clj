(ns donut.dbxray-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [donut.dbxray :as dbx]
   [matcher-combinators.test] ;; adds support for `match?` and `thrown-match?` in `is` expressions
   [next.jdbc :as jdbc])
  (:import
   (io.zonky.test.db.postgres.embedded EmbeddedPostgres)))

(defonce embedded-pg (future (EmbeddedPostgres/start)))
(def ^:dynamic *dbtype*)
(def ^:dynamic *dbconn*)

;;---
;; helpers
;;---

(defn execute-many!
  [conn stmts]
  (doseq [stmt stmts]
    (jdbc/execute! conn [stmt])))

(defn create-tables
  [conn create-table-statements]
  (doseq [table-name ["child_records" "parent_records" "unconnected_records"]]
    (try
      (jdbc/execute! conn [(str "DROP TABLE " table-name)])
      (catch Exception _)))
  (execute-many! conn create-table-statements))

(defmacro with-test-db
  [test-db & body]
  `(let [test-db-def# ~test-db
         test-db#     test-db-def#]
     (with-open [test-db# (if (= "embedded-postgres" (:dbtype test-db#))
                            (jdbc/get-connection (.getPostgresDatabase ^EmbeddedPostgres @embedded-pg))
                            (jdbc/get-connection test-db#))]
       (binding [*dbtype* (dbx/database-product-name (.getMetaData test-db#))
                 *dbconn* test-db#]
         (create-tables test-db# (:create-tables test-db-def#))
         (testing (str "db: " test-db#)
           ~@body)))))

;; Copied verbatim from the defunct clojure-contrib (http://bit.ly/deep-merge-with)
(defn deep-merge-with [f & maps]
  (apply
   (fn m [& maps]
     (if (every? map? maps)
       (apply merge-with m maps)
       (apply f maps)))
   maps))

(def deep-merge (partial deep-merge-with merge))

(def typical-core-result
  "Most vendors return something that looks like this"
  {:tables
   {:parent_records {:columns
                     {:id           {:column-type    :integer
                                     :primary-key?   true
                                     :unique?        true
                                     :autoincrement? true}
                      :varchar_ex   {:column-type :varchar
                                     :unique?     true}
                      :text_ex      {:column-type :text
                                     :nullable?   true}
                      :timestamp_ex {:column-type :timestamp
                                     :nullable?   true}}
                     :column-order
                     [:id :varchar_ex :text_ex :timestamp_ex]}
    :child_records  {:columns
                     {:id    {:column-type  :integer
                              :primary-key? true
                              :unique?      true}
                      :fk_id {:column-type :integer
                              :refers-to   [:parent_records :id]}}

                     :column-order
                     [:id :fk_id]}}

   :table-order
   [:unconnected_records :parent_records :child_records]})

(defn assert-vendor-data-matches
  [test-db result-extras]
  (with-test-db test-db
    (is (match? (deep-merge typical-core-result result-extras)
                (dbx/xray *dbconn*)))))


;;---
;; tests
;;---

;;---
;; postgres
;;---

(def ^:private test-postgres
  {:dbtype        "embedded-postgres"
   :dbname        "dbxray_test"
   :create-tables [(str "CREATE TABLE parent_records ("
                        "  id           serial NOT NULL PRIMARY KEY UNIQUE,"
                        "  varchar_ex   varchar(256) NOT NULL UNIQUE,"
                        "  text_ex      text,"
                        "  timestamp_ex TIMESTAMP NULL"
                        ")")
                   (str "CREATE TABLE child_records ("
                        "  id    integer PRIMARY KEY NOT NULL UNIQUE,"
                        "  fk_id integer NOT NULL,"
                        "  FOREIGN KEY(fk_id)"
                        "    REFERENCES parent_records(id)"
                        ")")
                   (str "CREATE TABLE unconnected_records ("
                        " id integer PRIMARY KEY NOT NULL UNIQUE"
                        ")")]})

(deftest postgresql-test
  (assert-vendor-data-matches
   test-postgres
   {:tables {:parent_records {:columns {:id           {:raw {:type_name   "serial"
                                                             :column_name "id"
                                                             :is_nullable "NO"}}
                                        :varchar_ex   {:raw {:type_name   "varchar"
                                                             :column_name "varchar_ex"
                                                             :is_nullable "NO"}}
                                        :text_ex      {:raw {:type_name   "text"
                                                             :column_name "text_ex"
                                                             :is_nullable "YES"}}
                                        :timestamp_ex {:raw {:type_name   "timestamp"
                                                             :column_name "timestamp_ex"
                                                             :is_nullable "YES"}}}}
             :child_records  {:columns {:id    {:raw {:type_name   "int4"
                                                      :column_name "id"
                                                      :is_nullable "NO"}}
                                        :fk_id {:raw {:type_name   "int4"
                                                      :column_name "fk_id"
                                                      :is_nullable "NO"}}}}}}))

;;---
;; mysql
;;---

(def ^:private test-mysql
  {:dbtype        "mysql"
   :dbname        "dbxray_test"
   :user          "root"
   :password      (System/getenv "MYSQL_ROOT_PASSWORD")
   :create-tables [(str "CREATE TABLE parent_records ("
                        "  id           integer NOT NULL PRIMARY KEY UNIQUE AUTO_INCREMENT,"
                        "  varchar_ex   varchar(256) NOT NULL UNIQUE,"
                        "  text_ex      text,"
                        "  timestamp_ex TIMESTAMP NULL"
                        ")")
                   (str "CREATE TABLE child_records ("
                        "  id    integer PRIMARY KEY NOT NULL UNIQUE,"
                        "  fk_id integer NOT NULL,"
                        "  FOREIGN KEY(fk_id)"
                        "    REFERENCES parent_records(id)"
                        ")")
                   (str "CREATE TABLE unconnected_records ("
                        " id integer PRIMARY KEY NOT NULL UNIQUE"
                        ")")]})

(deftest mysql-test
  (assert-vendor-data-matches
   test-mysql
   {:tables
    {:parent_records {:columns {:id           {:raw {:type_name   "INT"
                                                     :column_name "id"
                                                     :is_nullable "NO"}}
                                :varchar_ex   {:raw {:type_name   "VARCHAR"
                                                     :column_name "varchar_ex"
                                                     :is_nullable "NO"}}
                                :text_ex      {:raw {:type_name   "TEXT"
                                                     :column_name "text_ex"
                                                     :is_nullable "YES"}}
                                :timestamp_ex {:raw {:type_name   "TIMESTAMP"
                                                     :column_name "timestamp_ex"
                                                     :is_nullable "YES"}}}}
     :child_records  {:columns {:id    {:raw {:type_name   "INT"
                                              :column_name "id"
                                              :is_nullable "NO"}}
                                :fk_id {:raw {:type_name   "INT"
                                              :column_name "fk_id"
                                              :is_nullable "NO"}}}}}}))

;;---
;; sqlite
;;---

(def sqlite-create-tables
  [(str "CREATE TABLE parent_records ("
        "  id           integer NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,"
        "  varchar_ex   varchar(256) NOT NULL UNIQUE,"
        "  text_ex      text,"
        "  timestamp_ex TIMESTAMP NULL"
        ")")
   (str "CREATE TABLE child_records ("
        "  id    integer PRIMARY KEY NOT NULL UNIQUE,"
        "  fk_id integer NOT NULL,"
        "  FOREIGN KEY(fk_id)"
        "    REFERENCES parent_records(id)"
        ")")
   (str "CREATE TABLE unconnected_records ("
        " id integer PRIMARY KEY NOT NULL UNIQUE"
        ")")])

(def ^:private test-sqlite-mem
  {:dbtype         "sqlite"
   :connection-uri "jdbc:sqlite::memory:"
   :create-tables  sqlite-create-tables})

(def ^:private test-sqlite-fs
  {:dbtype        "sqlite"
   :dbname        "sqlite.db"
   :create-tables sqlite-create-tables})

(def ^:private sqlite-result
  {:tables
   {:parent_records {:columns {:id           {:raw {:type_name   "INTEGER"
                                                    :column_name "id"
                                                    :is_nullable "NO"}}
                               :varchar_ex   {:raw {:type_name   "VARCHAR(256)"
                                                    :column_name "varchar_ex"
                                                    :is_nullable "NO"}}
                               :text_ex      {:raw {:type_name   "TEXT"
                                                    :column_name "text_ex"
                                                    :is_nullable "YES"}}
                               :timestamp_ex {:raw {:type_name   "TIMESTAMP"
                                                    :column_name "timestamp_ex"
                                                    :is_nullable "YES"}}}}
    :child_records  {:columns {:id    {:raw {:type_name   "INTEGER"
                                             :column_name "id"
                                             :is_nullable "NO"}}
                               :fk_id {:raw {:type_name   "INTEGER"
                                             :column_name "fk_id"
                                             :is_nullable "NO"}}}}}})

(deftest sqlite-test
  (assert-vendor-data-matches test-sqlite-fs sqlite-result)
  (assert-vendor-data-matches test-sqlite-mem sqlite-result))

;;---
;; h2
;;---

(def ^:private test-h2
  {:dbtype        "h2"
   :dbname        "dbxray_test"
   :user          "root"
   :create-tables [(str "CREATE TABLE parent_records ("
                        "  id           integer NOT NULL IDENTITY PRIMARY KEY,"
                        "  varchar_ex   varchar(256) NOT NULL,"
                        "  text_ex      text,"
                        "  timestamp_ex TIMESTAMP NULL,"
                        "  UNIQUE(id),"
                        "  UNIQUE(varchar_ex)"
                        ")")
                   (str "CREATE TABLE child_records ("
                        "  id    integer PRIMARY KEY NOT NULL,"
                        "  fk_id integer NOT NULL,"
                        "  FOREIGN KEY(fk_id)"
                        "    REFERENCES parent_records(id),"
                        "  UNIQUE(id)"
                        ")")
                   (str "CREATE TABLE unconnected_records ("
                        " id integer PRIMARY KEY NOT NULL"
                        ")")]})

(deftest h2-test
  (with-test-db test-h2
    (is (match?
         {:tables
          {:PARENT_RECORDS
           {:columns {:ID           {:column-type    :integer
                                     :primary-key?   true
                                     :unique?        true
                                     :autoincrement? true
                                     :raw            {:type_name   "INTEGER"
                                                      :column_name "ID"
                                                      :is_nullable "NO"}}
                      :VARCHAR_EX   {:column-type :varchar
                                     :unique?     true
                                     :raw         {:type_name   "VARCHAR"
                                                   :column_name "VARCHAR_EX"
                                                   :is_nullable "NO"}}
                      :TEXT_EX      {:column-type :clob
                                     :nullable?   true
                                     :raw         {:type_name   "CLOB"
                                                   :column_name "TEXT_EX"
                                                   :is_nullable "YES"}}
                      :TIMESTAMP_EX {:column-type :timestamp
                                     :nullable?   true
                                     :raw         {:type_name   "TIMESTAMP"
                                                   :column_name "TIMESTAMP_EX"
                                                   :is_nullable "YES"}}}}

           :CHILD_RECORDS
           {:columns {:ID    {:column-type  :integer
                              :primary-key? true
                              :unique?      true
                              :raw          {:type_name   "INTEGER"
                                             :column_name "ID"
                                             :is_nullable "NO"}}
                      :FK_ID {:column-type :integer
                              :refers-to   [:PARENT_RECORDS :ID]
                              :raw         {:type_name   "INTEGER"
                                            :column_name "FK_ID"
                                            :is_nullable "NO"}}}}}

          :table-order
          [:UNCONNECTED_RECORDS :PARENT_RECORDS :CHILD_RECORDS]}
         (dbx/xray *dbconn*)))))

;;---
;; hsql
;;---

(def ^:private test-hsql
  {:dbtype        "hsql"
   :dbname        "dbxray_test"
   :user          "root"
   :create-tables [(str "CREATE TABLE parent_records ("
                        "  id           integer IDENTITY PRIMARY KEY,"
                        "  varchar_ex   varchar(256) NOT NULL UNIQUE,"
                        "  text_ex      clob,"
                        "  timestamp_ex TIMESTAMP NULL"
                        ")")
                   (str "CREATE TABLE child_records ("
                        "  id    integer PRIMARY KEY NOT NULL,"
                        "  fk_id integer NOT NULL,"
                        "  FOREIGN KEY(fk_id)"
                        "    REFERENCES parent_records(id)"
                        ")")
                   (str "CREATE TABLE unconnected_records ("
                        " id integer PRIMARY KEY NOT NULL"
                        ")")]})

(deftest hsql-test
  (with-test-db test-hsql
    (is
     (match?
      {:tables
       {:PARENT_RECORDS {:columns {:ID           {:column-type    :integer
                                                  :primary-key?   true
                                                  :unique?        true
                                                  :autoincrement? true
                                                  :raw            {:type_name   "INTEGER"
                                                                   :column_name "ID"
                                                                   :is_nullable "NO"}}
                                   :VARCHAR_EX   {:column-type :varchar
                                                  :unique?     true
                                                  :raw         {:type_name   "VARCHAR"
                                                                :column_name "VARCHAR_EX"
                                                                :is_nullable "NO"}}
                                   :TEXT_EX      {:column-type :clob
                                                  :nullable?   true
                                                  :raw         {:type_name   "CLOB"
                                                                :column_name "TEXT_EX"
                                                                :is_nullable "YES"}}
                                   :TIMESTAMP_EX {:column-type :timestamp
                                                  :nullable?   true
                                                  :raw         {:type_name   "TIMESTAMP"
                                                                :column_name "TIMESTAMP_EX"
                                                                :is_nullable "YES"}}}}
        :CHILD_RECORDS  {:columns {:ID    {:column-type  :integer
                                           :primary-key? true
                                           :unique?      true
                                           :raw          {:type_name   "INTEGER"
                                                          :column_name "ID"
                                                          :is_nullable "NO"}}
                                   :FK_ID {:column-type :integer
                                           :refers-to   [:PARENT_RECORDS :ID]
                                           :raw         {:type_name   "INTEGER"
                                                         :column_name "FK_ID"
                                                         :is_nullable "NO"}}}}}
       :table-order
       [:UNCONNECTED_RECORDS :PARENT_RECORDS :CHILD_RECORDS]}
      (dbx/xray *dbconn*)))))


;;---
;; scratch
;;---

(comment
  "to try out table creation"
  (let [dbconf test-hsql]
    (with-open [conn (jdbc/get-connection dbconf)]
      (create-tables conn (:create-tables dbconf)))))
