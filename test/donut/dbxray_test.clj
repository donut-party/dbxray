(ns donut.dbxray-test
  (:require
   [clojure.string :as str]
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
;; db defs
;;---

(def typical-create-tables
  [["CREATE TABLE parent_records ("
    "  id           integer NOT NULL PRIMARY KEY UNIQUE,"
    "  varchar_ex   varchar(256) NOT NULL UNIQUE,"
    "  text_ex      text,"
    "  timestamp_ex TIMESTAMP NULL"
    ")"]
   ["CREATE TABLE child_records ("
    "  id    integer PRIMARY KEY NOT NULL UNIQUE,"
    "  fk_id integer NOT NULL,"
    "  FOREIGN KEY(fk_id)"
    "    REFERENCES parent_records(id)"
    ")"]])

(def ^:private test-mysql      {:dbtype "mysql" :dbname "dbxray_test" :user "root" :create-tables typical-create-tables})
(def ^:private test-postgres   {:dbtype "embedded-postgres" :dbname "dbxray_test" :create-tables typical-create-tables})
(def ^:private test-sqlite-mem {:dbtype "sqlite" :connection-uri "jdbc:sqlite::memory:" :create-tables typical-create-tables})
(def ^:private test-sqlite-fs  {:dbtype "sqlite" :dbname "sqlite.db" :create-tables typical-create-tables})


;;---
;; atypical dbs
;; "atypical" is from the perspective of the author, it's not an official database thing.
;; it just means that these dbs are slightly different than the others:
;; - the create table syntax is different enough to be handled separately
;; - the metadata returned is transformed in ways that are different from "typica" dbs, e.g.
;;   column names are all upper-cased

(def ^:private test-h2   {:dbtype        "h2"
                          :dbname        "dbxray_test"
                          :user          "root"
                          :create-tables [["CREATE TABLE parent_records ("
                                           "  id           integer NOT NULL PRIMARY KEY,"
                                           "  varchar_ex   varchar(256) NOT NULL,"
                                           "  text_ex      text,"
                                           "  timestamp_ex TIMESTAMP NULL,"
                                           "  UNIQUE(id),"
                                           "  UNIQUE(varchar_ex)"
                                           ")"]
                                          ["CREATE TABLE child_records ("
                                           "  id    integer PRIMARY KEY NOT NULL,"
                                           "  fk_id integer NOT NULL,"
                                           "  FOREIGN KEY(fk_id)"
                                           "    REFERENCES parent_records(id),"
                                           "  UNIQUE(id)"
                                           ")"]]})
(def ^:private test-hsql {:dbtype        "hsql"
                          :dbname        "dbxray_test"
                          :user          "root"
                          :create-tables [["CREATE TABLE parent_records ("
                                           "  id           integer NOT NULL PRIMARY KEY,"
                                           "  varchar_ex   varchar(256) NOT NULL UNIQUE,"
                                           "  text_ex      clob,"
                                           "  timestamp_ex TIMESTAMP NULL"
                                           ")"]
                                          ["CREATE TABLE child_records ("
                                           "  id    integer PRIMARY KEY NOT NULL,"
                                           "  fk_id integer NOT NULL,"
                                           "  FOREIGN KEY(fk_id)"
                                           "    REFERENCES parent_records(id)"
                                           ")"]]})

;;---
;; helpers
;;---

(defn execute-many!
  [conn stmts]
  (doseq [stmt stmts]
    (jdbc/execute! conn [(str/join "" stmt)])))

(defn create-tables
  [conn create-table-statements]
  (doseq [table-name ["child_records" "parent_records"]]
    (try
      (jdbc/execute! conn [(str "DROP TABLE " table-name)])
      (catch Exception _)))
  (execute-many! conn create-table-statements))

(comment "to try out table creation"
         (let [dbconf test-hsql]
           (with-open [conn (jdbc/get-connection dbconf)]
             (create-tables conn (:create-tables dbconf)))))

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
  {:parent_records {:columns {:id           {:column-type  :integer
                                             :primary-key? true
                                             :unique?      true}
                              :varchar_ex   {:column-type :varchar
                                             :unique?     true}
                              :text_ex      {:column-type :text
                                             :nullable?   true}
                              :timestamp_ex {:column-type :timestamp
                                             :nullable?   true}}}
   :child_records  {:columns {:id    {:column-type  :integer
                                      :primary-key? true
                                      :unique?      true}
                              :fk_id {:column-type :integer
                                      :refers-to   [:parent_records :id]}}}})

(defn assert-vendor-data-matches
  [test-db result-extras]
  (with-test-db test-db
    (is (match? (deep-merge typical-core-result result-extras)
                (dbx/xray *dbconn*)))))


;;---
;; tests
;;---

;;---
;; typical dbs
;;---

(deftest postgresql-test
  (assert-vendor-data-matches
   test-postgres
   {:parent_records {:columns {:id           {:raw {:type_name   "int4"
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
                                             :is_nullable "NO"}}}}}))

(deftest mysql-test
  (assert-vendor-data-matches
   test-mysql
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
                                             :is_nullable "NO"}}}}}))

(deftest sqlite-test
  (assert-vendor-data-matches
   test-sqlite-fs
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
                                             :is_nullable "NO"}}}}})

  (assert-vendor-data-matches
   test-sqlite-mem
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
                                             :is_nullable "NO"}}}}}))

;;---
;; atypical dbs
;;---

(deftest h2-test
  (with-test-db test-h2
    (is
     (match?
      {:PARENT_RECORDS {:columns {:ID           {:column-type  :integer
                                                 :primary-key? true
                                                 :unique?      true
                                                 :raw          {:type_name   "INTEGER"
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
      (dbx/xray *dbconn*)))))

(deftest hsql-test
  (with-test-db test-hsql
    (is
     (match?
      {:PARENT_RECORDS {:columns {:ID           {:column-type  :integer
                                                 :primary-key? true
                                                 :unique?      true
                                                 :raw          {:type_name   "INTEGER"
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
      (dbx/xray *dbconn*)))))
