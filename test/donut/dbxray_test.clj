(ns donut.dbxray-test
  (:require
   [clojure.string :as str]
   [clojure.test :refer [deftest is testing]]
   [donut.dbxray :as dbx]
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
    (is (= (deep-merge typical-core-result result-extras)
           (dbx/xray *dbconn*)))))


;;---
;; tests
;;---

(deftest postgresql-test
  (assert-vendor-data-matches
   test-postgres
   {:parent_records {:columns {:id           {:raw-column-type "int4"}
                               :varchar_ex   {:raw-column-type "varchar"}
                               :text_ex      {:raw-column-type "text"}
                               :timestamp_ex {:raw-column-type "timestamp"}}}
    :child_records  {:columns {:id    {:raw-column-type "int4"}
                               :fk_id {:raw-column-type "int4"}}}}))

(deftest mysql-test
  (assert-vendor-data-matches
   test-mysql
   {:parent_records {:columns {:id           {:raw-column-type "INT"}
                               :varchar_ex   {:raw-column-type "VARCHAR"}
                               :text_ex      {:raw-column-type "TEXT"}
                               :timestamp_ex {:raw-column-type "TIMESTAMP"}}}
    :child_records  {:columns {:id    {:raw-column-type "INT"}
                               :fk_id {:raw-column-type "INT"}}}}))

(deftest sqlite-test
  (assert-vendor-data-matches
   test-sqlite-fs
   {:parent_records {:columns {:id           {:raw-column-type "INTEGER"}
                               :varchar_ex   {:raw-column-type "VARCHAR(256)"}
                               :text_ex      {:raw-column-type "TEXT"}
                               :timestamp_ex {:raw-column-type "TIMESTAMP"}}}
    :child_records  {:columns {:id    {:raw-column-type "INTEGER"}
                               :fk_id {:raw-column-type "INTEGER"}}}})

  (assert-vendor-data-matches
   test-sqlite-mem
   {:parent_records {:columns {:id           {:raw-column-type "INTEGER"}
                               :varchar_ex   {:raw-column-type "VARCHAR(256)"}
                               :text_ex      {:raw-column-type "TEXT"}
                               :timestamp_ex {:raw-column-type "TIMESTAMP"}}}
    :child_records  {:columns {:id    {:raw-column-type "INTEGER"}
                               :fk_id {:raw-column-type "INTEGER"}}}}))

(deftest h2-test
  (with-test-db test-h2
    (is (= {:PARENT_RECORDS {:columns {:ID           {:column-type     :integer
                                                      :primary-key?    true
                                                      :unique?         true
                                                      :raw-column-type "INTEGER"}
                                       :VARCHAR_EX   {:column-type     :varchar
                                                      :unique?         true
                                                      :raw-column-type "VARCHAR"}
                                       :TEXT_EX      {:column-type     :clob
                                                      :nullable?       true
                                                      :raw-column-type "CLOB"}
                                       :TIMESTAMP_EX {:column-type     :timestamp
                                                      :nullable?       true
                                                      :raw-column-type "TIMESTAMP"}}}
            :CHILD_RECORDS  {:columns {:ID    {:column-type     :integer
                                               :primary-key?    true
                                               :unique?         true
                                               :raw-column-type "INTEGER"}
                                       :FK_ID {:column-type     :integer
                                               :refers-to       [:PARENT_RECORDS :ID]
                                               :raw-column-type "INTEGER"}}}}
           (dbx/xray *dbconn*)))))

(deftest hsql-test
  (with-test-db test-hsql
    (is (= {:PARENT_RECORDS {:columns {:ID           {:column-type     :integer
                                                      :primary-key?    true
                                                      :unique?         true
                                                      :raw-column-type "INTEGER"}
                                       :VARCHAR_EX   {:column-type     :varchar
                                                      :unique?         true
                                                      :raw-column-type "VARCHAR"}
                                       :TEXT_EX      {:column-type     :clob
                                                      :nullable?       true
                                                      :raw-column-type "CLOB"}
                                       :TIMESTAMP_EX {:column-type     :timestamp
                                                      :nullable?       true
                                                      :raw-column-type "TIMESTAMP"}}}
            :CHILD_RECORDS  {:columns {:ID    {:column-type     :integer
                                               :primary-key?    true
                                               :unique?         true
                                               :raw-column-type "INTEGER"}
                                       :FK_ID {:column-type     :integer
                                               :refers-to       [:PARENT_RECORDS :ID]
                                               :raw-column-type "INTEGER"}}}}
           (dbx/xray *dbconn*)))))
