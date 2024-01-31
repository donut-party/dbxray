(ns donut.dbxray
  (:require
   [clojure.datafy :as df]
   [clojure.string :as str]
   [donut.dbxray.generate.spec :as clojure-spec]
   [donut.dbxray.generate.plumatic-schema :as plumatic-schema]
   [donut.dbxray.generate.malli :as malli]
   [donut.dbxray.generate.datapotato :as datapotato]
   [next.jdbc :as jdbc]
   [next.jdbc.datafy :as njdf]
   [next.jdbc.result-set :as njrs]
   [weavejester.dependency :as dep]
   [clojure.set :as set])
  (:import (java.util.regex Pattern)))

(def default-adapter
  {:schema-pattern nil
   :column-types   {#"int"      :integer
                    #"varchar"  :varchar
                    #"date"     :date
                    #"datetime" :datetime}
   :predicates     {:nullable?      (fn [{:keys [is_nullable] :as _raw}]
                                      (= "YES" is_nullable))
                    :unique?        (fn [{:keys [indexes] :as _raw}]
                                      (->> indexes
                                           (filter (complement :non_unique))
                                           seq))
                    :autoincrement? (fn [{:keys [is_autoincrement] :as _raw}]
                                      (= "YES" is_autoincrement))}})

(defmulti adapter* :dbtype)

(defmethod adapter* :postgresql
  [_]
  {:schema-pattern "public"
   :column-types   {#"serial" :integer
                    #"date"   :date}})


(defmethod adapter* :oracle
  [_]
  {:column-types {#"number"  :integer}})

(defmethod adapter* :sqlite
  [_]
  {:predicates {:unique? (fn [{:keys [indexes] :as _raw}]
                           (->> indexes
                                (filter #(= 0 (:non_unique %)))
                                seq))}})

(defmethod adapter* :default
  [_]
  nil)

(defn adapter
  [dbmd]
  (let [{:keys [schema-pattern column-types predicates]} (adapter* dbmd)]
    {:schema-pattern (or schema-pattern (:schema-pattern default-adapter))
     :column-types   (merge (:column-types default-adapter) column-types)
     :predicates     (merge (:predicates default-adapter) predicates)}))

(defn database-product-name
  [metadata]
  (-> metadata
      .getDatabaseProductName
      str/lower-case
      keyword))

(defn prep
  "returns metadata needed to construct xray"
  [conn & [adapter-opts]]
  (let [metadata (.getMetaData conn)
        dbtype   (database-product-name metadata)
        dbmd     {:metadata     metadata
                  :dbtype       dbtype
                  :catalog      (-> metadata .getConnection .getCatalog)
                  :include-raw? (:include-raw? adapter-opts)}]
    (assoc dbmd :dbadapter (merge (adapter dbmd) adapter-opts))))

(defn datafy-result-set
  [rs]
  (-> rs
      (njrs/datafiable-result-set nil {:builder-fn njrs/as-unqualified-lower-maps})
      df/datafy))

;; see https://docs.oracle.com/javase/8/docs/api/java/sql/DatabaseMetaData.html

(defn get-index-info
  [{:keys [metadata dbadapter catalog]} & [table-name]]
  (binding [njdf/*datafy-failure* :omit]
    (-> metadata
        (.getIndexInfo catalog (:schema-pattern dbadapter) table-name true true)
        datafy-result-set)))

(defn get-columns
  [{:keys [metadata dbadapter catalog]} & [table-name]]
  (binding [njdf/*datafy-failure* :omit]
    (-> metadata
        (.getColumns catalog (:schema-pattern dbadapter) table-name nil)
        datafy-result-set)))

(defn get-foreign-keys
  [{:keys [metadata dbadapter catalog]} & [table-name]]
  (binding [njdf/*datafy-failure* :omit]
    (-> metadata
        (.getImportedKeys catalog (:schema-pattern dbadapter) table-name)
        datafy-result-set)))

(defn get-primary-keys
  [{:keys [metadata dbadapter catalog]} & [table-name]]
  (binding [njdf/*datafy-failure* :omit]
    (-> metadata
        (.getPrimaryKeys catalog (:schema-pattern dbadapter) table-name)
        datafy-result-set)))

(defn get-tables
  [{:keys [metadata dbadapter catalog]}]
  (binding [njdf/*datafy-failure* :omit]
    (-> metadata
        (.getTables catalog (:schema-pattern dbadapter) nil (into-array ["TABLE"]))
        datafy-result-set)))

(defn- adapt-column-type
  [raw-col-type column-types]
  (loop [[[pattern normalized-col-type] :as column-types] (seq column-types)]
    (if pattern
      (if (re-find pattern raw-col-type)
        normalized-col-type
        (recur (rest column-types)))
      (keyword raw-col-type))))

(defn build-columns
  [{{:keys [predicates column-types]} :dbadapter
    :keys [include-raw?]
    :as dbmd}
   table-name
   table-cols]
  (let [fks (->> (get-foreign-keys dbmd table-name) (group-by :fkcolumn_name))
        pks (->> (get-primary-keys dbmd table-name) (group-by :column_name))
        ixs (->> (get-index-info dbmd table-name)   (group-by :column_name))]
    (reduce (fn [cols-map {:keys [column_name type_name] :as col}]
              (let [raw-column     (-> (select-keys col [:type_name
                                                         :column_name
                                                         :is_nullable
                                                         :is_autoincrement])
                                       (assoc :indexes (get ixs column_name)))
                    fk-ref         (some->> (get fks column_name)
                                            first
                                            ((juxt :pktable_name :pkcolumn_name))
                                            (mapv keyword))
                    nullable?      ((:nullable? predicates) raw-column)
                    unique?        ((:unique? predicates) raw-column)
                    autoincrement? ((:autoincrement? predicates) raw-column)
                    primary-key?   (get pks column_name)]
                (assoc cols-map
                       (keyword column_name)
                       (cond-> {:column-type (adapt-column-type (-> type_name str/lower-case) column-types)}
                         include-raw?   (assoc :raw raw-column)
                         nullable?      (assoc :nullable? true)
                         primary-key?   (assoc :primary-key? true)
                         unique?        (assoc :unique? true)
                         autoincrement? (assoc :autoincrement? true)
                         fk-ref         (assoc :refers-to fk-ref)))))
            {}
            table-cols)))

(defn- table-deps
  [xray]
  (for [[table-name table-xray] xray
        [_ column-xray]         (:columns table-xray)
        :let                    [refers-to (first (:refers-to column-xray))]
        :when                   refers-to]
    [table-name refers-to]))

(defn- unconnected-tables
  [table-xray connected-tables]
  (let [table-names (set (keys table-xray))]
    (->> (set/difference table-names (set connected-tables))
         sort
         vec)))

(defn- table-fk-sort
  "table names sorted by fk dependencies"
  [table-xray]
  (let [connected-tables (->> (table-deps table-xray)
                              (reduce (fn [g [table-name dep]]
                                        (try (dep/depend g table-name dep)
                                             (catch Throwable _ g)))
                                      (dep/graph))
                              (dep/topo-sort))]
    (into (unconnected-tables table-xray connected-tables)
          connected-tables)))

(defn- table-filter-regex
  [table-filter]
  (-> (if (keyword? table-filter)
        (name table-filter)
        table-filter)
      str
      (Pattern/compile Pattern/CASE_INSENSITIVE)))

(defn- filter-tables
  [table-filter tables]
  (cond
    (nil? table-filter)
    tables

    (fn? table-filter)
    (table-filter tables)

    ;; make it a case-insensitive match
    (or (instance? java.util.regex.Pattern table-filter)
        (string? table-filter)
        (keyword? table-filter))
    (filter #(re-matches (table-filter-regex table-filter) (:table_name %))
            tables)

    (sequential? table-filter)
    (let [table-set (into #{} table-filter)]
      (filter (comp table-set :table_name) tables))

    :else
    (throw (ex-info "Don't know how to filter with given table-filter" {:table-filter table-filter}))))

(defn xray
  "Given a JDBC connection, produce metadata for a database. Includes raw metadata
  for columns.

  use `:include-raw? true` in `adapter-opts` to include raw metadata"
  [conn & [{:keys [table-filter] :as opts}]]
  (let [dbmd       (prep conn opts)
        tables     (->> dbmd
                        get-tables
                        (filter-tables table-filter))
        columns    (group-by :table_name (get-columns dbmd))
        table-xray (reduce (fn [xr {:keys [table_name]}]
                             (let [table-cols (get columns table_name)]
                               (assoc xr
                                      (keyword table_name)
                                      {:columns      (build-columns dbmd
                                                                    table_name
                                                                    table-cols)
                                       :column-order (mapv (comp keyword :column_name)
                                                           table-cols)})))
                           {}
                           tables)]
    {:tables      table-xray
     :table-order (vec (table-fk-sort table-xray))}))

;; convenience aliases
(def clojure-spec clojure-spec/generate)
(def malli-schema malli/generate)
(def plumatic-schema plumatic-schema/generate)
(def datapotato-schema datapotato/generate)


(comment
  (get-index-info (prep (jdbc/get-connection
                         {:dbtype "mysql", :dbname "dbxray_test", :user "root"}))
                  "parent_records")
  (with-open [conn (jdbc/get-connection {:dbtype "sqlite", :dbname "sqlite.db"})]
    (clojure.datafy/nav (clojure.datafy/datafy (.getMetaData conn))
                        :schemas
                        nil))
  (with-open [conn (jdbc/get-connection {:dbtype "sqlite", :dbname "sqlite.db"})]
    (-> (.getMetaData conn)
        (.getTables nil nil nil nil)
        clojure.datafy/datafy))

  (-> {:dbtype "mysql", :dbname "dbxray_test", :user "root"}
      jdbc/get-connection
      prep
      :metadata
      .getCatalogs
      datafy-result-set
      ))


(comment
  ;; TODO pass in schema pattern
  ;; TODO pass in table filter
  )
