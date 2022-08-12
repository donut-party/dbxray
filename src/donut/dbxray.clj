(ns donut.dbxray
  (:require
   [clojure.datafy :as df]
   [clojure.string :as str]
   [next.jdbc :as jdbc]
   [next.jdbc.datafy :as njdf]
   [next.jdbc.result-set :as njrs]))

(defmulti schema-pattern :dbtype)

(defmethod schema-pattern :postgresql
  [_]
  "public")

(defmethod schema-pattern :sqlite
  [_]
  nil)

(defmulti normalize-column-type (fn [{:keys [dbtype]} _col-type] dbtype))
(defmethod normalize-column-type :postgresql
  [_ col-type]
  (col-type
   {:int4 :integer}
   col-type))

(defmethod normalize-column-type :default
  [_ col-type]
  col-type)

(defn prep
  [conn]
  (let [metadata (.getMetaData conn)]
    {:metadata metadata
     :dbtype   (-> metadata
                   .getDatabaseProductName
                   str/lower-case
                   keyword)}))

(defn datafy-result-set
  [rs]
  (-> rs
      (njrs/datafiable-result-set nil {:builder-fn njrs/as-unqualified-lower-maps})
      df/datafy))

(defn get-tables
  [{:keys [metadata] :as dbmd}]
  (binding [njdf/*datafy-failure* :omit]
    (-> metadata
        (.getTables nil (schema-pattern dbmd) nil (into-array ["TABLE"]))
        datafy-result-set)))

(defn get-columns
  [{:keys [metadata] :as dmbd} & [table-name]]
  (binding [njdf/*datafy-failure* :omit]
    (-> metadata
        (.getColumns nil (schema-pattern dmbd) table-name nil)
        datafy-result-set)))

(defn- parse-foreign-keys
  [fks]
  (->> fks
       (group-by :pk_name)
       vals
       (map (fn [fk-group]
              (reduce (fn [kv fk]
                        (-> kv
                            (update 0 conj (keyword (:fkcolumn_name fk)))
                            (update 1 conj (keyword (:pkcolumn_name fk)))))
                      [[] [(keyword (:pktable_name (first fk-group)))]]
                      fk-group)))
       (into {})))

(defn get-foreign-keys
  [{:keys [metadata] :as dbmd} table-name]
  (binding [njdf/*datafy-failure* :omit]
    (-> metadata
        (.getImportedKeys nil (schema-pattern dbmd) table-name)
        datafy-result-set)))

(defn get-primary-keys
  [{:keys [metadata] :as dbmd} table-name]
  (binding [njdf/*datafy-failure* :omit]
    (-> metadata
        (.getPrimaryKeys nil (schema-pattern dbmd) table-name)
        datafy-result-set)))

(defn build-columns
  [dbmd table-name table-cols]
  (let [fks (group-by :fkcolumn_name (get-foreign-keys dbmd table-name))
        pks (group-by :column_name (get-primary-keys dbmd table-name))]
    (reduce (fn [cols-map {:keys [column_name is_nullable type_name] :as col}]
              (assoc cols-map (keyword column_name) {:type         (->> type_name
                                                                        str/lower-case
                                                                        keyword
                                                                        (normalize-column-type dbmd))
                                                     :nullable?    (= "YES" is_nullable)
                                                     :primary-key? (boolean (get pks column_name))}))
            {}
            table-cols)))

(defn xray
  [conn]
  (let [dbmd    (prep conn)
        tables  (get-tables dbmd)
        columns (group-by :table_name (get-columns dbmd))]
    (reduce (fn [xr {:keys [table_name]}]
              (let [table-cols (get columns table_name)]
                (assoc xr (keyword table_name) {:columns (build-columns dbmd table_name table-cols)
                                                :foreign-keys (->> (get-foreign-keys dbmd table_name)
                                                                   parse-foreign-keys)})))
            {}
            tables)))




(defmacro explore
  [[binding db-spec] & body]
  `(binding [njdf/*datafy-failure* :omit]
     (let [~binding (-> ~db-spec
                        (prep)
                        :metadata)]
       (datafy-result-set ~@body))))

(comment
  (with-open [conn (jdbc/get-connection {:dbtype "sqlite" :dbname "sqlite.db"})]
    (clojure.datafy/nav (clojure.datafy/datafy (.getMetaData conn)) :schemas nil))

  (with-open [conn (jdbc/get-connection {:dbtype "sqlite" :dbname "sqlite.db"})]
    (->
     (.getMetaData conn)
     (.getTables nil nil nil nil)
     clojure.datafy/datafy
     ))

  )
