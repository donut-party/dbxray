(ns donut.dbxray
  (:require
   [clojure.datafy :as df]
   [clojure.string :as str]
   [flatland.ordered.map :as omap]
   [next.jdbc :as jdbc]
   [next.jdbc.datafy :as njdf]
   [next.jdbc.result-set :as njrs]))

(def default-adapter
  {:schema-pattern nil
   :column-types   {#"int" :integer}
   :predicates     {:nullable? (fn [is_nullable] (= "YES" is_nullable))
                    :unique?   (fn [{:keys [non_unique]}] (not non_unique))}})

(defmulti adapter* :dbtype)

(defmethod adapter* :postgresql
  [_]
  {:schema-pattern "public"})

(defmethod adapter* :sqlite
  [_]
  {:predicates   {:unique? (fn [{:keys [non_unique]}] (= 0 non_unique))}
   :column-types {#"varchar" :varchar}})

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
  [conn]
  (let [metadata (.getMetaData conn)
        dbtype   (database-product-name metadata)
        dbmd     {:metadata metadata
                  :dbtype   dbtype}]
    (assoc dbmd :dbadapter (adapter dbmd))))

(defn datafy-result-set
  [rs]
  (-> rs
      (njrs/datafiable-result-set nil {:builder-fn njrs/as-unqualified-lower-maps})
      df/datafy))

(defn get-index-info
  [{:keys [metadata dbadapter]} & [table-name]]
  (binding [njdf/*datafy-failure* :omit]
    (-> metadata
        (.getIndexInfo nil (:schema-pattern dbadapter) table-name true true)
        datafy-result-set)))

(defn get-columns
  [{:keys [metadata dbadapter]} & [table-name]]
  (binding [njdf/*datafy-failure* :omit]
    (-> metadata
        (.getColumns nil (:schema-pattern dbadapter) table-name nil)
        datafy-result-set)))

(defn get-foreign-keys
  [{:keys [metadata dbadapter]} & [table-name]]
  (binding [njdf/*datafy-failure* :omit]
    (-> metadata
        (.getImportedKeys nil (:schema-pattern dbadapter) table-name)
        datafy-result-set)))

(defn get-primary-keys
  [{:keys [metadata dbadapter]} & [table-name]]
  (binding [njdf/*datafy-failure* :omit]
    (-> metadata
        (.getPrimaryKeys nil (:schema-pattern dbadapter) table-name)
        datafy-result-set)))

(defn get-tables
  [{:keys [metadata dbadapter]}]
  (binding [njdf/*datafy-failure* :omit]
    (-> metadata
        (.getTables nil (:schema-pattern dbadapter) nil (into-array ["TABLE"]))
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
  [{{:keys [predicates column-types]} :dbadapter :as dbmd}
   table-name
   table-cols]
  (let [fks (->> (get-foreign-keys dbmd table-name) (group-by :fkcolumn_name))
        pks (->> (get-primary-keys dbmd table-name) (group-by :column_name))
        ixs (->> (get-index-info dbmd table-name)   (group-by :column_name))]
    (reduce (fn [cols-map {:keys [column_name is_nullable type_name] :as _col}]
              (let [fk-ref       (some->> (get fks column_name)
                                          first
                                          ((juxt :pktable_name :pkcolumn_name))
                                          (mapv keyword))
                    nullable?    ((:nullable? predicates) is_nullable)
                    primary-key? (get pks column_name)
                    unique?      (->> (get ixs column_name)
                                      (filter (:unique? predicates))
                                      seq)]
                (assoc cols-map
                       (keyword column_name)
                       (cond-> {:column-type (adapt-column-type (-> type_name str/lower-case) column-types)
                                :raw-column-type type_name}
                         nullable?    (assoc :nullable? true)
                         primary-key? (assoc :primary-key? true)
                         unique?      (assoc :unique? true)
                         fk-ref       (assoc :refers-to fk-ref)))))
            (omap/ordered-map)
            table-cols)))

(defn xray
  [conn]
  (let [dbmd    (prep conn)
        tables  (get-tables dbmd)
        columns (group-by :table_name (get-columns dbmd))]
    (reduce (fn [xr {:keys [table_name]}]
              (let [table-cols (get columns table_name)]
                (assoc xr (keyword table_name) {:columns (build-columns dbmd table_name table-cols)})))
            {}
            tables)))

(comment
  (defmacro explore
    [[binding db-spec] & body]
    `(binding [njdf/*datafy-failure* :omit]
       (let [~binding (-> ~db-spec
                          (prep)
                          :metadata)]
         (datafy-result-set ~@body))))

  (with-open [conn (jdbc/get-connection {:dbtype "sqlite" :dbname "sqlite.db"})]
    (clojure.datafy/nav (clojure.datafy/datafy (.getMetaData conn)) :schemas nil))

  (with-open [conn (jdbc/get-connection {:dbtype "sqlite" :dbname "sqlite.db"})]
    (->
     (.getMetaData conn)
     (.getTables nil nil nil nil)
     clojure.datafy/datafy
     )))
