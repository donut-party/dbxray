(ns donut.dbxray
  (:require
   [clojure.core.protocols :as p]
   [clojure.datafy :as df]
   [clojure.string :as str]
   [next.jdbc :as jdbc]
   [next.jdbc.datafy :as njdf]))

(defmulti schema-pattern :dbtype)

(defmethod schema-pattern :postgresql
  [_]
  "public")

(defn prep
  [db-spec]
  (let [metadata (-> db-spec jdbc/get-connection .getMetaData)]
    {:metadata metadata
     :dbtype   (-> metadata
                   .getDatabaseProductName
                   str/lower-case
                   keyword)}))

(defn xray
  [db-spec]
  (prep db-spec))

(defn get-tables
  [{:keys [metadata] :as opts}]
  (binding [njdf/*datafy-failure* :omit]
    (-> metadata
        (.getTables nil (schema-pattern opts) nil (into-array ["TABLE"]))
        df/datafy)))

(defn get-columns
  [{:keys [metadata] :as opts}]
  (binding [njdf/*datafy-failure* :omit]
    (-> metadata
        (.getColumns nil (schema-pattern opts) nil nil)
        df/datafy)))

(defmacro explore
  [[binding db-spec] & body]
  `(binding [njdf/*datafy-failure* :omit]
     (let [~binding (-> ~db-spec
                        (prep)
                        :metadata)]
       ~@body)))

(comment
  (require '[next.jdbc.datafy :as nj-datafy])
  (require '[next.jdbc.result-set :as rs])
  (extend-protocol p/Datafiable
    org.sqlite.jdbc4.JDBC4ResultSet
    (datafy [this]
      (if (instance? java.sql.ResultSetMetaData this)
        (#'next.jdbc.datafy/datafy-result-set-meta-data this)
        (let [s (.getStatement this)
              c (when s (.getConnection s))]
          (cond-> (#'next.jdbc.datafy/safe-bean this {})
            c (assoc :rows (rs/datafiable-result-set this c {})))))))

  (with-open [conn (jdbc/get-connection {:dbtype "sqlite" :dbname "sqlite.db"})]
    (clojure.datafy/nav (clojure.datafy/datafy (.getMetaData conn)) :schemas nil))

  (with-open [conn (jdbc/get-connection {:dbtype "sqlite" :dbname "sqlite.db"})]
    (->
     (.getMetaData conn)
     (.getTables nil nil nil nil)
     clojure.datafy/datafy
     ))

  (dbx/explore [md pg-conn]
               (df/datafy (.getImportedKeys md nil nil "todos"))))
