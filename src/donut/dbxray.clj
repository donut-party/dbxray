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

(defn prep
  [conn]
  (let [metadata (.getMetaData conn)]
    {:metadata metadata
     :dbtype   (-> metadata
                   .getDatabaseProductName
                   str/lower-case
                   keyword)}))

(defn xray
  [db-spec]
  (prep db-spec))

(defn datafy-result-set
  [rs]
  (-> rs
      (njrs/datafiable-result-set nil {:builder-fn njrs/as-unqualified-lower-maps})
      df/datafy))

(defn get-tables
  [{:keys [metadata] :as opts}]
  (binding [njdf/*datafy-failure* :omit]
    (-> metadata
        (.getTables nil (schema-pattern opts) nil (into-array ["TABLE"]))
        datafy-result-set)))

(defn get-columns
  [{:keys [metadata] :as opts}]
  (binding [njdf/*datafy-failure* :omit]
    (-> metadata
        (.getColumns nil (schema-pattern opts) nil nil)
        datafy-result-set)))

(defn get-foreign-keys
  [{:keys [metadata] :as opts}]
  (binding [njdf/*datafy-failure* :omit]
    (-> metadata
        (.getImportedKeys nil (schema-pattern opts) "*")
        datafy-result-set)))

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

  (dbx/explore [md pg-conn]
               (df/datafy (.getImportedKeys md nil nil "todos"))))
