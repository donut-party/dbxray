(ns donut.dbxray
  (:require
   [next.jdbc.result-set :as result-set]
   [next.jdbc.sql :as sql]))

(def sql-opts {:builder-fn result-set/as-unqualified-maps})

(defn assoc-some
  [m k v]
  (cond-> m
    v (assoc k v)))

(defmulti table (fn [{:keys [dbtype]}] dbtype))

(defn parse-column-constraints-postgres
  [constraints]
  {:primary-key? (->> constraints
                      (filter #(= "PRIMARY KEY" (:table_constraints/constraint_type %)))
                      seq
                      boolean)})

(defmethod table
  :postgres
  [{:keys [connection table-record]}]
  (let [column-records (sql/query connection
                                  ["SELECT * FROM information_schema.columns
                                    WHERE table_name = ?
                                      AND table_catalog = ?
                                      AND table_schema = ?"
                                   (:table_name table-record)
                                   (:table_catalog table-record)
                                   (:table_schema table-record)]
                                  sql-opts)
        constraints    (->> (sql/query connection
                                       ["SELECT
                                           tc.table_schema,
                                           tc.constraint_name,
                                           tc.table_name,
                                           tc.constraint_type,
                                           kcu.column_name,
                                           ccu.table_schema AS foreign_table_schema,
                                           ccu.table_name AS foreign_table_name,
                                           ccu.column_name AS foreign_column_name
                                         FROM
                                           information_schema.table_constraints AS tc
                                           JOIN information_schema.key_column_usage AS kcu
                                             ON tc.constraint_name = kcu.constraint_name
                                             AND tc.table_schema = kcu.table_schema
                                           JOIN information_schema.constraint_column_usage AS ccu
                                             ON ccu.constraint_name = tc.constraint_name
                                             AND ccu.table_schema = tc.table_schema
                                         WHERE tc.table_name = ?"
                                        (:table_name table-record)])
                            (group-by :key_column_usage/column_name))]

    {:columns (reduce (fn [m {:keys [column_name] :as column-record}]
                        (let [col-constraints (get constraints column_name)]
                          (assoc m (keyword column_name) (merge {:type      (keyword (:data_type column-record))
                                                                 :not-null? (= "NO" (:is_nullable column-record))}
                                                                (parse-column-constraints-postgres col-constraints)))))
                      {}
                      column-records)}))

(defmulti tables (fn [{:keys [dbtype]}] dbtype))

(defmethod tables
  :postgres
  [{:keys [connection] :as opts}]
  (let [table-records (sql/query connection
                                 ["SELECT table_name, table_catalog, table_schema
                                   FROM information_schema.tables WHERE table_schema='public'"]
                                 sql-opts)]
    (->> table-records
         (reduce (fn [m table-record]
                   (assoc m (-> table-record :table_name keyword) (table (assoc opts :table-record table-record))))
                 {}))))
