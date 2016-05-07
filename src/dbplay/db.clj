(ns dbplay.db
  (:require [clojure.java.io :as io]
            [datomic.api :as d]))

(def db-uri-base "datomic:mem://")

(def resource io/resource)

(defn scratch-conn
  "Create a connection to an anonymous, in-memory database."
  []
  (let [uri (str db-uri-base (d/squuid))]
    (d/delete-database uri)
    (d/create-database uri)
    (d/connect uri)))

(def conn (scratch-conn))

(def schema
  (let [schema (-> (io/resource "schema/meta.edn") slurp read-string)
        data (-> (io/resource "schema/data.edn") slurp read-string)]
    @(d/transact conn schema)
    @(d/transact conn data)))

(def rules
  '[[(ancestors ?entity ?ancestor) [?entity :entity/parent ?ancestor]]
    [(ancestors ?entity ?ancestor) (ancestors ?parent ?ancestor) [?entity :entity/parent ?parent]]
    [(descendants ?entity ?descendant) [?descendant :entity/parent ?cat]]
    [(descendants ?entity ?descendant) [?child :entity/parent ?cat]
     (descendants ?child ?descendant)]])

(defn get-root
  []
  (d/q '[:find ?e .
         :in $
         :where
         [?e :entity/name ?n]
         (not [?e :entity/parent])] (d/db conn)))

;; (get-root)

(defn get-descendants
  ""
  [node]
  (d/q '[:find [?n ...]
         :in $ % ?r
         :where
         [?e :entity/parent ?r]
         (descendants ?e ?d)
         [?d :entity/name ?n]]
       (d/db conn) rules node))

;; (time (get-descendants (get-root)))

(defn get-attr-local
  ""
  [id attr]
  (d/q '[:find [?e ?v]
         :in $ ?e ?attr
         :where [?e ?attr ?v]] (d/db conn) id attr))

;; (get-attr-local (first items) :entity/value)

(defn get-attr-inherited
  ""
  [id attr]
  (d/q '[:find [?p ?v]
         :in $ % ?e ?attr
         :where
         (ancestors ?e ?p)
         [?p ?attr ?v]
         [?p :entity/name ?]]
       (d/db conn) rules id attr))

(defn get-attr-value
  ""
  [id attr]
  (if-let [local (get-attr-local id attr)]
    local
    (get-attr-inherited id attr)))

(def items
  (d/q '[:find [?e ...]
         :in $
         :where [?e :entity/name]] (d/db conn)))

(defn get-all-who
  [attr value]
  (let [pairs (map #(get-attr-value % :entity/value) items)]
    (filter (fn [[id v]] (= value v)) pairs)))

;; (time (map #(get-attr-value % :entity/value) items))
;; (time (get-all-who :entity/value "1"))
