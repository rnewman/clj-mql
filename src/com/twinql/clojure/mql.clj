(ns com.twinql.clojure.mql
  (:import 
    (java.lang Exception)
    (java.net URI))
  (:require 
    [com.twinql.clojure.http :as http]
    [org.danlarkin.json :as json]))

(def *mql-version* (new URI "http://api.freebase.com/api/version"))
(def *mql-status* (new URI "http://api.freebase.com/api/status"))
(def *mql-read* (new URI "http://api.freebase.com/api/service/mqlread"))
(def *mql-search* (new URI "http://api.freebase.com/api/service/search"))

(defn- non-nil-values
  "Return the map with only those keys that map to non-nil values."
  [m]
  (into {} (filter (fn [[k v]] ((complement nil?) v)) m)))

(defn- process-multiple-query-results [res names]
  (when res
    (if (= "/api/status/ok" (:code res))
      (map process-query-result
           (vals (select-keys res (map keyword names))))
      (throw (new Exception
                  (str "Non-OK status from MQL query: "
                       (:status res)))))))

(defn- process-query-result [res]
  (when res
    (if (= "/api/status/ok" (:code res))
      (:result res)
      (throw (new Exception
                  (str "Non-OK status from MQL query: "
                       (:status res)))))))

(defn- envelope [q]
  {"query" q
   "escape" false})

(defmacro content
  "Return only HTTP content."
  [form]
  `(~form 2))

(defmacro debug-print [debug? value & msg]
  `(when ~debug?
     (println ~@msg)
     (prn ~value)))

(defmethod http/entity-as :json [entity as]
  (json/decode-from-reader (http/entity-as entity :reader)))

;; An infinite sequence of query names.
(def query-names
  (map (fn [i] (str "q" i)) (iterate inc 1)))

(defn mql-read 
  "Send a MQL query to Freebase. Optionally provide a dictionary of 
  arguments suitable to http/get, and a boolean for debug output.
  If a sequence of queries is provided, they are batched and run together;
  the output is as if this function had been mapped over the sequence of
  queries, but execution is more efficient."
  ([mql http-options debug?]
   (let [many? (sequential? mql)
         names (when many?
                 (take (count mql) query-names))
         q (if many?
             {"queries"
              (json/encode-to-str
                (zipmap names
                        (map (comp envelope vector) mql)))}
             {"query"
              (json/encode-to-str
                (envelope [mql]))})]
     
     (debug-print debug? q "Query parameters:")
     (let [[code response body]
           (http/get *mql-read*
                     :headers (:headers http-options)
                     :parameters (:parameters http-options)
                     :query q 
                     :as :json)]

       (debug-print debug? body "Got response" code response)
       (if (and (>= code 200)
                (< code 300))
         
         (if many?
           (process-multiple-query-results body names)
           (process-query-result body))

         (throw (new Exception
                     (str "Bad response: " code " " response)))))))
  
  ([mql http-options]
   (mql-read mql http-options false))
  
  ([mql]
   (mql-read mql {} false)))
  
(defn mql-version
  "Returns a map. Useful keys are :graph (graphd version), :me, :cdb, :relevance."
  []
  (content (http/get *mql-version* :as :json)))

(defn mql-status
  []
  (content (http/get *mql-status* :as :json)))

(defn mql-search
  "Perform a MQL search operation.
  Arguments are query (a string), then any of the following keys:
  :format
  :prefixed
  :limit
  :start
  :type
  :type-strict
  :domain
  :domain-strict
  :escape
  :http-options, a dictionary treated as the arguments to http/get."
  [query & args]
  (let [{:keys [format      ; json, id, guid
                prefixed    ; boolean
                limit       ; positive integer, 20 by default
                start       ; default 0
                type        ; string, MQL ID
                type-strict ; string 'any'
                domain
                domain-strict
                escape      ; 'html'

                http-options]} (apply hash-map args)
        
        query (non-nil-values
                {"query" query
                 "format" (or format "json")
                 "prefixed" prefixed
                 "limit" limit
                 "start" start
                 "type" type
                 "type_strict" type-strict
                 "domain" domain
                 "domain_strict" domain-strict
                 "escape" escape})]
    
    (let [[code response body]
          (http/get *mql-search*
                    :headers (:headers http-options)
                    :parameters (:parameters http-options)
                    :query query
                    :as :json)]

      (if (and (>= code 200)
               (< code 300))

        (if (or (nil? format)
                (= format :json)
                (= format "json"))
          (process-query-result body)
          body)
        (throw (new Exception
                    (str "Bad response: " code " " response)))))))

(comment
  (doseq [m (mql/mql-search "Los Gatos")]
    ;; Everything is a topic...
    (let [topic (first
                  (filter
                     #(not (= "Topic" %))
                     (map :name (:type m))))]
      (println 
        (str (:id m) (if topic (str ": a " topic) "")
             " named “" (or (:name m)
                            (:alias m)) "”"))))
  
  (doseq [m (mql/mql-search "Los Gatos" :type "/location/citytown")]
    ;; Everything is a topic.. filter them out..
    (let [topic (first
                  (filter
                     #(not (= "Topic" %))
                     (map :name (:type m))))]
      (println 
        (str (:id m) (if topic (str ": a " topic) "")
             " named “" (or (:name m)
                            (:alias m)) "”"))))
  )
