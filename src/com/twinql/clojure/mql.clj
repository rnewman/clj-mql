(ns com.twinql.clojure.mql
  (:import 
    (java.lang Exception)
    (java.net URI))
  (:require 
    [com.twinql.clojure.http :as http]
    [org.danlarkin.json :as json]))

(def *mql-version* (new URI "http://api.freebase.com/api/version"))
(def *mql-read* (new URI "http://api.freebase.com/api/service/mqlread"))

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

(defn envelope [q]
  {"query" q
   "escape" false})

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
                     (:headers http-options)
                     (:parameters http-options)
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
