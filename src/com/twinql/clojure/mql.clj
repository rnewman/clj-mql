(ns com.twinql.clojure.mql
  (:import 
    (java.lang Exception)
    (java.net URI))
  (:require 
    [com.twinql.clojure.http :as http]
    [org.danlarkin.json :as json]))

(def *query-server* (new URI "http://api.freebase.com/api/service/mqlread"))

(defn process-query-result [res]
  (when res
    (let [json (json/decode-from-str res)]
      (if (= "/api/status/ok" (json :code))
        (json :result)
        (throw (new Exception
                    (str "Non-OK status from MQL query: "
                         (json :status))))))))

(defn run-mql 
  ([mql debug]
    (let [[code response body] (http/get *query-server*
                                         :query {"query"
                                                 (json/encode-to-str
                                                   {"query" mql
                                                    "escape" false})}
                                         :as :string)]
      (when debug
        (println "Got response" code response)
        (prn body))
      (if (and (>= code 200)
               (< code 300))
        (process-query-result body)
        (throw (new Exception
                    (str "Bad response: " code " " response))))))
  ([mql]
   (run-mql mql false)))
