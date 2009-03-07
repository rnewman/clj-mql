(ns com.twinql.clojure.mql
  (:import 
    (java.lang Exception)
    (java.net URI)
    (org.apache.http.client.methods HttpGet HttpPost)
    (org.apache.http.client HttpClient ResponseHandler)
    (org.apache.http.client.utils URIUtils URLEncodedUtils)
    (org.apache.http.message BasicNameValuePair)
    (org.apache.http.impl.client DefaultHttpClient BasicResponseHandler))
  (:require 
    [org.danlarkin.json :as json]))

(def *server* (new URI "http://api.freebase.com/"))
(def *query-path* "api/service/mqlread")

(defn encode-query [q]
  "q is a map or list of pairs."
  (. URLEncodedUtils format
     (map (fn [[param value]] 
            (new BasicNameValuePair (str param) (str value)))
          q)
     "UTF-8"))

(defn submit-query [host path q]
  (let [encoded-query (when q (encode-query q))]
    (let [whole-url (if encoded-query
                      (. URIUtils resolve
                         host
                         (. URIUtils createURI nil nil 0 path encoded-query nil))
                      host)]
      (let [http-client (new DefaultHttpClient)
            http-get    (new HttpGet whole-url)
            response-handler (new BasicResponseHandler)]
            
        (let [response-body (. http-client execute http-get response-handler)]
          (. (. http-client getConnectionManager) shutdown)
          response-body)))))

(defn envelope [query]
  {"query" query
   "escape" false})

(defn process-query-result [res]
  (when res
    (let [json (json/decode-from-str res)]
      (if (= "/api/status/ok" (json :code))
        (json :result)
        (throw (new Exception
                    (str "Non-OK status from MQL query: " (json :status))))))))

(defn run-mql 
  ([mql debug]
    (let [submitted (submit-query *server* *query-path*
                                  {"query"
                                   (json/encode-to-str
                                     (envelope mql))})]
      (when debug
        (print submitted))
      (process-query-result submitted)))
  ([mql]
   (run-mql mql false)))
