(ns com.twinql.clojure.restaurants
  (:require [com.twinql.clojure.mql :as mql]))

(defn restaurants-in [city-id]
  "Returns an array of dictionaries. You can destructure these with let."
  (mql/run-mql 
    [{"/business/business_location/address" {"citytown" {"name" nil
                                                         "id" city-id}
                                             "/location/mailing_address/street_address" nil}
      "id" nil
      "name" nil
      "type" "/dining/restaurant"}]))
     
(defn print-restaurants-in [city-id]
  "Print something like
     Restaurant name, street address
   for each restaurant in city-id."
  (doseq [restaurant (restaurants-in city-id)]
    (let [{type :type
           name :name
           id   :id
           loc  :/business/business_location/address} restaurant]
      (let [{citytown :citytown
             street   :/location/mailing_address/street_address} loc]
             
        ;; You can customize this however you wish.
        (print name)
        (if street
          (println ", " street)
          (println ""))))))
