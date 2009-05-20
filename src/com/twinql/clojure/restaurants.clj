(ns com.twinql.clojure.restaurants
  (:require [com.twinql.clojure.mql :as mql]))

(defn restaurants-in
  "Returns an array of dictionaries. You can destructure these with let."
  ([city-id]
   (restaurants-in city-id false))
  ([city-id debug]
  (mql/run-mql 
    [{"/business/business_location/address" [{"citytown" {"name" nil
                                                         "id" city-id}
                                             "/location/mailing_address/street_address" nil}]
      "id" nil
      "name" nil
      "type" "/dining/restaurant"}]
    debug)))
     
(defn print-restaurants-in
  "Print something like
     Restaurant name, street address
   for each restaurant in city-id."
  [city-id]
  (doseq [restaurant (restaurants-in city-id)]
    (let [{type :type
           name :name
           id   :id} restaurant
          locs ((keyword "/business/business_location/address") restaurant)]
      (let [citytown (:citytown (first locs))
            street ((keyword "/location/mailing_address/street_address") (first locs))]
             
        ;; You can customize this however you wish.
        (print name)
        (if street
          (println ", " street)
          (println ""))))))
