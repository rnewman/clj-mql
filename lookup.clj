(use 'com.twinql.clojure.restaurants)
(let [city (first *command-line-args*)]
  (println "Looking in" city "...")
  (print-restaurants-in city))
