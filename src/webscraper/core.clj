; A small webscraper to demonstrate webscraping skills for clojure.
; The script scrapes the numbeo website, transforms the data into a readable format, and loads it to a csv
; All data is owned by https://www.numbeo.com/, and this is just for demonstration purposes.
(ns webscraper.core
  (:require [clj-http.client :as client])
  (:require [hickory.core :as hickory])
  (:require [hickory.select :as s])
  (:require [clojure.string :as string])
  (:require [clojure.data.csv :as csv])
  (:require [clojure.java.io :as io])
  (:require [semantic-csv.core :as sc]))

; Data
;; Capital cities in Europe
(def cities ["London", "Edinburgh", "Cardiff", "Belfast", "Dublin",
             "Paris", "Andorra-La-Vella", "Madrid", "Lisbon", "Monaco",
             "Luxembourg", "Bern", "Rome", "San-Marino-San-Marino", "Brussels",
             "Amsterdam", "Berlin", "Vaduz-Liechtenstein", "Vienna", "Prague",
             "Warsaw", "Vilnius", "Riga", "Tallinn", "Copenhagen", "Oslo",
             "Stockholm", "Reykjavik", "Helsinki", "Minsk", "Kiev",
             "Bratislava", "Ljubljana", "Zagreb", "Budapest", "Sarajevo",
             "Belgrade", "Bucharest", "Chisinau", "Sofia", "Podgorica",
             "Pristina", "Skopje", "Tirana", "Ankara", "Valletta"])

; Functions for collecting data
(defn make-url 
  "Adds `city` to the base url for later use." 
  [city] 
    (str "https://www.numbeo.com/cost-of-living/in/" city))

(defn get-and-parse 
  "Fetches the results from a `url` from [[make-url]], then parses them into hickory format."
  [url] 
    (-> (client/get url) :body hickory/parse hickory/as-hickory))

; Functions for transforming data
(defn parsed->table 
  "Collects table data from hickory formatted `parsed-data` from [[get-and-parse]]."
  [parsed-data]
    (s/select (s/tag :tr) parsed-data))

(defn row->map 
  "Maps `row` to a map structure of :name and :value pairs."
  [row]
    (let [data (s/select 
                (s/tag :td)
                 row)]
      (assoc {} :name  (-> data first :content first) ;; the category of data, i.e "Meal, Inexpensive Restaurant" 
                :value (-> data second :content second :content first)))) ;; the value of data, i.e "5.00 £"

(defn map-rows
  "Helper to map all `rows` to maps using [[row->map]]"
  [rows]
    (map row->map rows))

(defn is-value?
  "Returns true when :value starts with a digit in maps `m` from [[map-rows]]."
  [m]
    (let [value (m :value)]
      (if (string? value) (Character/isDigit (first value)) false)))

(defn filter-rows
  "Filters unwanted `rows` out by checking :value starts with a digit using [[is-value?]]."
  [rows]
    (filter is-value? rows))

(defn data->singular
  "Moves the :name, :value pairs to one pair of :name -> :value for maps `m` from [[map-rows]]."
  [m]
    (let [k (m :name) v (m :value)]
      (assoc {} 
              (keyword                ;; transform to keyword
                (string/lower-case    ;; lowercase
                  (string/replace     ;; replace commas with ""
                    (string/replace   ;; replace whitespace with "_"
                      (string/trim k) ;; remove trailing whitespace 
                        #" " "_")
                          #"," "")))
              v)))

(defn list->map
  "Using [[data->singular]], map entire collection `l` to map."
  [l]
    (reduce merge {} (map data->singular l)))

; Functions for loading data

(defn write-csv
  "Write `data` to csv given `file` path."
  [file data]
    (with-open [writer (io/writer file)]
      (csv/write-csv writer data)))

(defn maps->csv
  "Turns maps into csv file, using semantic-csv library and [[write-csv]]."
  [file maps]
    (->> maps sc/vectorize (write-csv file)))

(defn extract-and-transform
  "The main function to extract and transform numbeo data. Loading happens in [[-main]]"
  [city]
    (let [data (->> city           ;; take the city
                    make-url       ;; make a url for that city
                    get-and-parse  ;; fetch data from that url and parse to hickory
                    parsed->table  ;; retrieve tabular data from hickory format
                    map-rows       ;; map all rows to maps
                    filter-rows    ;; filter rows for numeric values
                    list->map)]    ;; convert list to a map of key-value pairs
      (assoc data :city city)))    ;; Add city to the master map

(defn -main
  "Extracts, Transforms, and loads numbeo data as required."
  []
    (maps->csv "resources/data.csv" (map extract-and-transform cities)))