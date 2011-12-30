(ns FriendFinder.core
  (:use [clojure.data.json :only (json-str write-json read-json)]
        [clojure.pprint]
        [clojure.set]
        [compojure.core] 
        [ring.adapter.jetty])
  (:require [compojure.route :as route]))

(def social-graph (ref {}))
(def edge-map (ref {}))
(def recording (atom true))

(def user-db (atom {}))

(declare ranking ranking-name ranking-str user-output)

(defroutes main-routes
           (GET "/graph" [] (str (dosync [@social-graph @edge-map])))
           (GET "/id/:id" [id] (user-output (read-string id)))
           (GET "/name/:name" [name] (ranking-name (read-string name)))
           (route/not-found "<h1>Page not found</h1>"))


(defn user-output [id]
  (json-str {:id id
             :name (@user-db id)
             :friends (ranking id)}))

(defn update-user-db [_ _ _ snapshot]
  (let [merge-fn (partial merge-with union)
        users (apply merge-fn 
                     (map (fn [{:keys [id nick]}] 
                            {nick #{id}}) 
                          (:tag (last snapshot))))]
    (swap! user-db merge-fn users)))

(def History (atom []))

(defn stop []
  (reset! recording false))

(defn store-graph []
  (spit "social_graph.txt" (dosync [@social-graph @edge-map])))

(defn read-graph []
  (read-string (slurp "social_graph.txt")))

(defn restore-graph []
  (let [[sg em] (read-graph)]
    (dosync
      (ref-set edge-map em)
      (ref-set social-graph sg)))
  "")

(defn reader-reducer [tags {:keys [id reader nick]}]
  (if-not (nil? reader)
    (if (get tags reader)
      (update-in tags [reader] conj {:id id :nick nick})
      (assoc tags reader [{:id id :nick nick}]))
    tags))

(defmulti get-points #(vector (if (= Integer (type %1)) :i :s) 
                              (if (= Integer (type %2)) :i :s)))
(defmethod get-points [:s :s] [x y]
  (let [[xid yid] (map @user-db [x y])]
    (when (= (count xid) (count yid) 1)
      (get-points (first xid)  (first yid)))))
(defmethod get-points [:s :i] [x y]
  (let [xid (@user-db x)]
    (when (= (count xid) 1)
      (get-points (first xid) y))))
(defmethod get-points [:i :s] [x y]
  (get-points y x))
(defmethod get-points [:i :i] [x y]
  (@edge-map (get-in @social-graph [x y])))

(defn group-by-reader [tags]
  (reduce reader-reducer {} tags))

(defn unique-users [user-by-reader]
  (map set (vals user-by-reader)))

(def counter (ref (- 1)))
(defn creat-edge []
  (alter counter inc))

(defn add-edge [social-graph [u1 u2] new-edge]
  (alter social-graph (partial merge-with merge)
                  {(:id u1) {u2 new-edge} (:id u2) {u1 new-edge}}))

(defn edge-updater [social-graph edge-map  pair]
  (dosync  
    (let [pair (vec pair)
          edge (get-in @social-graph [(:id (first pair)) (second pair)])]
      (if edge
        (alter edge-map update-in [edge] inc)
        (let [new-edge (creat-edge)]
          (add-edge social-graph pair new-edge)
          (alter edge-map assoc new-edge 1))))))

(defn update-graph [social-graph users]
  (let [pairs (set (for [user1 users
                         user2 users
                         :when (not= user1 user2)]
                     #{user1 user2}))]    
    (doseq [pair pairs]
      (edge-updater social-graph edge-map pair))))

(declare start start-recording)

(defn start []
  (future (start-recording)))

(defn start-jetty []
  (future (run-jetty main-routes {:port 8081})))

(defn start-recording []
  (reset! recording true)
  (add-watch History :update update-user-db)
  (while @recording
    (let [track-json (read-json (slurp "http://176.99.39.100/tracking.json"))
          unique-user-sets  (unique-users (group-by-reader (:tag track-json)))]
      (doseq [sets unique-user-sets]
        (update-graph social-graph sets))
      (swap! History conj track-json))
    (Thread/sleep 1000)))

(set! *print-length* 10)

(defn ranking [id]
  (sort-by #(nth % 2) >
         (map (fn [[key val]] 
                [(:id key) (:nick key)
                               (@edge-map val)])
              (@social-graph id))))

(defn ranking-name [name]
  (let [id-set (@user-db name)]
    (if (>= 1 (count id-set))
      (ranking (first id-set))
      (map ranking  id-set))))

(defn ranking-str [id]
  (doall (interpose "\n" 
             (map (partial apply str) 
                  (map #(interleave ["id: " " name: " " point: "] %)
                       (ranking id))))))

(comment (defn -main []
    (let [f (future (start-recording))]
      @f)))

