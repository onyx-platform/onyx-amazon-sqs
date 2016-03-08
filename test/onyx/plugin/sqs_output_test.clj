(ns onyx.plugin.sqs-output-test
  (:require [clojure.core.async :refer [chan >!! <!! close! sliding-buffer timeout alts!!]]
            [clojure.test :refer [deftest is testing]]
            [onyx.plugin.core-async :refer [take-segments!]]
	    [onyx.plugin.sqs :as s]
            [onyx.plugin.sqs-output]
	    [onyx.plugin.tasks.sqs :as task]
            [taoensso.timbre :refer [debug info warn] :as timbre]
            [onyx.test-helper :refer [load-config with-test-env add-test-env-peers! feedback-exception!]]
            [onyx.api]))

(def in-chan (atom nil))

(defn inject-in-ch [event lifecycle]
  {:core.async/chan @in-chan})

(def in-calls
  {:lifecycle/before-task-start inject-in-ch})

(defn pull-queue-results [client queue-url tries]
  (mapv :body (mapcat (fn [_] (let [messages (s/receive-messages client queue-url 10 [] 1)]
                                (run! #(s/delete-message client queue-url (:receipt-handle %)) messages)
                                messages))
                      (range tries))))

(def serializer-fn read-string)

(def region "us-east-1")

(deftest sqs-output-test
  (let [id (java.util.UUID/randomUUID)
        env-config {:onyx/tenancy-id id
                    :zookeeper/address "127.0.0.1:2188"
                    :zookeeper/server? true
                    :zookeeper.server/port 2188}
        peer-config {:onyx/tenancy-id id
                     :zookeeper/address "127.0.0.1:2188"
                     :onyx.peer/job-scheduler :onyx.job-scheduler/greedy
                     :onyx.messaging.aeron/embedded-driver? true
                     :onyx.messaging/allow-short-circuit? false
                     :onyx.messaging/impl :aeron
                     :onyx.messaging/peer-port 40200
                     :onyx.messaging/bind-addr "localhost"}
        client (s/new-async-client region)
        queue-name (apply str (take 10 (str (java.util.UUID/randomUUID))))
        non-default-queue-name (apply str (take 10 (str (java.util.UUID/randomUUID))))
        queue (s/create-queue client queue-name {"VisibilityTimeout" "200"
                                                 "MessageRetentionPeriod" "320"})
        non-default-queue (s/create-queue client non-default-queue-name {"VisibilityTimeout" "200"
                                                                         "MessageRetentionPeriod" "320"})]
    (with-test-env [test-env [3 env-config peer-config]]
      (let [batch-size 10
            job (-> {:workflow [[:in :identity] [:identity :out]]
                     :task-scheduler :onyx.task-scheduler/balanced
                     :catalog [{:onyx/name :in
                                :onyx/plugin :onyx.plugin.core-async/input
                                :onyx/type :input
                                :onyx/medium :core.async
                                :onyx/batch-size batch-size
                                :onyx/max-peers 1
                                :onyx/doc "Reads segments from a core.async channel"}

                               {:onyx/name :identity
                                :onyx/fn :clojure.core/identity
                                :onyx/type :function
                                :onyx/batch-size batch-size}
                               ;; Add :out task later
                               ]
                     :lifecycles [{:lifecycle/task :in
                                   :lifecycle/calls ::in-calls}
                                  {:lifecycle/task :in
                                   :lifecycle/calls :onyx.plugin.core-async/reader-calls}]}
                    (task/add-task (task/sqs-output :out 
                                                    region
                                                    ::serializer-fn 
                                                    {:sqs/queue-name queue-name})))
            n-messages 100
            _ (reset! in-chan (chan (inc (* 2 n-messages))))
            input-messages (map (fn [v] {:body {:n v}}) (range n-messages))
            non-default-queue-messages (map (fn [v] {:queue-url non-default-queue :body {:n v}}) 
                                            (range n-messages))]
        (run! #(>!! @in-chan %) (concat input-messages non-default-queue-messages))
        (let [job-id (:job-id (onyx.api/submit-job peer-config job))
              results (pull-queue-results client queue-name 50)
              non-default-results (pull-queue-results client non-default-queue-name 50)] 

	  (is (= (count results) 
		 (count (set results))))
	  (is (= (map :body input-messages) 
                 (sort-by :n (mapv read-string results))))
          
          (is (= (count non-default-results) 
		 (count (set non-default-results))))
	  (is (= (map :body non-default-queue-messages) 
                 (sort-by :n (mapv read-string non-default-results)))))))
    (s/delete-queue client queue)
    (s/delete-queue client non-default-queue)))
