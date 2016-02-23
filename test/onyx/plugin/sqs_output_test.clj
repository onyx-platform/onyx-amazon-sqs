(ns onyx.plugin.sqs-output-test
  (:require [clojure.core.async :refer [chan >!! <!! close! sliding-buffer timeout alts!!]]
            [clojure.test :refer [deftest is testing]]
            [onyx.plugin.core-async :refer [take-segments!]]
	    [amazonica.aws.sqs :as sqs]
            [amazonica.core :as ac]
	    [onyx.plugin.sqs :as s]
            [onyx.plugin.sqs-output]
	    [onyx.plugin.tasks.sqs :as task]
            [onyx.test-helper :refer [load-config with-test-env add-test-env-peers! feedback-exception!]]
            [onyx.api]))


(def in-chan (atom nil))

(defn inject-in-ch [event lifecycle]
  {:core.async/chan @in-chan})

(def in-calls
  {:lifecycle/before-task-start inject-in-ch})

(defn pull-queue-results [queue tries]
  (mapv :body (mapcat (fn [_] 
                        (:messages (sqs/receive-message :queue-url queue
                                                        :max-number-of-messages 10
                                                        :delete true
                                                        :attribute-names ["All"])))
                      (range tries))))

(def serializer-fn read-string)

(def region "us-east-1")

(deftest sqs-output-test
  (let [id (java.util.UUID/randomUUID)
	env-config {:onyx/id id
		    :zookeeper/address "127.0.0.1:2188"
		    :zookeeper/server? true
		    :zookeeper.server/port 2188}
	peer-config {:onyx/id id
		     :zookeeper/address "127.0.0.1:2188"
		     :onyx.peer/job-scheduler :onyx.job-scheduler/greedy
		     :onyx.messaging.aeron/embedded-driver? true
		     :onyx.messaging/allow-short-circuit? false
		     :onyx.messaging/impl :aeron
		     :onyx.messaging/peer-port 40200
		     :onyx.messaging/bind-addr "localhost"}
	queue-name (apply str (take 10 (str (java.util.UUID/randomUUID))))
	created (sqs/create-queue :queue-name queue-name
                                  :attributes {:VisibilityTimeout 60
                                               :MessageRetentionPeriod 180})
	non-default-queue-name (apply str (take 10 (str (java.util.UUID/randomUUID))))
        non-default-queue (sqs/create-queue :queue-name non-default-queue-name
                                            :attributes {:VisibilityTimeout 60
                                                         :MessageRetentionPeriod 180})
	queue (sqs/find-queue queue-name)]
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
            input-messages (map (fn [v] {:body {:n v}}) 
                                (range n-messages))
            non-default-queue-messages (map (fn [v] {:queue-url (:queue-url non-default-queue)
                                                     :body {:n v}}) 
                                            (range n-messages))]
        (run! #(>!! @in-chan %) input-messages)
        (run! #(>!! @in-chan %) non-default-queue-messages)
        (let [job-id (:job-id (onyx.api/submit-job peer-config job))
              results (pull-queue-results queue-name 100)
              non-default-results (pull-queue-results non-default-queue-name 100)] 

	  (is (= (count results) 
		 (count (set results))))
	  (is (= (map :body input-messages) 
                 (sort-by :n (mapv read-string results))))
          
          (is (= (count non-default-results) 
		 (count (set non-default-results))))
	  (is (= (map :body non-default-queue-messages) 
                 (sort-by :n (mapv read-string non-default-results)))))))
    (sqs/delete-queue queue)
    (sqs/delete-queue non-default-queue)))
