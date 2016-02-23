(ns onyx.plugin.sqs-output-test
  (:require [clojure.core.async :refer [chan >!! <!! close! sliding-buffer timeout alts!!]]
            [clojure.test :refer [deftest is testing]]
            [onyx.plugin.core-async :refer [take-segments!]]
	    [amazonica.aws.sqs :as sqs]
	    [onyx.plugin.sqs :as s]
            [onyx.plugin.sqs-input]
	    [onyx.plugin.tasks.sqs :as task]
            [onyx.test-helper :refer [load-config with-test-env add-test-env-peers! feedback-exception!]]
            [onyx.api]))


(def in-chan (atom nil))

(defn inject-in-ch [event lifecycle]
  {:core.async/chan @in-chan})

(def in-calls
  {:lifecycle/before-task-start inject-in-ch})

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
				  :attributes {:VisibilityTimeout 60})
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
                    (task/add-task (task/sqs-output :out {:sqs/queue-name queue-name})))
            n-messages 100
            _ (reset! in-chan (chan (inc n-messages)))
            input-messages (map #(hash-map :n %) (range n-messages))]
        (run! #(>!! @in-chan %) input-messages)
        (let [job-id (:job-id (onyx.api/submit-job peer-config job))
              results (mapv :body (mapcat (fn [_] 
                                            (:messages (sqs/receive-message :queue-url queue
                                                                            :max-number-of-messages 10
                                                                            :delete true
                                                                            :attribute-names ["All"])))
                                          (range 200)))] 

	  (is (= (count results) 
		 (count (set results))))
	  (is (= input-messages 
                 (sort-by :n (mapv read-string results)))))))
    (sqs/delete-queue queue)))
