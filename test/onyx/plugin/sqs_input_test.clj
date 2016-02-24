(ns onyx.plugin.sqs-input-test
  (:require [clojure.core.async :refer [chan >!! <!! close! sliding-buffer timeout alts!!]]
            [clojure.test :refer [deftest is testing]]
            [onyx.plugin.core-async :refer [take-segments!]]
	    [onyx.plugin.sqs :as s]
	    [onyx.plugin.tasks.sqs :as task]
            [onyx.plugin.sqs-input]
            [onyx.test-helper :refer [load-config with-test-env add-test-env-peers! feedback-exception!]]
            [onyx.api]))

(def out-chan (atom nil))

(defn inject-out-ch [event lifecycle]
  {:core.async/chan @out-chan})

(def out-calls
  {:lifecycle/before-task-start inject-out-ch})

(def region "us-east-1")

(deftest sqs-input-test
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
        client (s/new-client region)
        queue (s/create-queue client queue-name {"VisibilityTimeout" "20"
                                                 "MessageRetentionPeriod" "320"})]
    (with-test-env [test-env [8 env-config peer-config]]
      (let [batch-size 10
	    job (-> {:workflow [[:in :identity] [:identity :out]]
		     :task-scheduler :onyx.task-scheduler/balanced
		     :catalog [;; Add :in task later 
			       {:onyx/name :identity
				:onyx/fn :clojure.core/identity
				:onyx/type :function
                                :onyx/max-peers 1
				:onyx/batch-size batch-size}

			       {:onyx/name :out
				:onyx/plugin :onyx.plugin.core-async/output
				:onyx/type :output
				:onyx/medium :core.async
				:onyx/batch-size batch-size
				:onyx/max-peers 1
				:onyx/doc "Writes segments to a core.async channel"}]
		     :lifecycles [{:lifecycle/task :out
				   :lifecycle/calls ::out-calls}
				  {:lifecycle/task :out
				   :lifecycle/calls :onyx.plugin.core-async/writer-calls}]}
                    (task/add-task (task/sqs-input :in 
                                                   region
                                                   ::clojure.edn/read-string 
                                                   50 
                                                   {:sqs/queue-name queue-name
                                                    :onyx/batch-timeout 1000
                                                    :onyx/pending-timeout 10000})))
	    n-messages 500
	    input-messages (map (fn [v] {:n v}) (range n-messages))
            send-result (doall (pmap #(s/send-message-batch client queue %)
                                     (partition-all 10 (map pr-str input-messages))))]
	(reset! out-chan (chan 1000000))
        
	(let [job-id (:job-id (onyx.api/submit-job peer-config job))
	      timeout-ch (timeout 60000)
	      results (vec (keep first (repeatedly n-messages #(alts!! [timeout-ch @out-chan] :priority true))))]

	  (is (= (count results) 
		 (count (set (map :body results)))))

          (is (= input-messages 
                 (sort-by :n (map :body results)))))))

    (s/delete-queue client queue)))
