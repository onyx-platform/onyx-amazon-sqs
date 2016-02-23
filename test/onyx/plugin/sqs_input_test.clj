(ns onyx.plugin.sqs-input-test
  (:require [clojure.core.async :refer [chan >!! <!! close! sliding-buffer timeout alts!!]]
            [clojure.test :refer [deftest is testing]]
            [onyx.plugin.core-async :refer [take-segments!]]
	    [amazonica.aws.sqs :as sqs]
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
	created (sqs/create-queue :queue-name queue-name
				  :attributes {:VisibilityTimeout 10
                                               :MessageRetentionPeriod 180})
	queue (sqs/find-queue queue-name)]
    (with-test-env [test-env [3 env-config peer-config]]
      (let [batch-size 10
	    job (-> {:workflow [[:in :identity] [:identity :out]]
		     :task-scheduler :onyx.task-scheduler/balanced
		     :catalog [;; Add :in task later 
			       {:onyx/name :identity
				:onyx/fn :clojure.core/identity
				:onyx/type :function
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
                    (task/add-task (task/sqs-input :in ::clojure.edn/read-string 50 {:sqs/queue-name queue-name
                                                                                     :onyx/batch-timeout 1000
                                                                                     :onyx/pending-timeout 8000})))
	    n-messages 1000
	    input-messages (map (fn [v] {:n v}) (range n-messages))]
	(reset! out-chan (chan 50000))
	(doall (pmap #(sqs/send-message queue %) (map pr-str input-messages)))
	(let [job-id (:job-id (onyx.api/submit-job peer-config job))
	      timeout-ch (timeout 60000)
	      results (vec (keep first (repeatedly n-messages #(alts!! [timeout-ch @out-chan] :priority true))))]

	  (is (= (count results) 
		 (count (set (map :body results)))))

          (is (= input-messages 
                 (sort-by :n (map :body results)))))))

    (sqs/delete-queue queue)))
