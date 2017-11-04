(ns onyx.plugin.sqs-input-test
  (:require [clojure.core.async :refer [alts!! chan timeout]]
            [clojure.test :refer [deftest is]]
            [onyx api
             [job :refer [add-task]]
             [test-helper :refer [with-test-env]]]
            [onyx.plugin sqs-input
             [sqs :as s]]
            [onyx.tasks.sqs :as task]))

(def out-chan (atom nil))

(defn inject-out-ch [event lifecycle]
  {:core.async/chan @out-chan})

(def out-calls
  {:lifecycle/before-task-start inject-out-ch})

(def region "us-east-1")

(deftest sqs-input-test
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
        queue-name (apply str (take 10 (str (java.util.UUID/randomUUID))))
        client (s/new-async-buffered-client region)
        queue (s/create-queue client queue-name {"VisibilityTimeout" "10"
                                                 "MessageRetentionPeriod" "320"})]
    (with-test-env [test-env [3 env-config peer-config]]
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
                                   :lifecycle/calls ::out-calls}]}
                    (add-task (task/sqs-input :in
                                              region
                                              ::clojure.edn/read-string
                                              {:sqs/queue-name queue-name
                                               :onyx/batch-timeout 1000})))
            n-messages 500
            input-messages (map (fn [v] {:n v}) (range n-messages))
            send-result (time (doall (pmap #(s/send-message-batch client queue %)
                                           (partition-all 10 (map pr-str input-messages)))))]
        (assert (empty? 
                  (remove true? 
                          (map #(empty? (.getFailed %)) send-result))) 
                "Failed to send all messages to SQS. This invalidates the test results, but does not mean that the plugin is broken.")

        (reset! out-chan (chan 1000000))
        (let [job-id (:job-id (onyx.api/submit-job peer-config job))
              timeout-ch (timeout 40000)
              results (vec (keep first 
                                 (repeatedly n-messages 
                                             #(alts!! [timeout-ch @out-chan] :priority true))))
              get-epoch #(-> (onyx.api/job-snapshot-coordinates peer-config (-> peer-config :onyx/tenancy-id) job-id) :epoch)
              epoch (get-epoch)]
          (Thread/sleep 10000)
          (while (= epoch (get-epoch))
            (Thread/sleep 1000))
          (is (= input-messages
                 (sort-by :n (map :body results))))
          (onyx.api/kill-job peer-config job-id)
          (let [attrs (s/queue-attributes client queue ["ApproximateNumberOfMessages" 
                                                        "ApproximateNumberOfMessagesNotVisible"])]
            (is (= "0" (get attrs "ApproximateNumberOfMessages")))
            (is (= "0" (get attrs "ApproximateNumberOfMessagesNotVisible")))))))
    (s/delete-queue client queue)))
