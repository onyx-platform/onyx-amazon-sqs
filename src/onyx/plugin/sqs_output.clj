(ns onyx.plugin.sqs-output
  (:require [onyx.peer.function :as function]
            [onyx.peer.pipeline-extensions :as p-ext]
            [onyx.plugin.sqs :as sqs]
            [onyx.static.default-vals :refer [defaults arg-or-default]]
            [onyx.plugin.tasks.sqs :refer [SQSOutputTaskMap]]
            [schema.core :as s]
            [taoensso.timbre :refer [debug info] :as timbre])
  (:import [com.amazonaws.services.sqs AmazonSQS AmazonSQSClient AmazonSQSAsync AmazonSQSAsyncClient]
	   [com.amazonaws.handlers AsyncHandler]))

(defrecord SqsOutput [^AmazonSQS client queue-url]
  p-ext/Pipeline
  (read-batch 
    [_ event]
    (function/read-batch event))

  (write-batch 
    [_ {:keys [onyx.core/results] :as event}]
    ;; Should map serializer function here
    (let [batch (map (comp str :message) (mapcat :leaves (:tree results)))]
      (when-not (empty? batch) 
        (sqs/send-message-batch-async client 
                                      queue-url 
                                      batch
                                      (reify AsyncHandler
                                        (onSuccess [this request result]
                                          (spit "hi.txt." "ther.txte"))
                                        (onError [this e]
                                          (spit "FAILLLL.xtrst" "failfail.txt"))))))
    {})

  (seal-resource 
    [_ event]))

(defn output [event]
  (let [task-map (:onyx.core/task-map event)
        _ (s/validate SQSOutputTaskMap task-map)
        client ^AmazonSQS (sqs/new-async-client) 
        queue-url (sqs/get-queue-url client (:sqs/queue-name task-map))] 
    (->SqsOutput client queue-url)))
