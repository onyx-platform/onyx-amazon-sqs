(ns onyx.plugin.sqs-input
  (:require [onyx.peer.function :as function]
            [onyx.peer.pipeline-extensions :as p-ext]
            [onyx.plugin.sqs :as sqs]
            [onyx.static.default-vals :refer [defaults arg-or-default]]
            [onyx.types :as t]
            [onyx.peer.operation :refer [kw->fn]]
            [onyx.plugin.tasks.sqs :refer [SQSInputTaskMap]]
            [schema.core :as s]
            [taoensso.timbre :refer [debug info warn] :as timbre])
  (:import [com.amazonaws.services.sqs AmazonSQS AmazonSQSClient AmazonSQSAsync AmazonSQSAsyncClient]
           [com.amazonaws AmazonClientException]))

(defrecord SqsInput 
  [deserializer-fn max-pending batch-size batch-timeout pending-messages ^AmazonSQS client queue-url idle-backoff-ms attribute-names max-wait-time-secs]
  p-ext/Pipeline
  (write-batch 
    [this event]
    (function/write-batch event))

  (read-batch [_ event]
    (try 
      (let [pending (count @pending-messages)
            max-segments (min (- max-pending pending) batch-size)
            received (sqs/receive-messages client queue-url max-segments attribute-names max-wait-time-secs)
            deserialized (map #(update % :body deserializer-fn) received)
            batch (map #(t/input (java.util.UUID/randomUUID) %) deserialized)]
        (if (empty? batch)
          (Thread/sleep idle-backoff-ms)
          (doseq [m batch]
            (swap! pending-messages assoc (:id m) (:message m))))
        {:onyx.core/batch batch})
      (catch AmazonClientException e
        (warn e "sqs-input: read-batch receive messages error")
        {:onyx.core/batch []})))

  (seal-resource [this event])

  p-ext/PipelineInput
  (ack-segment [_ _ segment-id]
    (try 
      ;; Delete the message from the queue as it is fully acked
      (->> (@pending-messages segment-id)
           :receipt-handle
           (sqs/delete-message-async client queue-url))
      (catch AmazonClientException e
        (warn e "sqs-input: ack-segment error on delete message")))
    (swap! pending-messages dissoc segment-id))

  (retry-segment 
    [_ event segment-id]
    (try 
      (let [message-id (:message-id (@pending-messages segment-id))] 
        ;; Change visibility on message to 0 so that SQS will retry the message through read-batch
        (sqs/change-visibility-request-async client queue-url message-id 0))
        (catch AmazonClientException e
          (warn e "sqs-input: retry-segment, error on change visibility request")))
    (swap! pending-messages dissoc segment-id))

  (pending?
    [_ _ segment-id])

  (drained? 
    [_ _]
    ;; Cannot safely drain an SQS queue via :done, as there may be pending retries
    false))

(defn input [event]
  (let [task-map (:onyx.core/task-map event)
        _ (s/validate SQSInputTaskMap task-map)
        max-pending (arg-or-default :onyx/max-pending task-map)
        pending-timeout (arg-or-default :onyx/pending-timeout task-map)
        batch-size (:onyx/batch-size task-map)
        batch-timeout (arg-or-default :onyx/batch-timeout task-map)
        pending-messages (atom {})
        client ^AmazonSQS (sqs/new-async-client) 
        {:keys [sqs/idle-backoff-ms sqs/attribute-names sqs/deserializer-fn sqs/queue-url sqs/queue-name]} task-map
        _ (when (and queue-url queue-name)
            (throw (ex-info "Do not define both sqs/queue-url and sqs/queue-name as they both map to queue urls." task-map)))
        queue-url (or queue-url (sqs/get-queue-url client queue-name))
        idle-backoff-ms (:sqs/idle-backoff-ms task-map)
        deserializer-fn (kw->fn deserializer-fn)
        max-wait-time-secs (int (/ batch-timeout 1000))
        queue-attributes (sqs/queue-attributes client queue-url)
        visibility-timeout (Integer/parseInt (get queue-attributes "VisibilityTimeout"))]

    (when (<= (* visibility-timeout 1000) pending-timeout)
      (throw (ex-info "Pending timeout should be substantially smaller than the VisibilityTimeout on the SQS queue, otherwise SQS will timeout the message prior to the pending-timeout being hit.
                       Note that pending-timeout is in ms, whereas queue visibility timeout is in seconds."
                      {:onyx/pending-timeout pending-timeout
                       "VisibilityTimeout" visibility-timeout})))
    (->SqsInput deserializer-fn max-pending batch-size batch-timeout pending-messages client 
                queue-url idle-backoff-ms attribute-names max-wait-time-secs)))
