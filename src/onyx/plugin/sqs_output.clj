(ns onyx.plugin.sqs-output
  (:require [onyx.peer.function :as function]
            [onyx.peer.pipeline-extensions :as p-ext]
            [onyx.plugin.sqs :as sqs]
            [onyx.extensions :as extensions]
            [onyx.static.default-vals :refer [defaults arg-or-default]]
            [onyx.plugin.tasks.sqs :refer [SQSOutputTaskMap]]
            [onyx.log.commands.peer-replica-view :refer [peer-site]]
            [onyx.peer.operation :refer [kw->fn]]
            [onyx.types :refer [dec-count! inc-count!]]
            [schema.core :as s]
            [taoensso.timbre :refer [debug info warn] :as timbre])
  (:import [com.amazonaws.services.sqs AmazonSQS AmazonSQSClient AmazonSQSAsync AmazonSQSAsyncClient]
           [com.amazonaws AmazonClientException]
	   [com.amazonaws.handlers AsyncHandler]))

(defn default-queue-url [segment queue-url]
  (update segment :queue-url #(or % 
                                  queue-url 
                                  (throw (ex-info "queue-url must be defined in segment or task map." 
                                                  {:task-map-queue-url queue-url :segment segment})))))

(defn results->segments-acks [results queue-url]
  (mapcat (fn [{:keys [leaves]} ack]
            (map (fn [leaf] 
                   (list (default-queue-url (:message leaf) queue-url) 
                         ack))
                 leaves))
          (:tree results)
          (:acks results)))

(def serialization-fn str)

(defn build-callback-handler [peer-replica-view messenger acks]
  (reify AsyncHandler
    (onSuccess [this request result]
      (doseq [ack acks] 
        (when (dec-count! ack)
          (when-let [site (peer-site peer-replica-view (:completion-id ack))]
            (extensions/internal-ack-segment messenger site ack)))))
    (onError [this e])))

(defrecord SqsOutput [serializer-fn ^AmazonSQS client default-queue-url]
  p-ext/Pipeline
  (read-batch 
    [_ event]
    (function/read-batch event))

  (write-batch 
    [_ {:keys [onyx.core/results onyx.core/peer-replica-view onyx.core/messenger] :as event}]
    ;; Should map serializer function here
    (run! (fn [[batch-queue-url segment-acks]]
            (try 
              (let [acks (map second segment-acks)
                    segments (map (comp serialization-fn :body first) segment-acks)
                    callback (build-callback-handler peer-replica-view messenger acks)]
                ;; Increment ack reference count because we are writing async
                (run! inc-count! acks)
                (sqs/send-message-batch-async client batch-queue-url segments callback))
                (catch AmazonClientException e
                  (warn e "sqs-output: write-batch caught exception"))))
          (group-by (comp :queue-url first) 
                    (results->segments-acks results default-queue-url)))
    {})

  (seal-resource 
    [_ event]))

(defn output [event]
  (let [task-map (:onyx.core/task-map event)
        _ (s/validate SQSOutputTaskMap task-map)
        client ^AmazonSQS (sqs/new-async-client) 
        {:keys [sqs/queue-url sqs/queue-name sqs/serializer-fn]} task-map
        serializer-fn (kw->fn serializer-fn)
        default-queue-url (or queue-url 
                              (if queue-name 
                                (sqs/get-queue-url client queue-name)))] 
    (->SqsOutput serializer-fn client default-queue-url)))
