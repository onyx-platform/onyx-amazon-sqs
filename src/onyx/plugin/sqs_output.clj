(ns onyx.plugin.sqs-output
  (:require [onyx
             [extensions :as extensions]
             [types :refer [dec-count! inc-count!]]]
            [onyx.log.commands.peer-replica-view :refer [peer-site]]
            [onyx.peer
             [function :as function]
             [operation :refer [kw->fn]]
             [pipeline-extensions :as p-ext]]
            [onyx.plugin.sqs :as sqs]
            [onyx.static.default-vals :refer [arg-or-default defaults]]
            [onyx.tasks.sqs :refer [SQSOutputTaskMap]]
            [schema.core :as s]
            [taoensso.timbre :as timbre :refer [debug error info warn]])
  (:import com.amazonaws.AmazonClientException
           com.amazonaws.handlers.AsyncHandler
           [com.amazonaws.services.sqs AmazonSQS AmazonSQSAsync AmazonSQSAsyncClient AmazonSQSClient]))

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

(defn build-ack-callback [peer-replica-view messenger acks]
  (reify AsyncHandler
    (onSuccess [this request result]
      (try 
        (doseq [ack acks] 
          (when (dec-count! ack)
            (when-let [site (peer-site peer-replica-view (:completion-id ack))]
              (extensions/internal-ack-segment messenger site ack))))
        (catch Throwable t
          (error t "sqs-output: error in ack handler"))))
    (onError [this e]
      (warn e "sqs-output: batch send failed"))))

(defrecord SqsOutput [serializer-fn ^AmazonSQS client default-queue-url]
  p-ext/Pipeline
  (read-batch 
    [_ event]
    (function/read-batch event))

  (write-batch 
    [_ {:keys [onyx.core/results onyx.core/peer-replica-view onyx.core/messenger] :as event}]
    (let [segments-acks (results->segments-acks results default-queue-url)] 
      (run! inc-count! (map second segments-acks))
      (run! (fn [[batch-queue-url s]]
              (try 
                (let [acks (map second s)
                      segments (map (comp serializer-fn :body first) s)
                      callback (build-ack-callback peer-replica-view messenger acks)]
                  ;; Increment ack reference count because we are writing async
                  (sqs/send-message-batch-async client batch-queue-url segments callback))
                (catch AmazonClientException e
                  (warn e "sqs-output: write-batch caught exception"))))
            (group-by (comp :queue-url first) segments-acks)))
    {})

  (seal-resource 
    [_ event]))

(defn output [event]
  (let [task-map (:onyx.core/task-map event)
        _ (s/validate SQSOutputTaskMap task-map)
        {:keys [sqs/queue-url sqs/queue-name sqs/serializer-fn sqs/region]} task-map
        client ^AmazonSQS (sqs/new-async-client region)
        serializer-fn (kw->fn serializer-fn)
        default-queue-url (or queue-url 
                              (if queue-name 
                                (sqs/get-queue-url client queue-name)))] 
    (->SqsOutput serializer-fn client default-queue-url)))
