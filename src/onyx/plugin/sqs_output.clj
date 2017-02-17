(ns onyx.plugin.sqs-output
  (:require [onyx
             [extensions :as extensions]
             [schema :as os]]
            [onyx.plugin.sqs :as sqs]
            [onyx.static.util :refer [kw->fn]]
            [onyx.tasks.sqs :refer [SQSOutputTaskMap]]
            [schema.core :as s]
            [onyx.plugin.protocols.plugin :as p]
            [onyx.plugin.protocols.input :as i]
            [onyx.plugin.protocols.output :as o]
            [taoensso.timbre :as timbre :refer [error warn]])
  (:import com.amazonaws.AmazonClientException
           com.amazonaws.handlers.AsyncHandler
           com.amazonaws.services.sqs.AmazonSQS))

(defn add-default-queue-url [segment queue-url]
  (update segment :queue-url #(or %
                                  queue-url
                                  (throw (ex-info "queue-url must be defined in segment or task map."
                                                  {:task-map-queue-url queue-url :segment segment})))))

(defn write-handle-exception [event lifecycle lf-kw exception]
  :restart)

(def output-calls
  {:lifecycle/handle-exception write-handle-exception})

(def max-futures 10)

(defn clear-done-writes! [write-futures]
  (swap! write-futures (fn [fs] (remove future-done? fs))))

(defrecord SqsOutput [serializer-fn ^AmazonSQS client default-queue-url write-futures]
  p/Plugin
  (start [this event] 
    ;; move producer creation to in here
    this)

  (stop [this event] 
    (.shutdown client)
    this)

  o/Output
  (synced? [this epoch]
    (empty? (clear-done-writes! write-futures)))

  (recover! [this replica-version checkpoint]
    this)

  (checkpoint [this])

  (checkpointed! [this epoch]
    true)

  (prepare-batch [this event replica _]
    true)

  (write-batch [this {:keys [onyx.core/results]} replica _]
    (if (>= (count (clear-done-writes! write-futures)) max-futures)
      false
      (if-let [segs (seq (mapcat :leaves (:tree results)))] 
        (let [sqs-messages (map (fn [leaf]
                                  (add-default-queue-url leaf default-queue-url))
                                segs)]
          (run! (fn [[batch-queue-url messages]]
                  (let [bodies (map (comp serializer-fn :body) messages)]
                    (->> bodies
                         (sqs/send-message-batch-async client batch-queue-url)
                         (swap! write-futures conj))))
                (group-by :queue-url sqs-messages))
          true)
        true))))

(defn output [event]
  (let [task-map (:onyx.core/task-map event)
        _ (s/validate (os/UniqueTaskMap SQSOutputTaskMap) task-map)
        {:keys [sqs/queue-url sqs/queue-name sqs/serializer-fn sqs/region]} task-map
        client ^AmazonSQS (sqs/new-async-client region)
        serializer-fn (kw->fn serializer-fn)
        default-queue-url (or queue-url
                              (if queue-name
                                (sqs/get-queue-url client queue-name)))]
    (->SqsOutput serializer-fn client default-queue-url (atom []))))
