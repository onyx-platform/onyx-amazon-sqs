(ns onyx.plugin.sqs-output
  (:require [onyx
             [extensions :as extensions]
             [schema :as os]]
            [onyx.plugin.sqs :as sqs]
            [onyx.static.util :refer [kw->fn]]
            [onyx.tasks.sqs :refer [SQSOutputTaskMap]]
            [schema.core :as s]
            [onyx.plugin.protocols :as p]
            [taoensso.timbre :as timbre :refer [error warn info]])
  (:import com.amazonaws.AmazonClientException
           com.amazonaws.handlers.AsyncHandler
           com.amazonaws.services.sqs.AmazonSQS))

(defn add-default-queue-url [segment queue-url]
  (update segment :queue-url #(or % queue-url)))

(defn write-handle-exception [event lifecycle lf-kw exception]
  :restart)

(def output-calls
  {:lifecycle/handle-exception write-handle-exception})

(def max-futures 100)

(defn clear-done-writes! [write-futures]
  (swap! write-futures (fn [fs] 
                         (when-not (empty? fs) 
                           (run! (fn [f] (when-not (.getFailed @f)
                                           (throw (ex-info "SQS write failed." {})))) 
                                 fs))
                         (remove future-done? fs))))

(defrecord SqsOutput [serializer-fn ^AmazonSQS client default-queue-url write-futures]
  p/Plugin
  (start [this event] 
    ;; move producer creation to in here
    this)

  (stop [this event] 
    this)

  p/BarrierSynchronization
  (synced? [this epoch]
    (empty? (clear-done-writes! write-futures)))

  (completed? [this]
    (empty? (clear-done-writes! write-futures)))

  p/Checkpointed
  (recover! [this replica-version checkpoint]
    (reset! write-futures [])
    this)
  (checkpoint [this])
  (checkpointed! [this epoch]
    true)

  p/Output
  (prepare-batch [this event replica _]
    true)

  (write-batch [this {:keys [onyx.core/write-batch] :as event} replica _]
    (if (>= (count (clear-done-writes! write-futures)) max-futures)
      false
      (do 
       (when-not (empty? write-batch) 
         (->> write-batch
              (map (fn [leaf]
                     (add-default-queue-url leaf default-queue-url)))
              (group-by :queue-url)
              (run! (fn [[batch-queue-url messages]]
                      (when-not batch-queue-url 
                        (throw (ex-info "queue-url must be defined in segment or task map."
                                        {:task-map-queue-url batch-queue-url :segments messages})))
                      (->> (partition-all 10 messages)
                           (run! (fn [batch]
                                   (let [bodies (map (comp serializer-fn :body) batch)]
                                     (->> bodies
                                          (sqs/send-message-batch-async client batch-queue-url)
                                          (swap! write-futures conj))))))))))
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
