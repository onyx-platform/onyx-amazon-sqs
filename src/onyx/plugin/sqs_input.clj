(ns onyx.plugin.sqs-input
  (:require [onyx.schema :as os]
            [onyx.plugin.sqs :as sqs]
            [onyx.static.default-vals :refer [arg-or-default]]
            [onyx.static.util :refer [kw->fn]]
            [onyx.tasks.sqs :refer [SQSInputTaskMap]]
            [onyx.plugin.protocols :as p]
            [schema.core :as s]
            [taoensso.timbre :as timbre :refer [info warn]])
  (:import com.amazonaws.AmazonClientException
           com.amazonaws.services.sqs.AmazonSQS))

(def sqs-max-batch-size 10)
(def change-visibility-client-linger-ms 1000)

(defrecord SqsInput
  [deserializer-fn batch-size batch-timeout ^AmazonSQS client queue-url 
   attribute-names message-attribute-names epoch batch segment processing]

  p/Plugin
  (start [this event]
    this)

  (stop [this event] 
    ;; immediately put all of the messages that are in processing back on queue
    (let [message-ids (map :message-id (reduce into [] (vals @processing)))] 
      (if (empty? message-ids)
        (.shutdown client)
        (do 
         (try
          (doseq [message-id message-ids]
            ;; Change visibility on message to 0 so that SQS will retry the message through read-batch
            (sqs/change-visibility-request-async client queue-url message-id (int 0)))
          (catch AmazonClientException e
            (warn e "sqs-input: error on change visibility request")))
         ;; spin up a thread to shutdown the client after a reasonable amount of time
         ;; has passed that would allow the visibility requests to have completed
         (future (Thread/sleep change-visibility-client-linger-ms)
                 (.shutdown client)))))
    this)

  p/Checkpointed
  (checkpoint [this])

  (checkpointed! [this epoch]
    (->> (partition-all sqs-max-batch-size (get @processing epoch))
         (map (fn [batch]
                (->> batch
                     (map :receipt-handle)     
                     (sqs/delete-message-async-batch client queue-url))))
         (doall) 
         (run! deref))
    (vswap! processing dissoc epoch))

  (recover! [this replica-version checkpoint]
    (vreset! epoch 1)
    (vreset! processing {})
    this)

  p/BarrierSynchronization
  (synced? [this ep]
    (vreset! epoch (inc ep))
    true)

  ;; it's messy to seal an SQS task as the messages may just be invisible
  (completed? [this] 
    false)

  p/Input
  (poll! [this _ _]
    (if-let [segment (first @batch)] 
      (do
       (vswap! batch rest)
       (vswap! processing update @epoch conj (select-keys segment [:message-id :receipt-handle]))
       segment)
      (let [received (sqs/receive-messages client queue-url batch-size
                                           attribute-names
                                           message-attribute-names 0)
            deserialized (doall (map #(update % :body deserializer-fn) received))]
        (vreset! batch deserialized)
        nil))))

(defn read-handle-exception [event lifecycle lf-kw exception]
  :restart)

(def input-calls
  {:lifecycle/handle-exception read-handle-exception})

(defn input [event]
  (let [task-map (:onyx.core/task-map event)
        _ (s/validate (os/UniqueTaskMap SQSInputTaskMap) task-map)
        batch-size (:onyx/batch-size task-map)
        batch-timeout (arg-or-default :onyx/batch-timeout task-map)
        {:keys [sqs/attribute-names sqs/message-attribute-names sqs/deserializer-fn 
                sqs/queue-url sqs/queue-name sqs/region]} task-map
        deserializer-fn (kw->fn deserializer-fn)
        long-poll-timeout (int (/ batch-timeout 1000))
        client (sqs/new-async-buffered-client region {:max-batch-open-ms batch-timeout
                                                      :param-long-poll (not (zero? batch-timeout))
                                                      :long-poll-timeout long-poll-timeout})
        queue-url (or queue-url (sqs/get-queue-url client queue-name))
        queue-attributes (sqs/queue-attributes client queue-url)
        visibility-timeout (Integer/parseInt (get queue-attributes "VisibilityTimeout"))]
    (info "Task" (:onyx/name task-map) "opened SQS input queue" queue-url)
    (->SqsInput deserializer-fn batch-size batch-timeout 
                client queue-url attribute-names message-attribute-names (volatile! nil) 
                (volatile! []) (volatile! nil) (volatile! {}))))
