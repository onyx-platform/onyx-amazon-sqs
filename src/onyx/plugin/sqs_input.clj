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

(defrecord SqsInput
  [deserializer-fn batch-size batch-timeout ^AmazonSQS client queue-url 
   attribute-names message-attribute-names epoch batch segment processing]

  p/Plugin
  (start [this event]
    this)

  (stop [this event] 
    ;; TODO::: immediately put all of the messages that are in processing back on queue
    ; (try
    ;  (let [message-id (:message-id (@pending-messages segment-id))]
    ;    ;; Change visibility on message to 0 so that SQS will retry the message through read-batch
    ;    (sqs/change-visibility-request-async client queue-url message-id 0))
    ;  (catch AmazonClientException e
    ;    (warn e "sqs-input: retry-segment, error on change visibility request")))
    (.shutdown client)
    this)

  p/Checkpointed
  (checkpoint [this]
    {})

  (checkpointed! [this epoch])

  (recover! [this replica-version checkpoint]
    (vreset! epoch 1)
    this)

  p/BarrierSynchronization
  (synced? [this ep]
    (->> (partition-all sqs-max-batch-size (get @processing ep))
         (map (fn [batch]
                (->> batch
                     (map :receipt-handle)     
                     (sqs/delete-message-async-batch client queue-url))))
         (doall) 
         (run! deref))
    (vswap! processing dissoc ep)
    (vswap! epoch inc)
    true)

  ;; it's difficult to seal an SQS task as the messages may just be invisible
  (completed? [this] 
    false)

  p/Input
  (poll! [this _]
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
