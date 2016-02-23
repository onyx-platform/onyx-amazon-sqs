(ns onyx.plugin.tasks.sqs
  (:require [schema.core :as s]
            [onyx.schema :as os]
            [taoensso.timbre :refer [info debug fatal]]))

;;;;;;;;;;;;;;
;;;;;;;;;;;;;;
;; task schemas

(def max-batch-size 10)

(def max-batch-size (s/pred (fn [batch-size] 
                              (and (> batch-size 0)
                                   (<= batch-size 10)))
                            'max-sqs-batch-size-10))

(def batch-timeout
  (s/pred (fn [batch-timeout] 
            (and (>= batch-timeout 1000)
                 (rem batch-timeout 1000)))
          'min-batch-timeout-divisible-1000))

(def queue-name-or-queue-url
  (s/pred (fn [task-map]
            (or (and (string? (:sqs/queue-url task-map))
                     (nil? (:sqs/queue-name task-map)))
                (and (nil? (:sqs/queue-url task-map))
                     (string? (:sqs/queue-name task-map)))))
          'queue-name-XOR-queue-url-defined?))

(def SQSInputTaskMap
  (s/->Both [queue-name-or-queue-url
             (merge os/TaskMap 
                    {(s/optional-key :sqs/queue-name) s/Str
                     (s/optional-key :sqs/queue-url) s/Str
                     :onyx/batch-size max-batch-size
                     :sqs/deserializer-fn os/NamespacedKeyword
                     :sqs/idle-backoff-ms os/PosInt})]))

(def SQSOutputTaskMap
  (merge os/TaskMap 
         {(s/optional-key :sqs/queue-name) s/Str
          (s/optional-key :sqs/queue-url) s/Str
          :sqs/serializer-fn os/NamespacedKeyword
          :onyx/batch-size max-batch-size}))

(s/defn ^:always-validate sqs-input
  [task-name :- s/Keyword 
   deserializer-fn :- os/NamespacedKeyword 
   idle-backoff-ms :- s/Int 
   task-opts :- {s/Any s/Any}]
  {:task {:task-map (merge {:onyx/name task-name
                            :onyx/plugin :onyx.plugin.sqs-input/input
                            :onyx/type :input
                            :onyx/medium :sqs
                            :onyx/batch-size 10
                            :onyx/batch-timeout 1000
                            :sqs/deserializer-fn deserializer-fn
                            :sqs/attribute-names []
                            :sqs/idle-backoff-ms idle-backoff-ms
                            :onyx/doc "Reads segments from an SQS queue"}
                           task-opts)
          :lifecycles []}
   :schema {:task-map SQSInputTaskMap
            :lifecycles [os/Lifecycle]}})

(s/defn ^:always-validate sqs-output
  [task-name :- s/Keyword 
   serializer-fn :- os/NamespacedKeyword 
   task-opts :- {s/Any s/Any}]
  {:task {:task-map (merge {:onyx/name :out
                            :onyx/plugin :onyx.plugin.sqs-output/output
                            :onyx/type :output
                            :onyx/medium :sqs
                            :onyx/batch-size 10
                            :sqs/serializer-fn serializer-fn
                            :onyx/doc "Writes segments to SQS queues"}
                           task-opts)
          :lifecycles []}
   :schema {:task-map SQSOutputTaskMap
            :lifecycles [os/Lifecycle]}})

;; TODO, remove once this is in core
(s/defn add-task :- os/Job
  "Adds a task's task-definition to a job"
  [{:keys [lifecycles triggers windows flow-conditions] :as job}
   {:keys [task schema] :as task-definition}]
  (when schema (s/validate schema task))
  (cond-> job
    true (update :catalog conj (:task-map task))
    lifecycles (update :lifecycles into (:lifecycles task))
    triggers (update :triggers into (:triggers task))
    windows (update :windows into (:windows task))
    flow-conditions (update :flow-conditions into (:flow-conditions task))))
