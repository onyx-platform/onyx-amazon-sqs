(ns onyx.tasks.sqs
  (:require [onyx.schema :as os]
            [schema.core :as s]))

;;;;;;;;;;;;;;
;;;;;;;;;;;;;;
;; task schemas

(def max-batch-size
  (s/pred (fn [batch-size]
            (and (> batch-size 0)
                 (<= batch-size 10)))
          'max-sqs-batch-size-10))

(def batch-timeout-check
  (s/pred (fn [batch-timeout]
            (zero? (rem batch-timeout 1000)))
          'min-batch-timeout-divisible-1000))

(def SQSInputTaskMap
  {(s/optional-key :sqs/queue-name) s/Str
   (s/optional-key :sqs/queue-url) s/Str
   (s/optional-key :onyx/batch-timeout) batch-timeout-check
   (s/optional-key :sqs/attribute-names) []
   :sqs/region s/Str
   :onyx/batch-size max-batch-size
   :sqs/deserializer-fn os/NamespacedKeyword
   (os/restricted-ns :sqs) s/Any})

(def SQSOutputTaskMap
  {(s/optional-key :sqs/queue-name) s/Str
   (s/optional-key :sqs/queue-url) s/Str
   :sqs/region s/Str
   :sqs/serializer-fn os/NamespacedKeyword
   :onyx/batch-size max-batch-size
   (os/restricted-ns :sqs) s/Any})

(s/defn ^:always-validate sqs-input
  ([task-name task-opts]
   (assert (or (:sqs/queue-name task-opts) (:sqs/queue-url task-opts))
           "Must specify either :sqs/queue-name or :sqs/queue-url to taskbundle opts")
   {:task {:task-map (merge {:onyx/name task-name
                             :onyx/plugin :onyx.plugin.sqs-input/input
                             :onyx/type :input
                             :onyx/medium :sqs
                             :onyx/batch-size 10
                             :onyx/batch-timeout 1000
                             :sqs/attribute-names []
                             :onyx/doc "Reads segments from an SQS queue"}
                            task-opts)
           :lifecycles [{:lifecycle/task task-name
                         :lifecycle/calls :onyx.plugin.sqs-input/input-calls}]}
    :schema {:task-map SQSInputTaskMap}})
  ([task-name :- s/Keyword
    region :- s/Str
    deserializer-fn :- os/NamespacedKeyword
    task-opts :- {s/Any s/Any}]
   (sqs-input task-name (merge {:sqs/region region
                                :sqs/deserializer-fn deserializer-fn}
                               task-opts))))

(s/defn ^:always-validate sqs-output
  ([task-name :- s/Keyword task-opts :- {s/Any s/Any}]
   {:task {:task-map (merge {:onyx/name task-name
                             :onyx/plugin :onyx.plugin.sqs-output/output
                             :onyx/type :output
                             :onyx/medium :sqs
                             :onyx/batch-size 10
                             :onyx/doc "Writes segments to SQS queues"}
                            task-opts)
           :lifecycles [{:lifecycle/task task-name
                         :lifecycle/calls :onyx.plugin.sqs-output/output-calls}]}
    :schema {:task-map SQSOutputTaskMap}})
  ([task-name :- s/Keyword
    region :- s/Str
    serializer-fn :- os/NamespacedKeyword
    task-opts :- {s/Any s/Any}]
   (sqs-output task-name (merge {:sqs/region region
                                 :sqs/serializer-fn serializer-fn}
                                task-opts))))
