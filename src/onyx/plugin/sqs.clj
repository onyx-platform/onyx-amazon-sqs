(ns onyx.plugin.sqs
  (:import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
           com.amazonaws.handlers.AsyncHandler
           com.amazonaws.regions.RegionUtils
           [com.amazonaws.services.sqs AmazonSQS AmazonSQSAsync AmazonSQSAsyncClient AmazonSQSClient]
           [com.amazonaws.services.sqs.buffered AmazonSQSBufferedAsyncClient QueueBufferConfig]
           [com.amazonaws.services.sqs.model ChangeMessageVisibilityRequest
            CreateQueueRequest CreateQueueResult DeleteQueueRequest
            GetQueueAttributesRequest GetQueueUrlResult Message
            ReceiveMessageRequest SendMessageBatchRequest
            SendMessageBatchRequestEntry SendMessageRequest
            DeleteMessageBatchRequest DeleteMessageBatchRequestEntry
            MessageAttributeValue]))

(defn new-async-client ^AmazonSQSAsync [^String region]
  (let [credentials (DefaultAWSCredentialsProviderChain.)]
    (doto (AmazonSQSAsyncClient. credentials)
      (.setRegion (RegionUtils/getRegion region)))))

(defn new-client ^AmazonSQS [^String region]
  (let [credentials (DefaultAWSCredentialsProviderChain.)]
    (doto (AmazonSQSClient. credentials)
      (.setRegion (RegionUtils/getRegion region)))))

(defn new-async-buffered-client ^AmazonSQSAsync
  ([^String region {:keys [max-batch-open-ms max-inflight-outbound-batches max-inflight-receive-batches max-done-receive-batches
                           param-long-poll max-batch-size-bytes visibility-timeout long-poll-timeout max-batch]}]
   (AmazonSQSBufferedAsyncClient.
    (new-async-client region)
    (QueueBufferConfig. (or max-batch-open-ms QueueBufferConfig/MAX_BATCH_OPEN_MS_DEFAULT)
                        (or max-inflight-outbound-batches QueueBufferConfig/MAX_INFLIGHT_OUTBOUND_BATCHES_DEFAULT)
                        (or max-inflight-receive-batches QueueBufferConfig/MAX_INFLIGHT_RECEIVE_BATCHES_DEFAULT)
                        (or max-done-receive-batches QueueBufferConfig/MAX_DONE_RECEIVE_BATCHES_DEFAULT)
                        (or param-long-poll false)
                        (or max-batch-size-bytes QueueBufferConfig/SERVICE_MAX_BATCH_SIZE_BYTES)
                        (or visibility-timeout QueueBufferConfig/VISIBILITY_TIMEOUT_SECONDS_DEFAULT)
                        (or long-poll-timeout QueueBufferConfig/LONGPOLL_WAIT_TIMEOUT_SECONDS_DEFAULT)
                        (or max-batch QueueBufferConfig/MAX_BATCH_SIZE_DEFAULT))))
  ([^String region]
   (AmazonSQSBufferedAsyncClient. (new-async-client region))))

;; Attributes is a hashmap containing any of the following strings
;; DelaySeconds - The time in seconds that the delivery of all messages in the queue will be delayed. An integer from 0 to 900 (15 minutes). The default for this attribute is 0 (zero).
;; MaximumMessageSize - The limit of how many bytes a message can contain before Amazon SQS rejects it. An integer from 1024 bytes (1 KiB) up to 262144 bytes (256 KiB). The default for this attribute is 262144 (256 KiB).
;; MessageRetentionPeriod - The number of seconds Amazon SQS retains a message. Integer representing seconds, from 60 (1 minute) to 1209600 (14 days). The default for this attribute is 345600 (4 days).
;; Policy - The queue's policy. A valid AWS policy. For more information about policy structure, see Overview of AWS IAM Policies in the Amazon IAM User Guide.
;; ReceiveMessageWaitTimeSeconds - The time for which a ReceiveMessage call will wait for a message to arrive. An integer from 0 to 20 (seconds). The default for this attribute is 0.
;; VisibilityTimeout - The visibility timeout for the queue. An integer from 0 to 43200 (12 hours). The default for this attribute is 30. For more information about visibility timeout, see Visibility Timeout in the Amazon SQS Developer Guide.
;; DelaySeconds - The time in seconds that the delivery of all messages in the queue will be delayed. An integer from 0 to 900
(defn create-queue [^AmazonSQS client queue-name attributes]
  (.getQueueUrl ^CreateQueueResult
                (.createQueue client
                              (-> (CreateQueueRequest. queue-name)
                                  (.withAttributes attributes)))))

(defn delete-queue [^AmazonSQS client ^String queue-name]
  (.deleteQueue client (DeleteQueueRequest. queue-name)))

(defn get-queue-url [^AmazonSQS client ^String queue-name]
  (.getQueueUrl ^GetQueueUrlResult (.getQueueUrl client queue-name)))

(defn queue-attributes [^AmazonSQS client ^String queue-url]
  (into {} (.getAttributes
            (.getQueueAttributes client
                                 (.withAttributeNames (GetQueueAttributesRequest. queue-url)
                                                      ["All"])))))

(defn receive-request ^ReceiveMessageRequest
  [^AmazonSQS client
   ^String queue-url
   max-num-messages
   ^java.util.Collection attribute-names
   ^java.util.Collection message-attribute-names
   wait-time-secs]
  (let [req ^ReceiveMessageRequest (ReceiveMessageRequest.)]
    (.withQueueUrl
      (.withAttributeNames
        (.withMessageAttributeNames
         (.withMaxNumberOfMessages req ^Integer
         (int max-num-messages))
         message-attribute-names)
        attribute-names)
     queue-url)))

(defn- clojurify-message-attributes [^Message msg]
  (let [javafied-message-attributes (.getMessageAttributes msg)]
    (->> javafied-message-attributes
         (map (fn [[k ^MessageAttributeValue mav]] [(keyword k) (.getStringValue mav)]))
         (into {}))))

(defn message->clj [^Message msg]
  {:attributes (into {} (.getAttributes msg))
   :message-attributes (clojurify-message-attributes msg)
   :body (.getBody msg)
   :body-md5 (.getMD5OfBody msg)
   :message-id (.getMessageId msg)
   :receipt-handle (.getReceiptHandle msg)})

(defn receive-messages [^AmazonSQS client queue-url max-num-messages attribute-names message-attribute-names wait-time-secs]
  (let [request (receive-request client queue-url max-num-messages attribute-names message-attribute-names wait-time-secs)
        result (.receiveMessage client request)]
    (doall (map message->clj (.getMessages result)))))

(defn receive-message [^AmazonSQSAsync client ^String queue-url]
  (doall (message->clj (.receiveMessage client queue-url))))

(defn delete-message [^AmazonSQS client ^String queue-url ^String receipt-handle]
  (.deleteMessage client queue-url receipt-handle))

(defn delete-message-async [^AmazonSQSAsync client ^String queue-url ^String receipt-handle]
  (.deleteMessageAsync client queue-url receipt-handle))

(defn delete-message-batch-request ^DeleteMessageBatchRequest [queue-url receipt-handles]
  (DeleteMessageBatchRequest. queue-url
                              (map (fn [^String handle]
                                     (DeleteMessageBatchRequestEntry. (str (java.util.UUID/randomUUID)) 
                                                                      handle))
                                   receipt-handles)))

(defn delete-message-async-batch [^AmazonSQSAsync client ^String queue-url receipt-handles]
  (.deleteMessageBatchAsync client (delete-message-batch-request queue-url receipt-handles)))

(defn send-message-batch-request
  ^SendMessageBatchRequest [queue-url messages]
  (SendMessageBatchRequest. queue-url
                            (map (fn [msg]
                                   (SendMessageBatchRequestEntry. (str (java.util.UUID/randomUUID)) msg))
                                 messages)))

(defn send-message-batch [^AmazonSQS client ^String queue-url messages]
  (.sendMessageBatch client (send-message-batch-request queue-url messages)))

(defn send-message-batch-async
  [^AmazonSQSAsync client ^String queue-url messages]
  (.sendMessageBatchAsync client (send-message-batch-request queue-url messages)))

(defn send-message [^AmazonSQS client ^String queue-url ^String message]
  (.sendMessage client (SendMessageRequest. queue-url message)))

(defn send-message-async [^AmazonSQSAsync client ^String queue-url ^String message]
  (.sendMessageAsync client (SendMessageRequest. queue-url message)))

(defn change-visibility-request-async [^AmazonSQSAsync client ^String queue-url message-id visibility-time]
  (let [visibility-request ^ChangeMessageVisibilityRequest (ChangeMessageVisibilityRequest. queue-url message-id visibility-time)]
    (.changeMessageVisibilityAsync client visibility-request)))
