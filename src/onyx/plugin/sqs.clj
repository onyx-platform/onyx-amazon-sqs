(ns onyx.plugin.sqs
  (:import [com.amazonaws AmazonClientException]
           [com.amazonaws.auth DefaultAWSCredentialsProviderChain]
           [com.amazonaws.services.sqs AmazonSQS AmazonSQSClient AmazonSQSAsync AmazonSQSAsyncClient]
           [com.amazonaws.services.sqs.buffered AmazonSQSBufferedAsyncClient]
	   [com.amazonaws.handlers AsyncHandler]
	   [com.amazonaws.services.sqs.model 
            SendMessageBatchRequest SendMessageBatchRequestEntry GetQueueAttributesRequest
	    ChangeMessageVisibilityRequest DeleteMessageRequest CreateQueueRequest CreateQueueResult GetQueueUrlResult
	    Message ReceiveMessageRequest ReceiveMessageResult]))

(defn new-async-client ^AmazonSQSAsync []
  (let [credentials (DefaultAWSCredentialsProviderChain.)]
    (AmazonSQSAsyncClient. credentials)))

(defn new-client ^AmazonSQS []
  (let [credentials (DefaultAWSCredentialsProviderChain.)]
    (AmazonSQSClient. credentials)))

;; Attributes is a hashmap containing any of the following strings
;; DelaySeconds - The time in seconds that the delivery of all messages in the queue will be delayed. An integer from 0 to 900 (15 minutes). The default for this attribute is 0 (zero).
;; MaximumMessageSize - The limit of how many bytes a message can contain before Amazon SQS rejects it. An integer from 1024 bytes (1 KiB) up to 262144 bytes (256 KiB). The default for this attribute is 262144 (256 KiB).
;; MessageRetentionPeriod - The number of seconds Amazon SQS retains a message. Integer representing seconds, from 60 (1 minute) to 1209600 (14 days). The default for this attribute is 345600 (4 days).
;; Policy - The queue's policy. A valid AWS policy. For more information about policy structure, see Overview of AWS IAM Policies in the Amazon IAM User Guide.
;; ReceiveMessageWaitTimeSeconds - The time for which a ReceiveMessage call will wait for a message to arrive. An integer from 0 to 20 (seconds). The default for this attribute is 0.
;; VisibilityTimeout - The visibility timeout for the queue. An integer from 0 to 43200 (12 hours). The default for this attribute is 30. For more information about visibility timeout, see Visibility Timeout in the Amazon SQS Developer Guide.
;; DelaySeconds - The time in seconds that the delivery of all messages in the queue will be delayed. An integer from 0 to 900  ()
(defn create-queue [^AmazonSQS client queue-name attributes]
  (.getQueueUrl ^CreateQueueResult 
                (.createQueue client 
                              (doto (CreateQueueRequest. queue-name)
                                (.setAttributes attributes)))))

(defn get-queue-url [^AmazonSQS client ^String queue-name]
  (.getQueueUrl ^GetQueueUrlResult (.getQueueUrl client queue-name)))

(defn queue-attributes [^AmazonSQS client ^String queue-url]
  (into {} (.getAttributes 
             (.getQueueAttributes client 
                                  (.withAttributeNames (GetQueueAttributesRequest. queue-url) 
                                                       ["All"])))))

(defn receive-request ^ReceiveMessageRequest 
  [^AmazonSQS client queue-url max-num-messages attribute-names wait-time-secs]
  (doto (ReceiveMessageRequest.)
    (.withMaxNumberOfMessages (int max-num-messages))
    (.setAttributeNames attribute-names)
    (.setWaitTimeSeconds (int wait-time-secs))
    (.withQueueUrl queue-url)))

(defn message->clj [^Message msg]
  {:attributes (into {} (.getAttributes msg))
   :body (.getBody msg)
   :body-md5 (.getMD5OfBody msg)
   :message-id (.getMessageId msg)
   :receipt-handle (.getReceiptHandle msg)})

(defn receive-messages [^AmazonSQS client queue-url max-num-messages attribute-names wait-time-secs]
  (let [request (receive-request client queue-url max-num-messages attribute-names wait-time-secs)
        result ^ReceiveMessageResult (.receiveMessage client request)]
    (doall (map message->clj (.getMessages result)))))

(defn delete-message [^AmazonSQS client ^String queue-url ^String receipt-handle]
  (.deleteMessage client queue-url receipt-handle))

(defn delete-message-async [^AmazonSQSAsync client ^String queue-url ^String receipt-handle]
  (.deleteMessageAsync client queue-url receipt-handle))

(defn send-message-batch-request 
  ^SendMessageBatchRequest [queue-url messages]
  (SendMessageBatchRequest. queue-url 
                            (map (fn [msg]
                                   (SendMessageBatchRequestEntry. (str (java.util.UUID/randomUUID)) msg))
                                 messages)))

(defn send-message-batch [^AmazonSQS client ^String queue-url messages]
  (.sendMessageBatch client (send-message-batch-request queue-url messages)))

(defn send-message-batch-async 
  [^AmazonSQSAsync client ^String queue-url messages ^AsyncHandler handler]
  (.sendMessageBatchAsync client (send-message-batch-request queue-url messages) handler))

(defn change-visibility-request-async [^AmazonSQSAsync client ^String queue-url message-id visibility-time]
  (let [visibility-request ^ChangeMessageVisibilityRequest (ChangeMessageVisibilityRequest. queue-url message-id visibility-time)] 
    (.changeMessageVisibilityAsync client visibility-request)))
