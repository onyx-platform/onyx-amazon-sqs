## onyx-amazon-sqs

Onyx plugin for Amazon SQS.

#### Installation

In your project file:

```clojure
[org.onyxplatform/onyx-amazon-sqs "0.13.0.0"]
```

In your peer boot-up namespace:

```clojure
(:require [onyx.plugin.sqs-input]
          [onyx.plugin.sqs-output])
```

#### Limitations

This plugin currently only supports at least once behaviour.

It is neccessary to respond to failed async writes. For reads that have timed out, they will automatically retry. For failed writes we have to re-align the barriers, which resets to the last checkpoint. 

#### Functions

##### Input Task

Catalog entry:

```clojure
{:onyx/name <<TASK_NAME>>
 :onyx/plugin :onyx.plugin.sqs-input/input
 :onyx/type :input
 :onyx/medium :sqs
 :onyx/batch-size 10
 :onyx/batch-timeout 1000
 :sqs/region "us-east-1"
 :sqs/queue-name queue-name
 :sqs/deserializer-fn :clojure.edn/read-string
 :sqs/attribute-names []
 :sqs/message-attribute-names []
 ; :sqs/max-batch 10
 ; :sqs/max-inflight-receive-batches 10
 :onyx/doc "Reads segments from an SQS queue"}
```

In lieu of `:sqs/queue-name`, the url of the queue can be suppied via `:sqs/queue-url`.

SQS only supports batching up to 10 messages, which limits `:onyx/batch-size` to a maximum of 10.

#### Attributes

|key                                 | type      | description
|------------------------------------|-----------|------------
|`:sqs/deserializer-fn`              | `keyword` | A keyword pointing to a fully qualified function that will deserialize the :body of the queue entry from a string
|`:sqs/region`                       | `string`  | The SQS region to use
|`:sqs/queue-name`                   | `string`  | The SQS queue name
|`:sqs/queue-url`                    | `string`  | The SQS queue url, in lieu of sqs/queue-name
|`:sqs/attribute-names`              | `[string]`| A list of attributes to fetch for the queue entry. Default is to fetch no attributes. You can override it to fetch specific attributes or all (["All])
|`:sqs/message-attribute-names`      | `[string]`| A list of attributes to fetch for each message. Default is to fetch no message attributes. You can override it to fetch specific message attributes or all (["All])
|`:sqs/max-batch`                    | `string`  | Maximum number of messages read in a given batch.
|`:sqs/max-inflight-receive-batches` | `string`  | Maximum number of batches to buffer up. :sqs/max-batch * :sqs/max-inflight-receive-batches = number of messages buffered by client before read by onyx.



Possible attribute names can be found in the <a href="http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/sqs/model/ReceiveMessageRequest.html#withAttributeNames(java.util.Collection)">API documentation</a>.
And more about message attributes at <a href="http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/sqs/model/ReceiveMessageRequest.html#withMessageAttributeNames-java.util.Collection-">API documentation</a>.
##### Output Task

Catalog entry:

```clojure
{:onyx/name <<TASK_NAME>>
 :onyx/plugin :onyx.plugin.sqs-output/output
 :sqs/queue-name <<OPTIONAL_QUEUE_NAME>>
 :sqs/region "us-east-1"
 :sqs/serializer-fn :clojure.core/pr-str
 :onyx/type :output
 :onyx/medium :sqs
 :onyx/batch-size 10
 :onyx/doc "Writes segments to SQS queues"}
```

Segments received at this task must have a body key in string form, which will be written to the queue defined in the key `:sqs/queue-name`, OR `:sqs/queue-url` in the task-map, or via the key `:queue-url` in the segment. Note that queue-name and queue-url are in different formats, with the queue-name being in the form `"yourqueuename"` and queue-url in the form `https://sqs.us-east-1.amazonaws.com/039384834151/e3668c38-4`.

SQS only supports batching up to 10 messages, which limits `:onyx/batch-size` to a maximum of 10.

#### Attributes

|key                           | type      | description
|------------------------------|-----------|------------
|`:sqs/region`                 | `string`  | The SQS region to use
|`:sqs/queue-name`             | `string`  | Optional SQS queue name. If not present, segments will be routed via the `:queue-url` key of the segment
|`:sqs/queue-url`              | `string`  | Optional SQS queue url. If not present, segments will be routed via the `:queue-url` key of the segment
|`:sqs/serializer-fn`          | `keyword` | A keyword pointing to a fully qualified function that will serialize the :body key of the segment to a string

#### AWS Role Privileges
The AWS role running Onyx should have the following AWS privileges:
* sqs:SendMessage*
* sqs:ReceiveMessage
* sqs:DeleteMessage*
* sqs:ChangeMessageVisibility*
* sqs:GetQueueAttributes
* sqs:GetQueueUrl

#### Acknowledgments

Many thanks to [LockedOn](http://www.lockedon.com) for allowing this work to be open
sourced and contributed back to the Onyx Platform community.

#### Contributing

Pull requests into the master branch are welcomed.

#### License

Copyright © 2016 Distributed Masonry LLC

Distributed under the Eclipse Public License, the same as Clojure.
