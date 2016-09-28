## onyx-amazon-sqs

Onyx plugin for Amazon SQS.

#### Installation

In your project file:

```clojure
[org.onyxplatform/onyx-amazon-sqs "0.9.11.0-SNAPSHOT"]
```

In your peer boot-up namespace:

```clojure
(:require [onyx.plugin.sqs-input]
          [onyx.plugin.sqs-output])
```

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
 :sqs/idle-backoff-ms idle-backoff-ms
 :onyx/doc "Reads segments from an SQS queue"}
```

In lieu of `:sqs/queue-name`, the url of the queue can be suppied via `:sqs/queue-url`.

SQS only supports batching up to 10 messages, which limits `:onyx/batch-size` to a maximum of 10.

*NOTE*: `:onyx/pending-timeout` (in ms) should be shorter than your queue's VisibilityTimeout (in seconds), so that SQS does not trigger retries before Onyx's native retry mechanism.

#### Attributes

|key                           | type      | description
|------------------------------|-----------|------------
|`:sqs/deserializer-fn`        | `keyword` | A keyword pointing to a fully qualified function that will deserialize the :body of the queue entry from a string
|`:sqs/region`                 | `string`  | The SQS region to use
|`:sqs/queue-name`             | `string`  | The SQS queue name
|`:sqs/queue-url`              | `string`  | The SQS queue url, in lieu of sqs/queue-name
|`:sqs/attribute-names`        | `[string]`| A list of attributes to fetch for the queue entry 
|`:sqs/idle-backoff-ms`        | `int`     | Backoff on empty read for backoff ms in order to reduce SQS per request costs


Possible attribute names can be found in the <a href="http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/sqs/model/ReceiveMessageRequest.html#withAttributeNames(java.util.Collection)">API documentation</a>.

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

#### Acknowledgments

Many thanks to [LockedOn](http://www.lockedon.com) for allowing this work to be open
sourced and contributed back to the Onyx Platform community.

#### Contributing

Pull requests into the master branch are welcomed.

#### License

Copyright © 2016 Distributed Masonry LLC

Distributed under the Eclipse Public License, the same as Clojure.
