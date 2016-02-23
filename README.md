## onyx-amazon-sqs

Onyx plugin for Amazon SQS.

#### Installation

In your project file:

```clojure
[onyx-amazon-sqs "0.8.12.0-SNAPSHOT"]
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
 :sqs/attribute-names []
 :sqs/idle-backoff-ms idle-backoff-ms
 :sqs/queue-name queue-name
 :onyx/doc "Reads segments from an SQS queue"}
```

#### Attributes

|key                           | type      | description
|------------------------------|-----------|------------
|`:sqs/queue-name`             | `string`  | The SQS queue name
|`:sqs/attribute-names`        | `[string]`| A list of attributes to fetch for the queue entry 
|`:sqs/idle-backoff-ms`        | `int`     | Backoff on empty read for backoff ms in order to reduce SQS per request costs


Possible attribute names can be found in the <a href="http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/sqs/model/ReceiveMessageRequest.html#withAttributeNames(java.util.Collection)">API documentation</a>.

##### Output Task

Catalog entry:

```clojure
{:onyx/name <<TASK_NAME>>
 :onyx/plugin :onyx.plugin.sqs-output/output
 :sqs/queue-name <<OPTIONAL_QUEUE_NAME>>
 :onyx/type :output
 :onyx/medium :sqs
 :onyx/batch-size 10
 :onyx/doc "Writes segments to SQS queues"}
```

Segments received at this task must have a body key in string form, which will be written to the queue defined in the key `:sqs/queue-name` in the task-map, or in the key `:queue-name` in the segment. 

#### Attributes

|key                           | type      | description
|------------------------------|-----------|------------
|`:sqs/queue-name`             | `string`  | Optional SQS queue name. If not present, segments will be routed via the `:queue-name` key of the segment

#### Contributing

Pull requests into the master branch are welcomed.

#### License

Copyright © 2016 Distributed Masonry LLC

Distributed under the Eclipse Public License, the same as Clojure.
