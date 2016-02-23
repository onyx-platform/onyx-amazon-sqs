## onyx-sqs

Onyx plugin for sqs.

#### Installation

In your project file:

```clojure
[onyx-sqs "0.8.2.0"]
```

In your peer boot-up namespace:

```clojure
(:require [onyx.plugin.sqs])
```

#### Functions

##### sample-entry

Catalog entry:

```clojure
{:onyx/name :entry-name
 :onyx/plugin :onyx.plugin.sqs-input/input
 :onyx/type :input
 :onyx/medium :sqs
 :onyx/batch-size batch-size
 :onyx/doc "Reads segments from sqs"}
```

Lifecycle entry:

```clojure
[{:lifecycle/task :your-task-name
  :lifecycle/calls :onyx.plugin.sqs/lifecycle-calls}]
```

#### Attributes

|key                           | type      | description
|------------------------------|-----------|------------
|`:sqs/attr`            | `string`  | Description here.

#### Contributing

Pull requests into the master branch are welcomed.

#### License

Copyright © 2015 FIX ME

Distributed under the Eclipse Public License, the same as Clojure.
