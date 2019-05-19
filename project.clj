(defproject org.onyxplatform/onyx-amazon-sqs "0.14.5.1-SNAPSHOT"
  :description "Onyx plugin for Amazon SQS"
  :url "https://github.com/onyx-platform/onyx-amazon-sqs"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :repositories {"snapshots" {:url "https://clojars.org/repo"
                              :username :env
                              :password :env
                              :sign-releases false}
                 "releases" {:url "https://clojars.org/repo"
                             :username :env
                             :password :env
                             :sign-releases false}}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 ^{:voom {:repo "git@github.com:onyx-platform/onyx.git" :branch "master"}}
                 [org.onyxplatform/onyx "0.14.6-20190519_090646-g3377fe6"]                                                                                                                                                                                                                                                                                                       
                 [com.amazonaws/aws-java-sdk-sqs "1.11.228"]]
  :global-vars  {*warn-on-reflection* true}
  :resource-paths ["resources/"]
  :profiles {:dev {:dependencies []
                   :plugins [[lein-set-version "0.4.1"]
                             [lein-update-dependency "0.1.2"]
                             [lein-pprint "1.1.1"]]}})
