## 1.0.0.pre.1

* Essentially a ground-up rewrite to support CQL3

## 0.5.3

* Persist Dictionary changes in batches

## 0.5.2

* Allow overriding of default column family name for model

## 0.5.1

* Implement `Dictionary#first` and `Dictionary#last`
* Use default column limit when loading multiple wide rows

## 0.5.0

* Cequel::Model::Counter model class
* Counter column support for data sets
* Connection pool
* Load multiple dictionary rows in one query
* Allow erb yml files

## 0.4.2

* Default thrift options to empty hash if not provided
* Implement Dictionary#key?

## 0.4.1

* `Dictionary#each_slice`
* Release to Rubygems.org

## 0.4.0

* Inspect UUIDs nicer
* Allow `Cequel::Model` classes to implement `#generate_key`
* Implement `Cequel::Model::Dictionary`

## 0.3.3

* Fix Enumerator for `#find_each_row`

## 0.3.2

* `#find_in_batches` accounts for duplicate last row/first row

## 0.3.1

* Sanitize column names

## 0.3.0

* Chain select options
* Support for column ranges and column limits
* Implement `#find_in_batches`, `#find_each`

## 0.2.9

* Don't pre-sanitize CQL statements before sending to cassandra-cql

## 0.2.8

* Don't set `updated_at` if no dirty attributes

## 0.2.7

* Add NewRelic instrumentation

## 0.2.6

* Lazily create CassandraCql connection

## 0.2.5

* Include limit in `COUNT` query if specified
* Default scope

## 0.2.4

* Memoize column family name

## 0.2.3

* Fix subclass `#reflect_on_associations`

## 0.2.2

* Add `index_preference` for query plan hinting

## 0.2.1

* Don't call constructor when hydrating models

## 0.2.0

* Add support for dynamic attributes

## 0.1.9

* Tweaks to logging

## 0.1.8

* Add a slowlog

## 0.1.7

* Update based on attributes, not value of getters

## 0.1.6

* Strip nil values out of rows in Cequel::Model

## 0.1.5

* Add thrift client options when setting up connection

## 0.1.4

* Defer setting logger on keyspace until keyspace needs to be loaded

## 0.1.3

* Set logger correctly in railtie

## 0.1.2

* Initial library release
