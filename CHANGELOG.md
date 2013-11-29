## 1.0.0.rc1

* Rails integration: Add rake tasks for creating keyspace, migrations; generator
  for configuration file
* Implement update_all, delete_all, destroy_all
* Option for unlogged batches

## 1.0.0.pre.6

* Allow record sets to be scoped by multiple primary keys
* If a batch is a single statement, just send it as a statement
* Don't start a new batch if you're already in a batch
* Wrap record callbacks in logged batch
* Update `connection_pool` dependency

## 1.0.0.pre.5

* Support time range queries over `timeuuid` columns
* Typecast arguments to scoping functions of `RecordSet`
* Typecast values in collection columns
* Use correct ordering column for deeply nested keys
* Don't allow updating keys on persisted models
* Fail fast attempting to save a model that doesn't have all keys defined
* Fixes for legacy schema support

## 1.0.0.pre.4

* Full support for legacy CQL2 tables
* `dependent` option for `has_many` associations

## 1.0.0.pre.3

* **BIG BREAKING CHANGE:** Instead of inheriting from `Cequel::Model::Base`,
  include `Cequel::Record`.
* RecordSet can chain class methods defined in the model class
* New records are initialized using the key attributes from the current scope
* Auto-generated UUIDs
* Implement RecordSet#== and #inspect
* RecordSet#last takes optional count argument
* Dynamic column defaults
* Fix insertion of default values in new records

## 1.0.0.pre.2

* Secondary index support
* Dirty attribute tracking
* == implementation for model
* Add missing attributes argument to #create!
* Load cequel/model by default

## 1.0.0.pre.1

* Essentially a ground-up rewrite to support CQL3

## 0.5.6

* Ability to change default consistency within a block

## 0.5.5

* Calling ::load on a loaded Dictionary is a no-op

## 0.5.4

* Clear out Dictionary @row on save unless loaded
* Add homepage link so rubygems users can easily get to github
* Add link to cequel-migrations-rails to README
* Use parameters for pooling

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
