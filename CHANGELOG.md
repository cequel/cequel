## 3.0.2
* Fix problem with source reload creating duplicate finder methods (find_by_id_and_id) [Issue 206](https://github.com/cequel/cequel/issues/206)
* Fix problems with noisy logs in Ruby 2.4+ [Issue 373](https://github.com/cequel/cequel/issues/373)

## 3.0.1
* fix list modification bug with Cassandra versions > 2.2.10 and 3.11.0

## 3.0.0
* Drop support for changing the type of cluster keys as it is no longer support by Cassandra.
* Drop support for non-option based index specification in table schema DSL. For example, `column :author_name, :text, true` must be rewritten as `column :author_name, :text, index: true`.
* Fix Relic instrumentation for batch statements [PR 361](https://github.com/cequel/cequel/pull/361)
* Don't set table name when it is already present [PR 364](https://github.com/cequel/cequel/pull/364)

## 2.1.0

* Add ActiveRecord::Enum like support `column :status, :enum, values: { open: 1, closed: 2 }` ([PR 354](https://github.com/cequel/cequel/pull/354))
* Fix bug CQL statement execution error handling ([PR 357](https://github.com/cequel/cequel/pull/357)
* Documentation fixes ([PR 355](https://github.com/cequel/cequel/pull/355))
* Add support for `ALLOW FILTERING` queries ([PR 353](https://github.com/cequel/cequel/pull/353))
* Add support for `IF EXISTS` to schema modifications ([PR 349](https://github.com/cequel/cequel/pull/349))
* Make `test` the default rake tast ([PR 348](https://github.com/cequel/cequel/pull/348))

## 2.0.3

* Add synchronization around use of @cluster and other variables Fix ([PR 333](https://github.com/cequel/cequel/pull/333))
* expose if the dataset is on the last page ([PR 335](https://github.com/cequel/cequel/pull/335))
* Delegate error handling to a policy object, allow for arbitrary options to be passed to cassandra driver gem ([PR 336](https://github.com/cequel/cequel/pull/336))
* Fixes README.md ([PR 340](https://github.com/cequel/cequel/pull/340))
* skip synchronizing materialized view ([PR 346](https://github.com/cequel/cequel/pull/346))
* Fixed link to cassandra documentation ([PR 347](https://github.com/cequel/cequel/pull/347))


## 2.0.2

* Fix intermittent failures around preparing statements ([PR 330](https://github.com/cequel/cequel/pull/330))
* Fix new relic instrumentation ([PR 331](https://github.com/cequel/cequel/pull/331))

## 2.0.1

* Remove requirment on activemodel-serializers-xml ([PR 329](https://github.com/cequel/cequel/pull/329))

## 2.0.0

* add support for Cassandra 3.x ([PR 324](https://github.com/cequel/cequel/pull/324))
* upgrade cassandra driver to 3.x ([PR 323](https://github.com/cequel/cequel/pull/323))
* add support for storing blobs (via parameterized CQL statements) ([PR 320](https://github.com/cequel/cequel/pull/320))
* add support for Rails 5 ([PR 310](https://github.com/cequel/cequel/pull/310))
* drop support for JRuby ([PR 310](https://github.com/cequel/cequel/pull/310))
* handle missing indexes gracefully ([PR 317](https://github.com/cequel/cequel/pull/317))
* Dropped support for count, length, and size as it results in unbounded run times and memory usage
  ([PR 313](https://github.com/cequel/cequel/pull/313))

## 1.10.0

* `:foreign_key` option for belongs_to associations
  ([PR 287](https://github.com/cequel/cequel/pull/287))
* `:client_compression` option in cequel.yaml
  ([PR 304](https://github.com/cequel/cequel/pull/304))

## 1.9.1

* fix dirty checking for timestamps more precise than Cassandra can store
* fix bug with new relic instrumentation

## 1.9.0

* NewRelic instrumentation
* fix querying tables whose first partition key is a timestamp

## 1.8.0

* remove false claims of Rubinius support from readme (we would gratefully accept a PR to fix compatibility)
* add dirty tracking on unsaved records
* fix key column order bug on tables with more than 2 key columns
* trim very large field values to a reasonable size in log messages
* native CQL paging support

## 1.7.0

* Support Cassandra up to 2.2
* Support JRuby 9
* Drop support for Rails 3, Ruby 1.9
* Update to `cassandra-driver` 2.0
* Support SSL configuration
* Add `RecordSet#first_or_initialize` and `RecordSet#first!`
* Rake task to reset keyspace

## 1.6.1

* Ruby 1.9 no longer supported (EOL)
* Fix exception class changed in Rails 4.2
* Fix integration with ActiveModel::Serialization

## 1.6.0

* Replace cql-rb with cassandra-driver
* Don't overwrite ActiveSupport's `Module#forwardable` (fixes Rails 4.2
  incompatibility)
* Don't interact with updater/deleter if new record
* Drop support for Rails 3.1

## 1.5.0

* Support ActiveSupport instrumentation of Record create, update, destroy
* Allow config/cequel.yml to specify `:replication` and `:durable_writes`
  settings
* `Key#to_s` correctly returns String, not Symbol
* Don't assume all constants are classes in migrations
* Explicitly require yaml and erb in Railtie
* NewRelic integration can be explicitly disabled
* Demodulize model class name before transforming it to table name

## 1.4.5

* Fix recovery from connection error

## 1.4.4

* Round time to nearest millisecond when serializing for CQL

## 1.4.3

* Make Rake tasks work without Rails
* `RecordSet#reverse` and `find_in_batches` respect clustering order defined in
  schema

## 1.4.2

* Allow setting a key attribute to what it already is
* Don't reset model updater/deleter if save results in an error
* Read `:default_consistency` from cequel.yml
* `:max_retries` configuration parameter for customization of maximum retries that will be made to reconnect to cassandra

## 1.4.1

* Cequel::Record::descendants doesn't return weakrefs

## 1.4.0

* Support TTL and timestamp options to record persistence methods
* Convenience methods for test preparation

## 1.3.2

* Cast values passed to primary key filters in record sets

## 1.3.1

* Allow querying by both primary key and secondary index in `RecordSet`
* Expand cql-rb dependency to allow 2.0

## 1.3.0

* Add timestamps functionality (`created_at` and `updated_at`)
* More robust error handling when loading models for migrations
* Expose `#column_names` on Record and Schema

## 1.2.6

* Fixes for Type::quote

## 1.2.5

* Fix `puts` behavior when passed a Record instance
* Fix batch loading of record sets with bounds on first of multiple clustering
  columns
* Fix load order of namespaced models for migrations

## 1.2.4

* Apply empty attribute values when hydrating records

## 1.2.3

* Fix intermittent load failures of activesupport 4.0.x

## 1.2.2

* Support `:default` option for key columns
* Explicitly require `active_support` (Rails 4.1 compatibility)
* Detect namespaced models in subdirectories when running migrations
* Handle unset collection columns hydrated from database

## 1.2.1

* Remove `app_generators.orm` setting

## 1.2.0

* `where` can now be used to scope primary keys
* Magic finders for primary keys
* Pessimistic versioning for all dependencies

## 1.1.2

* Simplify logging implementation
* Support Cassandra authentication

## 1.1.1

* Specify NewRelicInstrumentation with full namespace
* Update config generator template for native protocol

## 1.1.0

* Switch to `cql-rb` (CQL native protocol) from `cassandra-cql` (Thrift
  protocol)
* Support for consistency tuning
* Add `partition: true` option to `belongs_to`

## 1.0.4

* Fix `#invalid?`

## 1.0.3

* Put missing .yml template file in gem – fix `cequel:configuration` generator
* Explicitly require I18n's hash extensions – fix config loading bug in Rails 3
* Add magic encoding comments for Ruby 1.9

## 1.0.2

* Fix for identifying varint columns when introspecting schema
* Add testing on Rubinius 2.2 and JRuby 1.7

## 1.0.1

* Don't set the same column multiple times in an UPDATE
* Allow clustering order in record key declarations

## 1.0.0

* Add `[]` and `[]=` method for property access
* Add Rails generator for records
* Fallback to filename when inferring model class for migrations

## 1.0.0.rc4

* Add Cequel::Record::ClassMethods

## 1.0.0.rc3

* Better interface for `::find`
* Better interface for `HasManyAssociation`
* Support `ActiveModel::Conversion`

## 1.0.0.rc2

* Add RecordSet#values_at method for IN queries
* Add `:partition` option to Record::Schema#key
* Fix regression in `List#<<`
* Raise RecordNotFound if multi-record load doesn't find all records
* Compatibility with Cassandra 2.0 and recent Cassandra 1.2 patchlevels
* Fail when a schema migration would change the clustering order
* Fail fast when attempting to change a column's type in an incompatible way
* YARD documentation for all public interfaces

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
