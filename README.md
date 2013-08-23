# Cequel #

Cequel is a
[CQL](http://www.datastax.com/docs/1.0/references/cql/index#cql-commands)
query builder and object-row mapper for
[Cassandra](http://cassandra.apache.org/).

[![Build Status](https://secure.travis-ci.org/outoftime/cequel.png?branch=1.0)](http://travis-ci.org/outoftime/cequel)

The library consists of two layers. The lower Cequel layer is a lightweight
CQL query builder, which uses chained scopes to construct CQL queries, execute
them against your Cassandra instance, and return results in friendly form.
The Cequel::Model layer implements an object-row mapper on top of Cequel,
with full [ActiveModel](https://github.com/rails/rails/tree/master/activemodel)
integration and an interface that conforms to established patterns for Ruby
persistence layers (e.g. ActiveRecord).

The lower Cequel layer is heavily inspired by the excellent
[Sequel](http://sequel.rubyforge.org/) library; Cequel::Model more closely
follows the form of [ActiveRecord](http://ar.rubyonrails.org/).

## Installation ##

To use only the lower-level Cequel query builder, add the following to your
Gemfile:

``` ruby
gem 'cequel'
```

For Cequel::Model, instead add:

``` ruby
gem 'cequel', :require => 'cequel/model'
```

### Rails integration ###

Cequel and Cequel::Model do not require Rails, but if you are using Rails, you
will need version 3.2+. Cequel::Model will read from the configuration file
`config/cequel.yml` if it is present. A simple example configuration would look
like this:

``` yaml
development:
  host: '127.0.0.1:9160'
  keyspace: myapp_development

production:
  hosts:
    - 'cass1.myapp.biz:9160'
    - 'cass2.myapp.biz:9160'
    - 'cass3.myapp.biz:9160'
  keyspace: myapp_production
  thrift:
    retries: 10
    timeout: 15
    connect_timeout: 15
```

## Cequel Query Builder ##

To connect to a keyspace, use `Cequel.connect`:

``` ruby
cassandra = Cequel.connect(
  :host => '127.0.0.1:9160',
  :keyspace => 'myapp_development'
)
```

Column family handles are referenced as follows:

``` ruby
posts = cassandra[:posts]
```

### Reading Data ###

To select data, you can form a query using the familiar chained scope pattern:

``` ruby
posts = cassandra[:posts].select(:title).
  consistency(:quorum).
  where(:id => 1).
  limit(10)

titles = posts.map { |post| post[:title] }
```

When working with wide rows, you often want to select a range of columns rather
than a predefined set:

``` ruby
# Select columns 1-5
cassandra[:posts].select(1..5)

# Select columns 5 and up
cassandra[:posts].select(:from => 5)

# Select columns up to 5
cassandra[:posts].select(:to => 5)

# Select the first 8 columns (in natural order of column type)
cassandra[:posts].select(:first => 8)

# Select the last 6 columns
cassandra[:posts].select(:last => 6)

# Combine ranges and limits
cassandra[:posts].select(1..100, :first => 5)

# Or open-ended ranges and limits
cassandra[:posts].select(:first => 5, :from => 20)
```

Data set scopes also support the `first` and `count` methods.

#### Subqueries ####

Cequel scopes support a subquery-like syntax, which can be used to populate
the scope of an outer query with the results of an inner query:

``` ruby
cassandra[:blogs].where(:id => cassandra[:posts].select(:blog_id))
```

This actually performs two queries to Cassandra, since CQL itself does not
support subqueries.

### Writing data ###

To insert data, use `insert`:

``` ruby
cassandra[:posts].insert(:id => 1, :title => 'My Post', :body => 'Some wisdom')
```

You can control consistency, timestamp, and time to live by passing a second
options hash to insert:

``` ruby
cassandra[:posts].insert(
  {:id => 1, :title => 'My Post', :body => 'Some wisdom'},
  :consistency => :quorum, :ttl => 10.minutes, :timestamp => 1.day.ago
)
```

To update data, construct a scope and then call `update` with the columns to
write:

``` ruby
cassandra[:posts].where(:id => [1, 2]).update(:title => 'My Post')
```

To update a [counter column](http://wiki.apache.org/cassandra/Counters), use
the `increment` or `decrement` method:

``` ruby
cassandra[:comment_counts].where(:id => 1).increment(post_id => 1)
cassandra[:comment_counts].where(:id => 1).decrement(post2_id => 4)
```

To delete entire rows, call the `delete` method; to delete certain columns from
a row, pass those columns to `delete`:

``` ruby
# delete rows 1 and 2 entirely
cassandra[:posts].where(:id => [1, 2]).delete
# delete title column from rows 1 and 2
cassandra[:posts].where(:id => [1, 2]).delete(:title)
```

## Cequel::Model ##

`Cequel::Model` is a higher-level object-row mapper built on top of the
low-level functionality described above. Cequel models are
ActiveModel-compliant and generally follow ActiveRecord-like patterns.

### Keyspace setup and migrations ###

The current version of Cequel does not provide built-in functionality for
schema creation and modification, but ActiveRecord-like migrations for Cequel
are available via the
[cequel-migrations-rails](https://github.com/reachlocal/cequel-migrations-rails)
library.

The [forthcoming release](https://github.com/brewster/cequel/tree/1.0) of Cequel
will support full schema introspection and modification, and will also provide
auto-migration functionality for models.

### Defining a model ###

Cequel models include the `Cequel::Model` module; the example below demonstrates
most of what's available for defining a model:

``` ruby
class Post

  include Cequel::Model
  include Cequel::Model::Timestamps

  key :id, :uuid
  column :title, :text
  column :body, :text

  belongs_to :blog
  has_many :comments

  attr_accessible :title, :body

  validates :title, :body, :blog_id, :presence => true

  after_create :post_to_twitter

  default_scope limit(100)

  private

  def generate_key
    CassandraCQL::UUID.new
  end

end
```

### Working with models: The non-surprising parts ###

Model behavior will be largely familiar to anyone who has worked with
ActiveRecord or another ActiveRecord-inspired object mapper. All of these
operations work pretty much exactly as you'd expect:

``` ruby
# Initialize a new instance
Post.new

# Initialize a new instance with some attributes
Post.new(:title => 'Hey')

# Initialize a new instance and set some properties
Post.new do |post|
  post.title = 'Hey'
end

# Create a new instance with attributes and save it
Post.create(:title => 'Hey')

# Create a new instance with attributes and save it violently
Post.create!(:title => 'Hey')

# Update an instance
post.title = 'New title'
post.save

# Destroy an instance
post.destroy

# Find an instance by key
Post.find(uuid)

# Find an instance by magic
Post.find_by_blog_id(blog_id)

# Find lots of instances by magic
Post.find_all_by_blog_id(blog_id)

# Find or initialize an instance by magic
Post.find_or_initialize_by_title('My Post')

# Find or initialize an instance by magic with some extra attributes
Post.find_or_initialize_by_title(:title => 'My Post', :body => 'Read more')

# Of course, find_or_create_by works too
Post.find_or_create_by_title('My Post')

# Query by scopes
Post.select(:title).where(:id => uuid).first

# Query by secondary indexes
Post.select(:title).where(:blog_id => blog_uuid).map { |post| post.title }

# This will execute three queries, because CQL secondary indexes don't play nice
# with IN restrictions. But it'll work:
Post.select(:title).
  where(:blog_id => [blog_id1, blog_id2, blog_id3]).
  map { |post| post.title }
```

### Working with models: The surprising parts ###

CQL is designed to be immediately familiar to those of us who are used to
working with SQL, which is all of us. Cequel advances this spirit by providing
an ActiveRecord-like mapping for CQL. However, Cassandra is very much not a
relational database, so some behaviors can come as a surprise. Here's an
overview.

#### Upserts ####

CQL provides `INSERT` and `UPDATE` statements that look more or less exactly
like their SQL equivalents. However, these statements do exactly the same thing,
just with different syntax. What they do is to write values into
columns at a key. So these two Cequel statements have identical behavior:

``` ruby
cassandra[:posts].insert(:id => 1, :title => 'Post')
cassandra[:posts].where(:id => 1).update(:title => 'Post')
```

Both of these statements instruct Cassandra to set the value of the `title`
column in row 1 to "Post".

Cequel::Model uses the `INSERT` statement to persist objects that have been
newly initialized in memory, and the `UPDATE` statement to save changes to
objects that were loaded out of Cassandra. There is no particular reason for
this; it just feels right. But beware: you may think you're inserting a new row
when you're actually overwriting data that already exists in that row:

``` ruby
# I'm just creating a post here.
post1 = Post.new(:id => 1, :title => 'My Post', :blog_id => 1)
post1.save!

# And let's make another one
post2 = Post.new(:id => 1, :title => 'Another Post')
post2.save!
```

Living in a relational world, we'd expect the second statement to throw an
error because row 1 already exists. But not Cassandra: the above code will just
overwrite the `title` in that row. Note that the `blog_id` will not be touched;
upserts only work on the columns that are given.

#### Dirty Updates ####

Cequel::Model includes ActiveModel's dirty tracking. When you save a persisted
model, only columns that have changed in memory will be included in the `UPDATE`
statement.

Note that updating a model may generate two CQL statements. This is because
Cassandra does not have a concept of null values; a column either has data or it
doesn't. So, if you change an attribute of your model from a non-nil value to
`nil`, Cequel::Model will issue a DELETE statement just for the column(s) in
question.

If you don't change anything, calling '#save' on a persisted model is a no-op.

#### Pondering Existence ####

In a relational database, there is a well-defined concept of existence; there is
either a row for a given primary key or there isn't. It's possible to have a row
consisting of only a primary key, and that row still "exists" in a meaningful
way.

Cassandra works more like a key-value store: each key either has data, or it
doesn't, but beyond that there is no explicit concept of a key or row existing.
Semantically, we can think of a Cassandra row existing if it has data in any
column. But that's a concept that only exists in our minds (and in Cequel), not
in the database itself. Consider the following:

``` ruby
cassandra[:posts].where(:id => 1).first
#=> {'id' => 1}
```

The above behavior will hold even if no data has ever been written to key 1. It
will also happen if key 1 existed at one time and then was deleted.

This behavior is complicated by "range ghosts". Range ghosts happen when you
delete all the data from a row. You'll only see them when performing unlimited
or key-range queries, and they go away after a while. There's a good reason for
this, but it's confusing. For instance, let's say in the entire history of our
database, all we've done is create post 1, and then delete it. Let's see what
happens when we select all posts:

``` ruby
cassandra[:posts].to_a
#=> [{'id' => 1}]
```

That's a range ghost: it's a result row consisting of only the key.

Cequel::Model makes explicit our implicit semantic idea that rows only exist if
they have data in a column (not counting the key, which isn't really a column).
So any time Cequel::Model sees a row that's either empty or only has a key, it
drops it. You'll never get back a model instance containing data in no non-key
columns.

If you perform a `#find` and get back no non-key data, the library will raise
`Cequel::Model::RecordNotFound`.

This behavior can especially trip you up when you are selecting specific
columns. For instance, let's say post 1 only has data in the `title` field:

``` ruby
Post.find(uuid)
# Gives me back a nice post object

Post.select(:blog_id).find(uuid)
# Raises Cequel::Model::RecordNotFound, because there was no data in the row

Post.select(:id).find(uuid)
# Fails fast before any interaction with Cassandra: this is a meaningless query
```

#### Key and Secondary Index Selection ####

CQL gives you a few ways to filter the rows you want returned in a query:

* A single key
* A list of keys
* A range of keys
* A secondary index
* A secondary index combined with one or more filters

That's it. You can't filter by:

* A non-indexed column
* A key/list of keys combined with a secondary index
* A key/list of keys combined with a filter

So let's say our `posts` column family has a secondary index on `blog_id` and
nothing else. These will work:

``` ruby
Post.find(uuid)
Post.find([uuid1, uuid2])
Post.where('id > ?', uuid)
Post.find_by_blog_id(blog_id)
Post.where(:blog_id => blog_id).where('created_at > ?', 1.day.ago)
```

These won't work:

``` ruby
Post.where('created_at > ?', 1.day.ago)
Post.where(:id => uuid, :blog_id => blog_id)
Post.where(:id => uuid).where('created_at > ?, 1.day.ago)
```

## Cequel::Model::Dictionary ##

The functionality of the Cequel::Model class maps the "skinny row" style of
column family structure: each row has a small set of predefined columns, with
heterogeneous value types. However, the "wide row" structure will also play an
important role in most Cassandra schemas (if this is news to you, I recommend
reading
[this article](http://www.rackspace.com/blog/cassandra-by-example/?072d7a80)).
Cequel provides the `Cequel::Model::Dictionary` class, which abstracts wide rows
as a dictionary object, behaving much like a Hash.

Applications should define subclasses of the `Dictionary` class to interact with
data in a certain column family. For instance, let's say I've got a `blog_posts`
column family:

``` ruby
class BlogPosts < Cequel::Model::Dictionary

  key :blog_id, :uuid
  maps :uuid => :text

  private

  def serialize_value(column, value)
    value.to_json
  end

  def deserialize_value(column, value)
    JSON.parse(value)
  end

end
```

In this case, your column family has a key with alias `blog_id` of type `uuid`,
comparator of type `uuid`, and default validation of type `text`. The
`serialize_value` and `deserialize_value` methods are optional, but aid with the
common pattern of storing blobs of JSON, msgpack, etc. in wide-row values.

### Reading data ###

To grab a handle to a dictionary, use the bracket operator:

``` ruby
posts = BlogPosts[blog_id]
```

This does not perform any queries against Cassandra; it just gives you an object
pointing at a particular row. By default, reads are lazy:

``` ruby
post_json = posts[post_id]
```

This will select a single column from the `blog_posts` column family and return
its deserialized value. The value is not retained in the dictionary itself.

If you want to work with the entire contents of the wide row in memory, use the
`#load` method:

``` ruby
posts = BlogPosts[blog_id]
posts.load # loads all values into memory
posts[post_id] # doesn't do an additional query
```

Dictionaries expose the major read methods of the Hash interface:

``` ruby
posts.each_pair { |column, value| do_something(column, value) }
posts.keys
posts.values
posts.map { |column, value| transform(column, value) }
posts.slice(uuid1, uuid2, uuid3) # returns a Hash
```

All of the above methods will read from Cassandra if the dictionary is unloaded,
and read from memory if the dictionary is loaded. Note that for methods that
read all columns out of the database, columns will be loaded in batches of 1000
by default.

### Writing Data ###

Modifying data is, unsurprisingly, done using the `[]=` operator. When you call
`#save`, any keys that you have modified with the `[]=` operator will be
persisted to Cassandra. The dictionary does not use true dirty tracking, in the
sense that it will write an attribute even if you set it to the same value it
had previously.

Write behavior is the same regardless of loaded status.

### Loading data in bulk ###

Dictionaries implement the `::load` method, which allows you to read multiple
rows at once. Unlike the `#each` and `#load` methods, `::load` will not attempt
to paginate over very wide rows (10K+ columns); if your rows are very wide, you
will probably want to load them one at a time anyway.

```ruby
post_rows = BlogPosts.load(blog1_id, blog2_id) # load rows at key blog1_id, blog2_id
post_rows = BlogPosts.load(blog1_id, blog2_id) # wider rows
```

### Counters ###

Counters are a special type of Cassandra column that implements a consistent
distributed counter. The only write operations that are possible on counter
columns are increment and decrement (they can be deleted, technically, but the
behavior is undesirable). You can create a counter dictionary using the
`Cequel::Model::Counter` class:

```ruby
class CommentCounts < Cequel::Model::Counter
  key :blog_id, :int
  columns :uuid # values are always of 'counter' type
end
```

For read operations, counters work exactly like normal dictionaries. For write
operations, counters have `#increment` and `#decrement` methods available
(but not `#[]=`):

```ruby
comment_counts = CommentCounts[blog_id]
comment_counts.increment(post_id) # increment by 1
comment_counts.increment(post_id, 4) # increment by 4
comment_counts.increment([post1_id, post2_id], 3) # increment multiple at once
comment_counts.increment(post1_id => 2, post2_id => 4) # by different values
comment_counts.decrement(post_id) # accepts all the same forms as #increment
```

## Road Map ##

As mentioned previously in this document, there are considerable differences
between modeling data in Cassandra and modeling data in a relational database,
despite their superficial similarities. In Cassandra, wide rows are an important
part of schema design; "existence" is a fuzzy concept; denormalization is often
a good idea; secondary indexes are of limited use. Broadly, the goal for future
versions of Cequel is to provide a more robust abstraction and tool kit for
modeling data in Cassandra the right way. Specifically, here are some things to
look forward to in future Cequel versions:

* Support for auto-migrations by introspecting the schema and making
  modifications to fit the model-defined schema.
* One-one relationships using multiple classes per column family.
* Additional wide-row data structures: lists and sets.
* Tighter integration between Cequel::Model and Cequel::Model::Dictionary;
  `references_many` associations.
* Bidirectional associations.
* Using defined column types to ensure objects passed to CassandraCQL layer are
  of the correct type/encoding.

## Support & Bugs ##

If you find a bug, feel free to
[open an issue](https://github.com/brewster/cequel/issues/new) on GitHub.
Pull requests are most welcome.

For questions or feedback, hit up our mailing list at
[cequel@groups.google.com](http://groups.google.com/group/cequel)
or find outoftime in the #cassandra IRC channel on Freenode.

## License ##

Cequel is distributed under the MIT license. See the attached LICENSE for all
the sordid details.
