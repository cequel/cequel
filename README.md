# Cequel #

Cequel is a Ruby ORM for [Cassandra](http://cassandra.apache.org/) using
[CQL3](http://www.datastax.com/documentation/cql/3.0/webhelp/index.html).

[![Gem Version](https://badge.fury.io/rb/cequel.png)](http://badge.fury.io/rb/cequel)
[![Build Status](https://travis-ci.org/cequel/cequel.png?branch=master)](https://travis-ci.org/cequel/cequel)
[![Dependency Status](https://gemnasium.com/cequel/cequel.png)](https://gemnasium.com/cequel/cequel)
[![Code Climate](https://codeclimate.com/github/cequel/cequel.png)](https://codeclimate.com/github/cequel/cequel)
[![Inline docs](http://inch-ci.org/github/cequel/cequel.png)](http://inch-ci.org/github/cequel/cequel)

`Cequel::Record` is an ActiveRecord-like domain model layer that exposes
the robust data modeling capabilities of CQL3, including parent-child
relationships via compound primary keys and collection columns.

The lower-level `Cequel::Metal` layer provides a CQL query builder interface
inspired by the excellent [Sequel](http://sequel.rubyforge.org/) library.

## Installation ##

Add it to your Gemfile:

``` ruby
gem 'cequel'
```

### Rails integration ###

Cequel does not require Rails, but if you are using Rails, you
will need version 3.2+. Cequel::Record will read from the configuration file
`config/cequel.yml` if it is present. You can generate a default configuarion
file with:

```bash
rails g cequel:configuration
```

Once you've got things configured (or decided to accept the defaults), run this
to create your keyspace (database):

```bash
rake cequel:keyspace:create
```

## Setting up Models ##

Unlike in ActiveRecord, models declare their properties inline. We'll start with
a simple `Blog` model:

```ruby
class Blog
  include Cequel::Record

  key :subdomain, :text
  column :name, :text
  column :description, :text
end
```

Unlike a relational database, Cassandra does not have auto-incrementing primary
keys, so you must explicitly set the primary key when you create a new model.
For blogs, we use a natural key, which is the subdomain. Another option is to
use a UUID.

### Compound keys and parent-child relationships ###

While Cassandra is not a relational database, compound keys do naturally map
to parent-child relationships. Cequel supports this explicitly with the
`has_many` and `belongs_to` relations. Let's create a model for posts that acts
as the child of the blog model:

```ruby
class Post
  include Cequel::Record
  belongs_to :blog
  key :id, :timeuuid, auto: true
  column :title, :text
  column :body, :text
end
```

The `auto` option for the `key` declaration means Cequel will initialize new
records with a UUID already generated. This option is only valid for `:uuid` and
`:timeuuid` key columns.

Note that the `belongs_to` declaration must come *before* the `key` declaration.
This is because `belongs_to` defines the
[partition key](http://www.datastax.com/documentation/cql/3.0/webhelp/index.html#cql/ddl/../../cassandra/glossary/gloss_glossary.html#glossentry_dhv_s24_bk); the `id` column is
the [clustering column](http://www.datastax.com/documentation/cql/3.0/webhelp/index.html#glossentry_h31_xjk_bk).

Practically speaking, this means that posts are accessed using both the
`blog_subdomain` (automatically defined by the `belongs_to` association) and the
`id`. The most natural way to represent this type of lookup is using a
`has_many` association. Let's add one to `Blog`:

```ruby
class Blog
  include Cequel::Record

  key :subdomain, :text
  column :name, :text
  column :description, :text

  has_many :posts
end
```

Now we might do something like this:

```ruby
class PostsController < ActionController::Base
  def show
    Blog.find(current_subdomain).posts.find(params[:id])
  end
end
```

### Timestamps ###

If your final primary key column is a `timeuuid` with the `:auto` option set,
the `created_at` method will return the time that the UUID key was generated.

To add timestamp columns, simply use the `timestamps` class macro:

```ruby
class Blog
  key :subdomain, :text
  column :name, :text
  timestamps
end
```

This will automatically define `created_at` and `updated_at` columns, and
populate them appropriately on save.

If the creation time can be extracted from the primary key as outlined above,
this method will be preferred and no `created_at` column will be defined.

### Schema synchronization ###

Cequel will automatically synchronize the schema stored in Cassandra to match
the schema you have defined in your models. If you're using Rails, you can
synchronize your schemas for everything in `app/models` by invoking:

```bash
rake cequel:migrate
```

### Record sets ###

Record sets are lazy-loaded collections of records that correspond to a
particular CQL query. They behave similarly to ActiveRecord scopes:

```ruby
Post.select(:id, :title).reverse.limit(10)
```

To scope a record set to a primary key value, use the `[]` operator. This will
define a scoped value for the first unscoped primary key in the record set:

```ruby
Post['bigdata'] # scopes posts with blog_subdomain="bigdata"
```

You can pass multiple arguments to the `[]` operator, which will generate an
`IN` query:

```ruby
Post['bigdata', 'nosql'] # scopes posts with blog_subdomain IN ("bigdata", "nosql")
```

To select ranges of data, use `before`, `after`, `from`, `upto`, and `in`. Like
the `[]` operator, these methods operate on the first unscoped primary key:

```ruby
Post['bigdata'].after(last_id) # scopes posts with blog_subdomain="bigdata" and id > last_id
```

You can also use `where` to scope to primary key columns, but a primary key
column can only be scoped if all the columns that come before it are also
scoped:

```ruby
Post.where(blog_subdomain: 'bigdata') # this is fine
Post.where(blog_subdomain: 'bigdata', permalink: 'cassandra') # also fine
Post.where(blog_subdomain: 'bigdata').where(permalink: 'cassandra') # also fine
Post.where(permalink: 'cassandra') # bad: can't use permalink without blog_subdomain
```

Note that record sets always load records in batches; Cassandra does not support
result sets of unbounded size. This process is transparent to you but you'll see
multiple queries in your logs if you're iterating over a huge result set.

#### Time UUID Queries ####

CQL has [special handling for the `timeuuid`
type](http://www.datastax.com/documentation/cql/3.0/webhelp/index.html#cql/cql_reference/cql_data_types_c.html#reference_ds_axc_xk5_yj),
which allows you to return a rows whose UUID keys correspond to a range of
timestamps. 

Cequel automatically constructs timeuuid range queries if you pass a `Time`
value for a range over a `timeuuid` column. So, if you want to get the posts
from the last day, you can run:

```ruby
Blog['myblog'].posts.from(1.day.ago)
```

### Updating records ###

When you update an existing record, Cequel will only write statements to the
database that correspond to explicit modifications you've made to the record in
memory. So, in this situation:

```ruby
@post = Blog.find(current_subdomain).posts.find(params[:id])
@post.update_attributes!(title: "Announcing Cequel 1.0")
```

Cequel will only update the title column. Note that this is not full dirty
tracking; simply setting the title on the record will signal to Cequel that you
want to write that attribute to the database, regardless of its previous value.

### Unloaded models ###

In the above example, we call the familiar `find` method to load a blog and then
one of its posts, but we didn't actually do anything with the data in the Blog
model; it was simply a convenient object-oriented way to get a handle to the
blog's posts. Cequel supports unloaded models via the `[]` operator; this will
return an **unloaded** blog instance, which knows the value of its primary key,
but does not read the row from the database. So, we can refactor the example to
be a bit more efficient:

```ruby
class PostsController < ActionController::Base
  def show
    @post = Blog[current_subdomain].posts.find(params[:id])
  end
end
```

If you attempt to access a data attribute on an unloaded class, it will
lazy-load the row from the database and become a normal loaded instance.

You can generate a collection of unloaded instances by passing multiple
arguments to `[]`:

```ruby
class BlogsController < ActionController::Base
  def recommended
    @blogs = Blog['cassandra', 'nosql']
  end
end
```

The above will not generate a CQL query, but when you access a property on any
of the unloaded `Blog` instances, Cequel will load data for all of them with
a single query. Note that CQL does not allow selecting collection columns when
loading multiple records by primary key; only scalar columns will be loaded.

There is another use for unloaded instances: you may set attributes on an
unloaded instance and call `save` without ever actually reading the row from
Cassandra. Because Cassandra is optimized for writing data, this "write without
reading" pattern gives you maximum efficiency, particularly if you are updating
a large number of records.

### Collection columns ###

Cassandra supports three types of collection columns: lists, sets, and maps.
Collection columns can be manipulated using atomic collection mutation; e.g.,
you can add an element to a set without knowing the existing elements. Cequel
supports this by exposing collection objects that keep track of their
modifications, and which then persist those modifications to Cassandra on save.

Let's add a category set to our post model:


```ruby
class Post
  include Cequel::Record

  belongs_to :blog
  key :id, :uuid
  column :title, :text
  column :body, :text
  set :categories, :text
end
```

If we were to then update a post like so:

```ruby
@post = Blog[current_subdomain].posts[params[:id]]
@post.categories << 'Kittens'
@post.save!
```

Cequel would send the CQL equivalent of "Add the category 'Kittens' to the post
at the given `(blog_subdomain, id)`", without ever reading the saved value of
the `categories` set.

### Secondary indexes ###

Cassandra supports secondary indexes, although with notable restrictions:

* Only scalar data columns can be indexed; key columns and collection columns
  cannot.
* A secondary index consists of exactly one column.
* Though you can have more than one secondary index on a table, you can only use
  one in any given query.

Cequel supports the `:index` option to add secondary indexes to column
definitions:

```ruby
class Post
  include Cequel::Record

  belongs_to :blog
  key :id, :uuid
  column :title, :text
  column :body, :text
  column :author_id, :uuid, :index => true
  set :categories, :text
end
```

Defining a column with a secondary index adds several "magic methods" for using
the index:

```ruby
Post.with_author_id(id) # returns a record set scoped to that author_id
Post.find_by_author_id(id) # returns the first post with that author_id
Post.find_all_by_author_id(id) # returns an array of all posts with that author_id
```

You can also call the `where` method directly on record sets:

```ruby
Post.where(:author_id, id)
```

### Consistency tuning ###

Cassandra supports [tunable
consistency](http://www.datastax.com/documentation/cassandra/2.0/cassandra/dml/dml_config_consistency_c.html),
allowing you to choose the right balance between query speed and consistent
reads and writes. Cequel supports consistency tuning for reads and writes:

```ruby
Post.new(id: 1, title: 'First post!').save!(consistency: :all)

Post.consistency(:one).find_each { |post| puts post.title }
```

Both read and write consistency default to `QUORUM`.

### ActiveModel Support ###

Cequel supports ActiveModel functionality, such as callbacks, validations,
dirty attribute tracking, naming, and serialization. If you're using Rails 3,
mass-assignment protection works as usual, and in Rails 4, strong parameters are
treated correctly. So we can add some extra ActiveModel goodness to our post
model:

```ruby
class Post
  include Cequel::Record

  belongs_to :blog
  key :id, :uuid
  column :title, :text
  column :body, :text

  validates :body, presence: true

  after_save :notify_followers
end
```

Note that validations or callbacks that need to read data attributes will cause
unloaded models to load their row during the course of the save operation, so if
you are following a write-without-reading pattern, you will need to be careful.

Dirty attribute tracking is only enabled on loaded models.

## Upgrading from Cequel 0.x ##

Cequel 0.x targeted CQL2, which has a substantially different data
representation from CQL3. Accordingly, upgrading from Cequel 0.x to Cequel 1.0
requires some changes to your data models.

### Upgrading a Cequel::Model ###

Upgrading from a `Cequel::Model` class is fairly straightforward; simply add the
`compact_storage` directive to your class definition:

```ruby
# Model definition in Cequel 0.x
class Post
  include Cequel::Model

  key :id, :uuid
  column :title, :text
  column :body, :text
end

# Model definition in Cequel 1.0
class Post
  include Cequel::Record

  key :id, :uuid
  column :title, :text
  column :body, :text

  compact_storage
end
```

Note that the semantics of `belongs_to` and `has_many` are completely different
between Cequel 0.x and Cequel 1.0; if you have data columns that reference keys
in other tables, you will need to hand-roll those associations for now.

### Upgrading a Cequel::Model::Dictionary ###

CQL3 does not have a direct "wide row" representation like CQL2, so the
`Dictionary` class does not have a direct analog in Cequel 1.0. Instead, each
row key-map key-value tuple in a `Dictionary` corresponds to a single row in
CQL3. Upgrading a `Dictionary` to Cequel 1.0 involves defining two primary keys
and a single data column, again using the `compact_storage` directive:

``` ruby
# Dictionary definition in Cequel 0.x
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

# Equivalent model in Cequel 1.0
class BlogPost
  include Cequel::Record

  key :blog_id, :uuid
  key :id, :uuid
  column :data, :text

  compact_storage

  def data
    JSON.parse(read_attribute(:data))
  end

  def data=(new_data)
    write_attribute(:data, new_data.to_json)
  end
end
```

`Cequel::Model::Dictionary` did not infer a pluralized table name, as
`Cequel::Model` did and `Cequel::Record` does. If your legacy `Dictionary`
table has a singlar table name, add a `self.table_name = :blog_post` in the
model definition.

Note that you will want to run `::synchronize_schema` on your models when
upgrading; this will not change the underlying data structure, but will add some
CQL3-specific metadata to the table definition which will allow you to query it.

### CQL Gotchas ###

CQL is designed to be immediately familiar to those of us who are used to
working with SQL, which is all of us. Cequel advances this spirit by providing
an ActiveRecord-like mapping for CQL. However, Cassandra is very much not a
relational database, so some behaviors can come as a surprise. Here's an
overview.

#### Upserts ####

Perhaps the most surprising fact about CQL is that `INSERT` and `UPDATE` are
essentially the same thing: both simply persist the given column data at the
given key(s). So, you may think you are creating a new record, but in fact
you're overwriting data at an existing record:

``` ruby
# I'm just creating a blog here.
blog1 = Blog.create!(
  subdomain: 'big-data',
  name: 'Big Data',
  description: 'A blog about all things big data')

# And another new blog.
blog2 = Blog.create!(
  subdomain: 'big-data',
  name: 'The Big Data Blog')
```

Living in a relational world, we'd expect the second statement to throw an
error because the row with key 'big-data' already exists. But not Cassandra: the
above code will just overwrite the `name` in that row.  Note that the
`description` will not be touched by the second statement; upserts only work on
the columns that are given.

## Compatibility ##

### Rails ###

* 4.2
* 4.1
* 4.0
* 3.2

### Ruby ###

* Ruby 2.2, 2.1, 2.0
* JRuby 1.7
* Rubinius 2.5

### Cassandra ###

* 1.2
* 2.0

Though Cequel is tested against Cassandra 2.0, it does not at this time support
any of the CQL3.1 features introduced in Cassandra 2.0. This will change in the
future.

## Support & Bugs ##

If you find a bug, feel free to
[open an issue](https://github.com/cequel/cequel/issues/new) on GitHub.
Pull requests are most welcome.

For questions or feedback, hit up our mailing list at
[cequel@groups.google.com](http://groups.google.com/group/cequel)
or find outoftime in the #cassandra IRC channel on Freenode.

## Contributing ##

See
[CONTRIBUTING.md](https://github.com/cequel/cequel/blob/master/CONTRIBUTING.md)

## Credits ##

Cequel was written by:

* Mat Brown
* Aubrey Holland
* Keenan Brock
* Insoo Buzz Jung
* Louis Simoneau
* Peter Williams
* Kenneth Hoffman
* Antti Tapio
* Ilya Bazylchuk
* Dan Cardamore
* Kei Kusakari
* Oleh Novosad
* John Smart
* Angelo Lakra
* Olivier Lance
* Tomohiro Nishimura
* Masaki Takahashi
* G Gordon Worley III
* Clark Bremer
* Tamara Temple
* Long On
* Lucas Mundim

Special thanks to [Brewster](https://www.brewster.com), which supported the 0.x
releases of Cequel.

## Shameless Self-Promotion ##

If you're new to Cassandra, check out [Learning Apache
Cassandra](http://www.amazon.com/gp/product/1783989203/ref=s9_simh_co_p14_d4_i1?pf_rd_m=ATVPDKIKX0DER&pf_rd_s=left-1&pf_rd_r=1TX356WHGF06W32ZHD8S&pf_rd_t=3201&pf_rd_p=1953562742&pf_rd_i=typ01),
a hands-on guide to Cassandra application development by example, written by
the maintainer of Cequel.

## License ##

Cequel is distributed under the MIT license. See the attached LICENSE for all
the sordid details.
