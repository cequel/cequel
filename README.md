# Cequel #

Cequel is a Ruby ORM for [Cassandra](http://cassandra.apache.org/) using
[CQL3](http://www.datastax.com/documentation/cql/3.0/webhelp/index.html).

<!--[![Build Status](https://secure.travis-ci.org/outoftime/cequel.png)](http://travis-ci.org/outoftime/cequel) -->

`Cequel::Model` is an ActiveRecord-like domain model layer that exposes
the robust data modeling capabilities of CQL3, including parent-child
relationships via compound primary keys and collection columns.

The lower-level `Cequel::Metal` layer provides a CQL query builder interface
inspired by the excellent [Sequel](http://sequel.rubyforge.org/) library.

## Installation ##

Add it to your Gemfile:

``` ruby
gem 'cequel', '1.0.0.pre.1', require: 'cequel/model'
```

### Rails integration ###

Cequel does not require Rails, but if you are using Rails, you
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

## Setting up Models ##

Unlike in ActiveRecord, models declare their properties inline. We'll start with
a simple `Blog` model:

```ruby
class Blog < Cequel::Model::Base
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
class Post < Cequel::Model::Base
  belongs_to :blog
  key :id, :uuid
  column :title, :text
  column :body, :text
end
```

Note that the `belongs_to` declaration must come *before* the `key` declaration.
This is because `belongs_to` defines the
[partition key](http://www.datastax.com/documentation/cql/3.0/webhelp/index.html#cql/ddl/../../cassandra/glossary/gloss_glossary.html#glossentry_dhv_s24_bk); the `id` column is
the [clustering column](http://www.datastax.com/documentation/cql/3.0/webhelp/index.html#glossentry_h31_xjk_bk).

Practically speaking, this means that posts are accessed using both the
`blog_subdomain` (automatically defined by the `belongs_to` association) and the
`id`. The most natural way to represent this type of lookup is using a
`has_many` association. Let's add one to `Blog`:

```ruby
class Blog < Cequel::Model::Base
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

### Schema synchronization ###

Cequel will automatically synchronize the schema stored in Cassandra to match
the schema you have defined in your models. Synchronizing your schema for a
model is as simple as:

```ruby
Blog.synchronize_schema
Post.synchronize_schema
```

### Record sets ###

Record sets are lazy-loaded collections of records that correspond to a
particular CQL query. They behave similarly to ActiveRecord scopes:

```ruby
Post.select(:id, :title).reverse.limit(10)
```

To scope a record set to a primary key value, use the `at` method. This will
define a scoped value for the first unscoped primary key in the record set:

```ruby
Post.at('bigdata') # scopes posts with blog_subdomain="bigdata"
```

To select ranges of data, use `before`, `after`, `from`, `upto`, and `in`. Like
the `at` method, these methods operate on the first unscoped primary key:

```ruby
Post.at('bigdata').after(last_id) # scopes posts with blog_subdomain="bigdata" and id > last_id
```

Note that record sets always load records in batches; Cassandra does not support
result sets of unbounded size. This process is transparent to you but you'll see
multiple queries in your logs if you're iterating over a huge result set.

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
    Blog[current_subdomain].posts.find(params[:id])
  end
end
```

If you attempt to access a data attribute on an unloaded class, it will
lazy-load the row from the database and become a normal loaded instance.

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
class Post < Cequel::Model::Base
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
class Post < Cequel::Model::Base
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

Note that `where` is only for 

### ActiveModel Support ###

Cequel supports ActiveModel functionality, such as callbacks, validations,
dirty attribute tracking, naming, and serialization. If you're using Rails 3,
mass-assignment protection works as usual, and in Rails 4, strong parameters are
treated correctly. So we can add some extra ActiveModel goodness to our post
model:

```ruby
class Post < Cequel::Model::Base
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

* 4.0
* 3.2
* 3.1

### Ruby ###

* 2.0
* 1.9.3
* Rubinius 1.0 in 1.9 mode

### Cassandra ###

* Cassandra 1.2

## Support & Bugs ##

If you find a bug, feel free to
[open an issue](https://github.com/cequel/cequel/issues/new) on GitHub.
Pull requests are most welcome.

For questions or feedback, hit up our mailing list at
[cequel@groups.google.com](http://groups.google.com/group/cequel)
or find outoftime in the #cassandra IRC channel on Freenode.

## Credits ##

Cequel was written by:

* Mat Brown
* Aubrey Holland
* Keenan Brock
* Insoo Buzz Jung
* Randy Meech

Special thanks to [Brewster](https://www.brewster.com), which supported the 0.x
releases of Cequel.

## License ##

Cequel is distributed under the MIT license. See the attached LICENSE for all
the sordid details.
