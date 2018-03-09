# How to contribute #

Contributions to Cequel are highly welcome! Here's a quick guide.

## Submitting a change ##

1. Fork the repo and create a topic branch
2. Set up your environment and run the tests. The easiest way to do this is to
   use Docker; [see below](#running-the-tests). For those who already have a suitable Cassandra
   instance running locally: `rake test`
3. Add tests for your change.
4. Make the tests pass.
5. Push to your topic branch and submit a pull request.

### Do's and don'ts ###

* **Do** write tests. If you don't test your patch, I'll have to write tests
  for it, which is likely to delay the pull request getting accepted.
* **Do** write documentation for new functionality and update documentation for
  changed functionality. Cequel uses
  [YARD](http://rubydoc.info/gems/yard/file/docs/GettingStarted.md) for
  documentation. If you're adding a significant new feature, update the
  `README` too.
* **Do** use code style consistent with the project. Cequel by and large
  follows the [Ruby Style Guide](https://github.com/bbatsov/ruby-style-guide).
* **Don't** make changes to the `cequel.gemspec` or `version.rb` files, except
  to add new dependencies in the former.

## Running the tests ##

### For the impatient ###

[Install Docker](https://docs.docker.com/engine/installation/) first, then run tests:
```bash
git clone git@github.com:yourname/cequel.git
cd cequel
git remote add upstream git@github.com:cequel/cequel.git
bundle install
bundle exec rake test
```

### Using Docker

Cequel's test suite runs against a live Cassandra instance. The easiest way to
get one is to use Docker, `docker run --rm -p 9042:9042 cassandra`.

### Using different ports

You can configure the cequel test suite to use a different port by setting the `CEQUEL_TEST_PORT` environment variable. Example:
1. `docker run --rm -p 33333:9042 cassandra` in one terminal
1. `rake test CEQUEL_TEST_PORT=33333` in another termainal

### Cassandra versions

Cequel is tested against a large range of Ruby, Rails, and Cassandra
versions; for most patches, you can just run the tests using the
latest version of all of them. If you're messing with the
`Cequel::Schema` or `Cequel::Type` modules, you'll want to test at
least against the first and latest releases of 2.1, 2.2 and 3 series.

## And finally

**THANK YOU!**
