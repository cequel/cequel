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

Cequel's test suite, including a development bash environment, container, and Cassandra
instance is setup for use through Docker Compose.

The local folder is mapped into the docker container. So, you can use your IDE of choice
to make edits, leveraging the docker-compose to provide Cassandra and an lightweight container for
running tests.

To use, update the docker-compose.yml with your personal details (for Git compatibility) and then:
```bash
docker-compose run dev
```
This will drop you to a bash prompt in the `/cequel/` folder.  From there, you can run
tests using familiar RSpec commands.

### Cassandra versions

Cequel is tested against a large range of Ruby, Rails, and Cassandra
versions; for most patches, you can develop the tests using the
latest version of all of them. If you're messing with the
`Cequel::Schema` or `Cequel::Type` modules, you'll want to test at
least against the first and latest releases of 2.1, 2.2 and 3 series.

If want to use a specific version of Cassandra in development do this: 

```bash
docker-compose down
CASSANDRA_VERSION=3.10 docker-compose run dev
```

## And finally

**THANK YOU!**
