# How to contribute #

Contributions to Cequel are highly welcome! Here's a quick guide.

## Submitting a change ##

1. Fork the repo and create a topic branch
2. Set up your environment and run the tests. The easiest way to do this is to
   use Vagrant; see below. For those who already have a suitable Cassandra
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

```bash
$ git clone git@github.com:yourname/cequel.git
$ cd cequel
$ git remote add upstream git@github.com:cequel/cequel.git
$ brew tap phinze/cask
$ brew install brew-cask
$ brew cask install virtualbox vagrant
$ vagrant up 2.2.5
$ rake test
```

### Using Vagrant

Cequel's test suite runs against a live Cassandra instance. The easiest way to
get one is to use the `Vagrantfile` included in the repo. You'll need to
install [VirtualBox](https://www.virtualbox.org/) and
[Vagrant](http://www.vagrantup.com/); both are available via
[Homebrew-cask](https://github.com/phinze/homebrew-cask) if you're on OS X.

Cequel's Vagrantfile can generate a virtual machine for any Cassandra version
that Cequel supports (i.e., 2.1.x & 2.2.x). You can run multiple VMs at the
same time; the first machine you boot will expose its Cassandra instance on
port `9042`, which is the default port that Cequel will look for.

Cequel is tested against a large range of Ruby, Rails, and Cassandra versions;
for most patches, you can just run the tests using the latest version of all of
them. If you're messing with the `Cequel::Schema` or `Cequel::Type` modules,
you'll want to test at least against an early 2.1 release, a
later 2.1 release (2.1.13), and the latest 2.2 release.

## And finally

**THANK YOU!**
