require 'yaml'
require 'bundler/setup'
require 'rspec/core/rake_task'
require 'rubocop/rake_task'
require 'wwtd/tasks'
require 'travis'
require File.expand_path('../lib/cequel/version', __FILE__)

RUBY_VERSIONS = YAML.load_file(File.expand_path('../.travis.yml', __FILE__))['rvm']

task :default => :release
task :release => [
  :verify_changelog,
  :rubocop,
  :"test:all",
  :build,
  :tag,
  :update_stable,
  :push,
  :cleanup
]

desc 'Build gem'
task :build do
  system 'gem build cequel.gemspec'
end

desc 'Create git release tag'
task :tag do
  system "git tag -a -m 'Version #{Cequel::VERSION}' #{Cequel::VERSION}"
  system "git push git@github.com:cequel/cequel.git #{Cequel::VERSION}:#{Cequel::VERSION}"
end

desc 'Update stable branch on GitHub'
task :update_stable do
  if Cequel::VERSION =~ /^(\d+\.)+\d+$/ # Don't push for prerelease
    system "git push -f origin HEAD:stable"
  end
end

desc 'Push gem to repository'
task :push do
  system "gem push cequel-#{Cequel::VERSION}.gem"
end

task 'Remove packaged gems'
task :cleanup do
  system "rm -v *.gem"
end

desc 'Run the specs'
RSpec::Core::RakeTask.new(:test) do |t|
  t.pattern = './spec/examples/**/*_spec.rb'
  rspec_opts = '--backtrace'
  version = File.basename(File.dirname(RbConfig::CONFIG['bindir']))
  gemfile = ENV.fetch('BUNDLE_GEMFILE', 'Gemfile')
  log_path = File.expand_path("../spec/log/#{Time.now.to_i}-#{version}-#{File.basename(gemfile, '.gemfile')}", __FILE__)
  FileUtils.mkdir_p(File.dirname(log_path))
  File.open(log_path, 'w') do |f|
    f.puts "RBENV_VERSION=#{version} BUNDLE_GEMFILE=#{gemfile} bundle exec rake test"
  end
  rspec_opts << " --out='#{log_path}' --format=progress"
  t.rspec_opts = rspec_opts
end

desc 'Check style with Rubocop'
RuboCop::RakeTask.new(:rubocop) do |task|
  task.patterns = ['lib/**/*.rb']
  task.formatters = ['files']
  task.fail_on_error = true
end

namespace :test do
  desc 'Run the specs with progress formatter'
  RSpec::Core::RakeTask.new(:concise) do |t|
    t.pattern = './spec/examples/**/*_spec.rb'
    t.rspec_opts = '--fail-fast --format=progress'
    t.fail_on_error = true
  end

  task :all do
    travis = Travis::Repository.find('cequel/cequel')
    current_commit = `git rev-parse HEAD`.chomp
    build = travis.builds.find { |build| build.commit.sha == current_commit }
    if build.nil?
      puts "Could not find build for #{current_commit}; running tests locally"
      abort unless system('bundle', 'exec', 'wwtd', '--parallel')
    elsif !build.finished?
      puts "Build for #{current_commit} is not finished; running tests locally"
      abort unless system('bundle', 'exec', 'wwtd', '--parallel')
    elsif build.green?
      puts "Travis build for #{current_commit} is green; skipping local tests"
    else
      abort "Travis build for #{current_commit} failed; canceling release"
    end
  end
end

desc 'Update changelog'
task :changelog do
  require './lib/cequel/version.rb'

  last_tag = `git tag`.each_line.map(&:strip).last
  existing_changelog = File.read('./CHANGELOG.md')
  File.open('./CHANGELOG.md', 'w') do |f|
    f.puts "## #{Cequel::VERSION}"
    f.puts ""
    f.puts `git log --no-merges --pretty=format:'* %s' #{last_tag}..`
    f.puts ""
    f.puts existing_changelog
  end
end

task :verify_changelog do
  require './lib/cequel/version.rb'

  if File.read('./CHANGELOG.md').each_line.first.strip != "## #{Cequel::VERSION}"
    abort "Changelog is not up-to-date."
  end
end

namespace :cassandra do
  namespace :versions do
    desc 'Update list of available Cassandra versions'
    task :update do
      listing = Net::HTTP.get(URI.parse("http://archive.apache.org/dist/cassandra/"))
      versions = listing.scan(%r(href="(\d+\.\d+\.\d+)/")).map(&:first)
      File.open(File.expand_path('../.cassandra-versions', __FILE__), 'w') do |f|
        f.puts(versions.sort_by(&Gem::Version.method(:new)).join("\n"))
      end
    end
  end
end
