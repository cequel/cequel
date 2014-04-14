require 'yaml'
require 'bundler/setup'
require 'rspec/core/rake_task'
require 'rubocop/rake_task'
require 'appraisal'
require File.expand_path('../lib/cequel/version', __FILE__)

RUBY_VERSIONS = YAML.load_file(File.expand_path('../.travis.yml', __FILE__))['rvm']

task :default => :release
task :release => [
  :verify_changelog,
  #:rubocop,
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
  t.rspec_opts = '-b'
end

namespace :bundle do
  desc 'Run bundler for all environments'
  task :all do
    abort unless all_rubies('bundle')
    abort unless all_rubies('rake', 'appraisal:install')
  end

  desc 'Update to latest dependencies on all environments'
  task :update_all do
    gemfiles = File.expand_path("../gemfiles", __FILE__)
    FileUtils.rm_r(gemfiles, :verbose => true) if File.exist?(gemfiles)
    abort unless system('bundle', 'update')
    abort unless all_rubies('bundle')
    abort unless all_rubies('rake', 'appraisal:install')
  end
end

desc 'Check style with Rubocop'
Rubocop::RakeTask.new(:rubocop) do |task|
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
end

namespace :test do
  task :all do
    abort unless all_rubies('rake', 'appraisal', 'test:concise')
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

def all_rubies(*command)
  !RUBY_VERSIONS.find do |version|
    !system('rvm', version, 'do', *command)
  end
end
