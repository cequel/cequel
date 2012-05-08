require File.expand_path('../lib/cequel/version', __FILE__)
require 'rspec/core/rake_task'

task :default => :release
task :release => [:test, :build, :tag, :update_stable, :push, :cleanup]

desc 'Build gem'
task :build do
  system 'gem build cequel.gemspec'
end

desc 'Create git release tag'
task :tag do
  system "git tag -a -m 'Version #{Cequel::VERSION}' #{Cequel::VERSION}"
  system "git push origin #{Cequel::VERSION}:#{Cequel::VERSION}"
end

desc 'Update stable branch on GitHub'
task :update_stable do
  if Cequel::VERSION =~ /^(\d+\.)+\d+$/ # Don't push for prerelease
    system "git push -f origin HEAD:stable"
  end
end

desc 'Push gem to repository'
task :push => :inabox

task 'Push gem to geminabox'
task :inabox do
  system "gem inabox cequel-#{Cequel::VERSION}.gem"
end

task 'Remove packaged gems'
task :cleanup do
  system "rm -v *.gem"
end

desc 'Run the specs'
RSpec::Core::RakeTask.new(:test)
