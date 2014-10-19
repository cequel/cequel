# -*- encoding : utf-8 -*-
require File.expand_path('../../environment', __FILE__)
require 'cequel'
require 'tzinfo'

Dir.glob(File.expand_path('../../support/**/*.rb', __FILE__)).each do |file|
  require file
end
Dir.glob(File.expand_path('../../shared/**/*.rb', __FILE__)).each do |file|
  require file
end

RSpec.configure do |config|
  config.include(Cequel::SpecSupport::Helpers)
  config.extend(Cequel::SpecSupport::Macros)

  config.filter_run_excluding rails: ->(requirement) {
    !Gem::Requirement.new(requirement).
      satisfied_by?(Gem::Version.new(ActiveSupport::VERSION::STRING))
  }

  unless defined? CassandraCQL
    config.filter_run_excluding thrift: true
  end

  config.before(:all) do
    cequel.schema.create!
    Cequel::Record.connection = cequel
    Time.zone = 'UTC'
    I18n.enforce_available_locales = false
    SafeYAML::OPTIONS[:default_mode] = :safe if defined? SafeYAML
  end

  config.after(:all) do
    cequel.schema.drop!
  end

  config.after(:each) { Timecop.return }

  config.filter_run :focus => true
  config.run_all_when_everything_filtered = true
end

if defined? byebug
  Kernel.module_eval { alias_method :debugger, :byebug }
end
