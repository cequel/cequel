require File.expand_path('../../spec_helper', __FILE__)
require 'cequel/model'

Dir.glob(File.join(File.dirname(__FILE__), '../../models/**/*.rb')).each do |file|
  require file
end

RSpec.configure do |config|
  config.before :each do
    Cequel::Model.keyspace = Cequel::Keyspace.new(connection)
  end
end
