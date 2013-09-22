require_relative 'spec_helper'

describe Cequel::Record::Scoped do
  model :Post do
    key :blog_subdomain, :text
    key :id, :uuid, auto: true
    column :name, :text
  end

  it 'should use current scoped key values to populate new record' do
    Post['bigdata'].new.blog_subdomain.should == 'bigdata'
  end
end
