# -*- encoding : utf-8 -*-
require_relative 'spec_helper'

describe Cequel::Record::Scoped do
  model :Post do
    key :blog_subdomain, :text
    key :id, :uuid, auto: true
    column :name, :text
  end

  it 'should use current scoped key values to populate new record' do
    expect(Post['bigdata'].new.blog_subdomain).to eq('bigdata')
  end

  it "should not mess up class' #puts" do
    StringIO.new.tap do |out|
      out.puts Post
      expect(out.string).to eq("Post\n")
    end

  end
end
