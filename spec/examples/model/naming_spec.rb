require_relative 'spec_helper'

describe 'naming' do
  model :Blog do
    key :subdomain, :text
    column :name, :text
  end

  it 'should implement model_name' do
    Blog.model_name.should == 'Blog'
  end

  it 'should implement model_name interpolations' do
    Blog.model_name.i18n_key.should == :blog
  end
end
