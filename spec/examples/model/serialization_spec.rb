require_relative 'spec_helper'

describe 'serialization' do
  model :Post do
    key :blog_subdomain, :text
    key :id, :uuid
    column :title, :text
    column :body, :text
  end

  uuid :id

  let(:attributes) do
    {
      blog_subdomain: 'big-data',
      id: id,
      title: 'Cequel',
    }
  end

  it 'should provide JSON serialization' do
    Post.new(attributes).as_json.symbolize_keys.
      should == attributes.merge(body: nil)
  end
end
