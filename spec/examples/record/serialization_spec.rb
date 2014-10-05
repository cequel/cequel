# -*- encoding : utf-8 -*-
require_relative 'spec_helper'

describe 'serialization' do
  model :Post do
    key :blog_subdomain, :text
    key :id, :uuid, auto: true
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
    Post.include_root_in_json = false
    expect(Post.new(attributes).as_json.symbolize_keys).
      to eq(attributes.merge(body: nil))
  end
end
