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

  let(:post){ Post.new(attributes) }

  before :each do
    Post.include_root_in_json = false
  end

  it 'should provide JSON serialization' do
    json = post.as_json.deep_symbolize_keys
    compare_attributes = attributes.merge(body: nil, id: {n: post.attributes["id"].to_i, s: post.attributes["id"].to_s})
    expect(json).to eq(compare_attributes)
  end

  it 'should be able to serialize restricting to some attributes' do
    json = post.as_json(only: [:id]).deep_symbolize_keys
    expect(json).to eq(id: {n: post.attributes[:id].to_i, s: post.attributes[:id].to_s})
  end
end
