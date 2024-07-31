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
    json = post.to_json
    expected = attributes.merge(body: nil).to_json

    expect(JSON.parse(json)).to eq(JSON.parse(expected))
  end

  it 'should be able to serialize restricting to some attributes' do
    json = post.to_json(only: [:id])
    expected = { id: attributes[:id] }.to_json

    expect(JSON.parse(json)).to eq(JSON.parse(expected))
  end
end
