# -*- encoding : utf-8 -*-
require_relative 'spec_helper'

describe 'naming' do
  model :Blog do
    key :subdomain, :text
    column :name, :text
  end

  it 'should implement model_name' do
    expect(Blog.model_name).to eq('Blog')
  end

  it 'should implement model_name interpolations' do
    expect(Blog.model_name.i18n_key).to eq(:blog)
  end
end
