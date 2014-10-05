# -*- encoding : utf-8 -*-
require File.expand_path('../spec_helper', __FILE__)

describe Cequel::Record::Dirty do
  model :Post do
    key :permalink, :text
    column :title, :text
    set :categories, :text
  end

  context 'loaded model' do
    let(:post) do
      Post.create!(
        permalink: 'cequel',
        title: 'Cequel',
        categories: Set['Libraries']
      )
    end

    it 'should not have changed attributes by default' do
      expect(post.changed_attributes).to be_empty
    end

    it 'should have changed attributes if attributes change' do
      post.title = 'Cequel ORM'
      expect(post.changed_attributes).
        to eq({:title => 'Cequel'}.with_indifferent_access)
    end

    it 'should not have changed attributes if attribute set to the same thing' do
      post.title = 'Cequel'
      expect(post.changed_attributes).to be_empty
    end

    it 'should support *_changed? method' do
      post.title = 'Cequel ORM'
      expect(post.title_changed?).to eq(true)
    end

    it 'should not have changed attributes after save' do
      post.title = 'Cequel ORM'
      post.save
      expect(post.changed_attributes).to be_empty
    end

    it 'should have previous changes after save' do
      post.title = 'Cequel ORM'
      post.save
      expect(post.previous_changes).
        to eq({ :title => ['Cequel', 'Cequel ORM'] }.with_indifferent_access)
    end

    it 'should detect changes to collections' do
      post.categories << 'Gems'
      expect(post.changes).to eq(
        {categories: [Set['Libraries'], Set['Libraries', 'Gems']]}.
        with_indifferent_access
      )
    end
  end

  context 'unloaded model' do
    let(:post) { Post['cequel'] }

    it 'should not track changes' do
      post.title = 'Cequel'
      expect(post.changes).to be_empty
    end
  end
end
