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
      post.changed_attributes.should be_empty
    end

    it 'should have changed attributes if attributes change' do
      post.title = 'Cequel ORM'
      post.changed_attributes.
        should == {:title => 'Cequel'}.with_indifferent_access
    end

    it 'should not have changed attributes if attribute set to the same thing' do
      post.title = 'Cequel'
      post.changed_attributes.should be_empty
    end

    it 'should support *_changed? method' do
      post.title = 'Cequel ORM'
      post.title_changed?.should be_true
    end

    it 'should not have changed attributes after save' do
      post.title = 'Cequel ORM'
      post.save
      post.changed_attributes.should be_empty
    end

    it 'should have previous changes after save' do
      post.title = 'Cequel ORM'
      post.save
      post.previous_changes.
        should == { :title => ['Cequel', 'Cequel ORM'] }.with_indifferent_access
    end

    it 'should detect changes to collections' do
      post.categories << 'Gems'
      post.changes.should ==
        {categories: [Set['Libraries'], Set['Libraries', 'Gems']]}.
        with_indifferent_access
    end
  end

  context 'unloaded model' do
    let(:post) { Post['cequel'] }

    it 'should not track changes' do
      post.title = 'Cequel'
      post.changes.should be_empty
    end
  end
end
