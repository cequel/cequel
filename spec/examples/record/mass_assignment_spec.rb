require_relative 'spec_helper'

describe Cequel::Record::MassAssignment do
  context 'with strong parameters', :rails => '~> 4.0' do
    model :Post do
      key :permalink, :text
      column :title, :text
    end

    it 'should allow assignment of vanilla hash' do
      Post.new(:title => 'Cequel').title.should == 'Cequel'
    end

    it 'should allow assignment of permitted strong params' do
      Post.new(StrongParams.new(true, :title => 'Cequel')).title.
        should == 'Cequel'
    end

    it 'should raise exception when assigned non-permitted strong params' do
      expect { Post.new(StrongParams.new(false, :title => 'Cequel')) }.
        to raise_error(ActiveModel::ForbiddenAttributesError)
    end

    class StrongParams < DelegateClass(Hash)
      def initialize(permitted, params)
        super(params)
        @permitted = !!permitted
      end

      def permitted?
        @permitted
      end
    end
  end

  context 'with mass-assignment protection', :rails => '~> 3.1' do
    model :Post do
      key :permalink, :text
      column :title, :text
      column :page_views, :int

      attr_accessible :title
    end

    let(:post) { Post.new(:title => 'Cequel', :page_views => 1000) }

    it 'should allow assignment of accessible params' do
      post.title.should == 'Cequel'
    end

    it 'should not allow assignment of inaccessible params' do
      post.page_views.should be_nil
    end
  end
end
