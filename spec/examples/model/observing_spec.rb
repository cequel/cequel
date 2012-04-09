require File.expand_path('../spec_helper', __FILE__)

describe Cequel::Model::Observing do
  before do
    Cequel::Model.observers = [:post_observer]
    Cequel::Model.instantiate_observers
  end

  let(:post) do
    connection.stub(:execute).
      with("SELECT * FROM posts WHERE id = 1 LIMIT 1").
      and_return result_stub('id' => 1, 'title' => 'Hey')
    Post.find(1)
  end

  shared_examples_for 'observing callbacks' do |*callbacks|

    all_callbacks = [
      :before_create, :after_create, :before_update, :after_update,
      :before_save, :after_save, :before_destroy, :after_destroy,
      :before_validation, :after_validation
    ]

    callbacks.each do |callback|
      it "should observe #{callback}" do
        post.should have_been_observed(callback)
      end
    end

    (all_callbacks - callbacks).each do |callback|
      it "should not observe #{callback}" do
        post.should_not have_been_observed(callback)
      end
    end
  end

  context 'on create' do
    let(:post) do
      connection.stub(:execute).
        with "INSERT INTO posts (id, title) VALUES (1, 'Hey')"
      Post.new(1, :title => 'Hey')
    end

    before { post.save }

    it_should_behave_like 'observing callbacks',
      :before_create, :after_create, :before_save, :after_save, :before_validation, :after_validation
  end

  context 'on update' do

    before { post.save }

    it_should_behave_like 'observing callbacks',
      :before_update, :after_update, :before_save, :after_save, :before_validation, :after_validation
  end

  context 'on destroy 'do

    before do
      connection.stub(:execute).
        with "DELETE FROM posts WHERE id = 1"

      post.destroy
    end

    it_should_behave_like 'observing callbacks', :before_destroy, :after_destroy
  end

  context 'on validation' do
    before do
      post.valid?
    end

    it_should_behave_like 'observing callbacks', :before_validation, :after_validation
  end
end
