require File.expand_path('../spec_helper', __FILE__)

describe Cequel::Record::Callbacks do
  model :Post do
    key :permalink, :text
    column :title, :text

    def self.track_callbacks(*events)
      events.each do |event|
        %w(before after).each do |position|
          callback_name = :"#{position}_#{event}"
          __send__(callback_name) do |post|
            post.executed_callbacks << callback_name
          end
        end
      end
    end

    track_callbacks :save, :create, :update, :destroy

    def executed_callbacks
      @executed_callbacks ||= []
    end

  end

  model :Comment do
    belongs_to :post
    key :id, :timeuuid, :auto => true
    column :body, :text

    before_save :create_post
    after_save :run_instance_after_save

    attr_writer :instance_after_save

    private

    def create_post
      post = Post.create!(permalink: 'autopost', title: 'Auto Post')
      self.post = post
    end

    def run_instance_after_save
      @instance_after_save.call
    end
  end

  let(:new_post) do
    Post.new do |post|
      post.permalink = 'new-post'
      post.title = 'New Post'
    end
  end

  let!(:existing_post) do
    Post.new do |post|
      post.permalink = 'existing-post'
      post.title = 'Existing Post'
    end.save!
    Post.find('existing-post').tap do |post|
      post.title = 'An Existing Post'
    end
  end

  context 'on create' do
    before { new_post.save! }
    subject { new_post.executed_callbacks }

    it { should include(:before_save) }
    it { should include(:after_save) }
    it { should include(:before_create) }
    it { should include(:after_create) }
    it { should_not include(:before_update) }
    it { should_not include(:after_update) }
    it { should_not include(:before_destroy) }
    it { should_not include(:after_destroy) }
  end

  context 'on update' do
    before { existing_post.save! }
    subject { existing_post.executed_callbacks }

    it { should include(:before_save) }
    it { should include(:after_save) }
    it { should_not include(:before_create) }
    it { should_not include(:after_create) }
    it { should include(:before_update) }
    it { should include(:after_update) }
    it { should_not include(:before_destroy) }
    it { should_not include(:after_destroy) }
  end

  context 'on destroy' do
    before { existing_post.destroy }

    subject { existing_post.executed_callbacks }

    it { should_not include(:before_save) }
    it { should_not include(:after_save) }
    it { should_not include(:before_create) }
    it { should_not include(:after_create) }
    it { should_not include(:before_update) }
    it { should_not include(:after_update) }
    it { should include(:before_destroy) }
    it { should include(:after_destroy) }
  end

  describe 'atomic writes' do
    it 'should run callbacks in a logged batch' do
      comment = Comment.new(:body => 'Great web site!')
      comment.instance_after_save =
        -> { expect { Post.find('autopost') }.
          to raise_error(Cequel::Record::RecordNotFound) }
      comment.save!
      Post.find('autopost').title.should == 'Auto Post'
    end
  end
end
