require File.expand_path('../spec_helper', __FILE__)

describe Cequel::Model::Translation do
  before do
    I18n.backend.load_translations(
      File.expand_path('../../../support/en.yml', __FILE__))
    I18n.enforce_available_locales = false
  end

  it 'should translate model names' do
    Post.model_name.human.should == 'Blog post'
  end

  it 'should translate attribute names' do
    Post.human_attribute_name(:title).should == 'Post title'
  end

  it 'should translate error messages' do
    post = Post.new(:id => 1, :require_title => true)
    post.valid?
    post.errors.full_messages.should include("Post title is a required field")
  end
end
