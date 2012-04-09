require File.expand_path('../spec_helper', __FILE__)

describe 'Serialization' do
  let(:post) { Post.new(1, :title => 'Cequel') }

  it 'should serialize to JSON' do
    post.to_json.should ==
      {'post' => {'id' => 1, 'title' => 'Cequel'}}.to_json
  end

  it 'should serialize to XML' do
    post.to_xml.should == <<XML
<?xml version="1.0" encoding="UTF-8"?>
<post>
  <id type="integer">1</id>
  <title>Cequel</title>
</post>
XML
  end
end
