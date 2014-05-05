require File.expand_path('../spec_helper', __FILE__)

describe Cequel::Record::Timestamps do

  model :Post do
    key :blog_subdomain, :text
    key :id, :uuid, auto: true
    column :name, :text
    timestamps
  end

  it 'should populate created_at after create new record' do
    p = Post['bigdata'].new
    p.save!
    p.created_at.should_not nil
  end

  it 'should populate updated_at after create new record' do
    p = Post['bigdata'].new
    p.save!
    p.updated_at.should_not nil
  end

  it 'should update updated_at after record update but not created_at' do
    p = Post['bigdata'].new
    p.save!
    sleep(1)
    p.name = 'name'
    p.save!
    expect(p.updated_at).not_to eq(p.created_at)
  end

end