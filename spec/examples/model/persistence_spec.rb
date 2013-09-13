require File.expand_path('../spec_helper', __FILE__)

describe Cequel::Model::Persistence do
  model :Blog do
    key :subdomain, :text
    column :name, :text
    column :description, :text
    column :owner_id, :uuid
  end

  model :Post do
    key :blog_subdomain, :text
    key :permalink, :text
    column :title, :text
    column :body, :text
    column :author_id, :uuid
  end

  context 'simple keys' do
    subject { cequel[:blogs].where(:subdomain => 'cequel').first }

    let!(:blog) do
      Blog.new do |blog|
        blog.subdomain = 'cequel'
        blog.name = 'Cequel'
        blog.description = 'A Ruby ORM for Cassandra 1.2'
      end.tap(&:save)
    end

    describe '#save' do
      context 'on create' do
        it 'should save row to database' do
          subject[:name].should == 'Cequel'
        end

        it 'should mark row persisted' do
          blog.should be_persisted
        end
      end

      context 'on update' do
        uuid :owner_id

        before do
          blog.name = 'Cequel 1.0'
          blog.owner_id = owner_id
          blog.description = nil
          blog.save
        end

        it 'should change existing column value' do
          subject[:name].should == 'Cequel 1.0'
        end

        it 'should add new column value' do
          subject[:owner_id].should == owner_id
        end

        it 'should remove old column values' do
          subject[:description].should be_nil
        end
      end
    end

    describe '::create' do
      uuid :owner_id

      describe 'with block' do
        let! :blog do
          Blog.create do |blog|
            blog.subdomain = 'big-data'
            blog.name = 'Big Data'
          end
        end

        it 'should initialize with block' do
          blog.name.should == 'Big Data'
        end

        it 'should save instance' do
          Blog.find(blog.subdomain).name.should == 'Big Data'
        end
      end

      describe 'with attributes' do
        let!(:blog) do
          Blog.create(:subdomain => 'big-data', :name => 'Big Data')
        end

        it 'should initialize with block' do
          blog.name.should == 'Big Data'
        end

        it 'should save instance' do
          Blog.find(blog.subdomain).name.should == 'Big Data'
        end
      end
    end

    describe '#update_attributes' do
      let! :blog do
        Blog.create(:subdomain => 'big-data', :name => 'Big Data')
      end

      before { blog.update_attributes(:name => 'The Big Data Blog') }

      it 'should update instance in memory' do
        blog.name.should == 'The Big Data Blog'
      end

      it 'should save instance' do
        Blog.find(blog.subdomain).name.should == 'The Big Data Blog'
      end
    end

    describe '#destroy' do
      before { blog.destroy }

      it 'should delete entire row' do
        subject.should be_nil
      end

      it 'should mark record transient' do
        blog.should be_transient
      end
    end
  end

  context 'compound keys' do
    subject do
      cequel[:posts].
        where(:blog_subdomain => 'cassandra', :permalink => 'cequel').first
    end

    let!(:post) do
      Post.new do |post|
        post.blog_subdomain = 'cassandra'
        post.permalink = 'cequel'
        post.title = 'Cequel'
        post.body = 'A Ruby ORM for Cassandra 1.2'
      end.tap(&:save)
    end

    describe '#save' do
      context 'on create' do
        it 'should save row to database' do
          subject[:title].should == 'Cequel'
        end

        it 'should mark row persisted' do
          post.should be_persisted
        end
      end

      context 'on update' do
        uuid :author_id

        before do
          post.title = 'Cequel 1.0'
          post.author_id = author_id
          post.body = nil
          post.save
        end

        it 'should change existing column value' do
          subject[:title].should == 'Cequel 1.0'
        end

        it 'should add new column value' do
          subject[:author_id].should == author_id
        end

        it 'should remove old column values' do
          subject[:body].should be_nil
        end
      end
    end

    describe '#destroy' do
      before { post.destroy }

      it 'should delete entire row' do
        subject.should be_nil
      end

      it 'should mark record transient' do
        post.should be_transient
      end
    end
  end
end
