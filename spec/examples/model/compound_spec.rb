require File.expand_path('../spec_helper', __FILE__)

describe Cequel::Model::Persistence do
  model :Post do
    key :blog_subdomain, :text
    key :permalink, :text
    column :title, :text
    column :body, :text
    column :author_id, :uuid
    list :tags, :text
    set :categories, :text
    map :shares, :text, :int
  end

  describe 'reading' do
    before do
      cequel[:posts].insert(
        :blog_subdomain => 'cassandra',
        :permalink => 'cequel',
        :title => 'Cequel',
        :tags => %w(big-data cql),
        :categories => Set['Big Data', 'CQL'],
        :shares => {'facebook' => 1}
      )
    end

    describe '::find' do
      subject { Post.find(:blog_subdomain => 'cassandra', :permalink => 'cequel') }

      its(:blog_subdomain) { should == 'cassandra' }
      its(:permalink) { should == 'cequel' }
      its(:title) { should == 'Cequel' }
      its(:tags) { should == %w(big-data cql) }
      its(:categories) { should == Set['Big Data', 'CQL'] }
      its(:shares) { should == {'facebook' => 1} }

      it { should be_persisted }
      it { should_not be_transient }
      specify { Post.new.should_not be_persisted }
      specify { Post.new.should be_transient }

      specify do
        expect { Post.find(:blog_subdomain => 'bogus')}.
          to raise_error(Cequel::Model::RecordNotFound)
        expect { Post.find(:blog_subdomain => 'bogus', :permalink => 'cequel') }.
          to raise_error(Cequel::Model::RecordNotFound)
      end
    end

    describe '::[]' do
      subject { Post[:blog_subdomain => 'cassandra', :permalink => 'cequel'] }

      it 'should not query the database' do
        expect(cequel).not_to receive(:execute)
        subject.blog_subdomain.should == 'cassandra'
        subject.permalink.should == 'cequel'
      end

      it 'should lazily query the database when attribute accessed' do
        subject.title.should == 'Cequel'
      end

      it 'should get all eager-loadable attributes on first lazy load' do
        subject.title
        expect(cequel).not_to receive(:execute)
        subject.tags.should == %w(big-data cql)
      end
    end
    
    describe '::new' do
      subject { Post.new(:blog_subdomain => 'cassandra', :permalink => 'cequel') }

      it 'should not query the database' do
        expect(cequel).not_to receive(:execute)
        subject.blog_subdomain.should == 'cassandra'
        subject.permalink.should == 'cequel'
      end

      it 'should not query the database when attribute accessed' do
        expect(cequel).not_to receive(:execute)
        subject.title.should == nil
      end
    end
  end

  describe 'writing' do
    subject { cequel[:posts].where(:blog_subdomain => 'cassandra', :permalink => 'cequel').first }

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
