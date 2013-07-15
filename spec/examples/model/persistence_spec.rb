require File.expand_path('../spec_helper', __FILE__)

describe Cequel::Model::Persistence do
  model :Post do
    key :permalink, :text
    column :title, :text
    list :tags, :text
    set :categories, :text
    map :shares, :text, :int
  end

  describe 'reading' do
    before do
      cequel[:posts].insert(
        :permalink => 'cequel',
        :title => 'Cequel',
        :tags => %w(big-data cql),
        :categories => Set['Big Data', 'CQL'],
        :shares => {'facebook' => 1}
      )
    end

    describe '::find' do
      subject { Post.find('cequel') }

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
        expect { Post.find('bogus') }.
          to raise_error(Cequel::Model::RecordNotFound)
      end
    end

    describe '::[]' do
      subject { Post['cequel'] }

      it 'should not query the database' do
        expect(cequel).not_to receive(:execute)
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
  end
end
