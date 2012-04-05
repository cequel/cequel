class Post

  include Cequel::Model

  key :id, :integer
  column :title, :string

  def self.for_blog(blog_id)
    where(:blog_id => blog_id)
  end

end
