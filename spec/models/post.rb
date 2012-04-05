class Post

  include Cequel::Model

  key :id, :integer
  column :title, :string

end
