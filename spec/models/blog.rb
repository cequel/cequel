class Blog

  include Cequel::Model

  key :id, :integer
  column :name, :varchar

  has_many :posts

end
