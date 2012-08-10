class Comment
  include Cequel::Model

  key :id, :uuid
  column :body, :text

  private

  def generate_key
    SimpleUUID::UUID.new
  end
end
