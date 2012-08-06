class PostComments < Cequel::Model::Dictionary
  key :post_id, :int
  maps :int => :text

  private

  def serialize_value(data)
    data.to_json
  end

  def deserialize_value(json)
    JSON.parse(json)
  end
end
