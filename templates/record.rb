class <%= class_name %>
  include Cequel::Record

  key :id, :uuid, auto: true
  <%- attributes.each do |attribute| -%>
  column <%= attribute.name.to_sym.inspect %>, <%= attribute.type.to_sym.inspect %>
  <%- end -%>
end
