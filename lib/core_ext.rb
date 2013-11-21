require 'andand'

class Object
  alias_method :maybe, :andand
end
