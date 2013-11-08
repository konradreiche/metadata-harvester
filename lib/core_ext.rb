require 'andand'
require 'json'

class Object
  alias_method :maybe, :andand
end

module CoreExt
  module ClassMethods

    ##
    # Applies JSON.parse on every element.
    # 
    # Returns null on an invalid JSON string.
    #
    def parse_recursively(source)
      recursion = lambda do |source|
        if source.is_a?(Hash)
          source.each { |key, value| source[key] = recursion.call(value) }
        elsif source.is_a?(Array)
          source.map { |item| recursion.call(item) }
        elsif source.is_a?(String) && valid?(source)
          recursion.call(JSON.parse(source))
        else
          source
        end
      end

      return source unless valid?(source)
      source = JSON.parse(source)
      recursion.call(source)
    end

    def valid?(source)
      JSON.parse(source)
      true
    rescue JSON::ParserError
      false
    end

  end

  extend ClassMethods

  def self.included(other)
    other.extend(ClassMethods)
  end    

end

JSON.send(:include, CoreExt)
