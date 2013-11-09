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
    def parse_recursively(input)
      recursion = lambda do |source|
        if source.is_a?(Hash)
          source.each { |key, value| source[key] = recursion.call(value) }
        elsif source.is_a?(Array)
          source.map { |item| recursion.call(item) }
        elsif source.is_a?(String) && valid?(source)
          recursion.call(JSON.load(source))
        else
          source
        end
      end

      return input unless valid?(input)
      input = JSON.load(input)
      recursion.call(input)
    end

    def valid?(source)
      JSON.load(source)
      true
    rescue JSON::ParserError, NoMethodError
      false
    end

  end

  extend ClassMethods

  def self.included(other)
    other.extend(ClassMethods)
  end    

end

JSON.send(:include, CoreExt)
