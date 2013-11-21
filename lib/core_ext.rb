require 'andand'
require 'json'
require 'oj'

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
    #
    def parse_recursively(input)
      if not input.is_a?(String) or not valid?(input)
        raise TypeError, "Expected a JSON String"
      end

      recursion = lambda do |source|
        if source.is_a?(Hash)
          source.each { |key, value| source[key] = recursion.call(value) }
        elsif source.is_a?(Array)
          source.map { |item| recursion.call(item) }
        elsif source.is_a?(String) and valid?(source)
          recursion.call(Oj.load(source))
        else
          source
        end
      end

      recursion.call(input)
    end

    def valid?(source)
      value = Oj.load(source)
      value.is_a?(Array) or value.is_a?(Hash)
    rescue Oj::ParseError
      false
    end

  end

  extend ClassMethods

  def self.included(other)
    other.extend(ClassMethods)
  end    

end

JSON.send(:include, CoreExt)
