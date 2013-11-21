require 'json'
require 'oj'

# This class encapsulates KAN specific functionality.
#
# @author Konrad Reiche
#
class CKAN

  def self.normalize_extras(input)
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

  def self.valid?(source)
    value = Oj.load(source)
    value.is_a?(Array) or value.is_a?(Hash)
  rescue Oj::ParseError
    false
  end

end
