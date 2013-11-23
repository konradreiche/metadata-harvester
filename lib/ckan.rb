require 'json'
require 'oj'

# This class encapsulates KAN specific functionality.
#
# @author Konrad Reiche
#
class CKAN

  def self.normalize_extras(source)
    if source.is_a?(Hash)
      source.each { |key, value| source[key] = normalize_extras(value) }
    elsif source.is_a?(Array)
      source.map { |item| normalize_extras(item) }
    elsif source.is_a?(String) and valid?(source)
      normalize_extras(@value)
    else
      source
    end
  end

  # Validates the source and stores the result for later use.
  #
  def self.valid?(source)
    @value = Oj.load(source)
    @value.is_a?(Array) or @value.is_a?(Hash) or @value.is_a?(String)
  rescue Oj::ParseError
    false
  end

end
