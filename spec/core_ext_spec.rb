require 'spec_helper'
require 'core_ext'

describe CoreExt do
  describe "#parse_recursively" do

    it "is available on the JSON module" do
      expect(JSON.respond_to?(:parse_recursively)).to be_true
    end

    it "parses a simple JSON object" do
      source = '{"title": "Psychology of Crowds", "year": 2009}'
      result = { "title" => "Psychology of Crowds", "year" => 2009 }
      expect(JSON.parse_recursively(source)).to eq(result)
    end

    it "parses a JSON object containing a string encoded object" do
      source = '{"values": "\{\"a\": 1, \"b\": 2, \"c\": 3\}"}'
      result = { "values" => { "a" => 1, "b" => 2, "c" => 3 } }
      expect(JSON.parse_recursively(source)).to eq(result)
    end

    it "parses a JSON object containing a string encoded array" do
      source = '{"values": "\[1, 2, 3\]"}'
      result = { "values" => [1, 2, 3] }
      expect(JSON.parse_recursively(source)).to eq(result)
    end

    it "parses a JSON object with escaped brackets" do
      source = "\{\"a\": 1, \"b\": 2, \"c\": 3\}"
      result = { "a" => 1, "b" => 2, "c" => 3 }
      expect(JSON.parse_recursively(source)).to eq(result)
    end

    it "does not parse an array" do
      source = [1, 2, 3]
      expect { JSON.parse_recursively(source) }.to raise_error(TypeError)
    end
    
    it "does not parse a number" do
      source = 1
      expect { JSON.parse_recursively(source) }.to raise_error(TypeError)
    end

    it "does not parse a simple a unstructred string" do
      source = "text"
      expect { JSON.parse_recursively(source) }.to raise_error(JSON::ParserError)
    end

    it "parses multiple nested string encoded JSON objects" do
      source = '{"a": "\{\"b\": \"\{\}\"\}"}'
      result = { "a" => { "b" => {} } }
      expect(JSON.parse_recursively(source)).to eq(result)
    end

  end
end
