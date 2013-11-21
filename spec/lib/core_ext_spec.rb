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
      source = '{"values": "{\"a\": 1, \"b\": 2, \"c\": 3}"}'
      result = { "values" => { "a" => 1, "b" => 2, "c" => 3 } }
      expect(JSON.parse_recursively(source)).to eq(result)
    end

    it "parses a JSON object containing a string encoded array" do
      source = '{"values": "[1, 2, 3]"}'
      result = { "values" => [1, 2, 3] }
      expect(JSON.parse_recursively(source)).to eq(result)
    end

    it "parses a JSON object with escaped brackets" do
      source = "\{\"a\": 1, \"b\": 2, \"c\": 3\}"
      result = { "a" => 1, "b" => 2, "c" => 3 }
      expect(JSON.parse_recursively(source)).to eq(result)
    end

    it "only parses Strings" do
      source = [1, 2, 3]
      expect { JSON.parse_recursively(source) }.to raise_error(TypeError)

      source = 1
      expect { JSON.parse_recursively(source) }.to raise_error(TypeError)

      source = true
      expect { JSON.parse_recursively(source) }.to raise_error(TypeError)
    end
    
    it "only parses valid JSON" do
      source = "text"
      expect { JSON.parse_recursively(source) }.to raise_error(TypeError)
    end

    it "parses multiple nested string encoded JSON objects" do
      c = JSON.dump({ "c" => JSON.dump({}) })
      b = JSON.dump({ "b" => c })
      a = JSON.dump({ "a" => b })

      result = { "a" => { "b" => { "c" => {} } } }
      expect(JSON.parse_recursively(a)).to eq(result)
    end

    it "parses multiple nested string encoded JSON arrays" do
      b = JSON.dump([1, 2, 3])
      a = JSON.dump([b, b, b])

      result = [[1, 2, 3], [1, 2, 3], [1, 2, 3]]
      expect(JSON.parse_recursively(a)).to eq(result)
    end

    it "does not normalize Integers" do
      source = JSON.dump({ "a" => '1' })
      expectation = { "a" => '1' }
      expect(JSON.parse_recursively(source)).to eq(expectation)
    end

    it "does not normalize floating point" do
      source = JSON.dump({ "a" => '3.5' })
      expectation = { "a" => '3.5' }
      expect(JSON.parse_recursively(source)).to eq(expectation)
    end

    it "does not normalize Boolean" do
      source = JSON.dump({ "a" => 'true' })
      expectation = { "a" => 'true' }
      expect(JSON.parse_recursively(source)).to eq(expectation)
    end

  end
end
