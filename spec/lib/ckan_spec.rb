require 'spec_helper'
require 'ckan'

describe CKAN do
  describe "::normalize_extras" do

    it "parses a simple JSON object" do
      source = '{"title": "Psychology of Crowds", "year": 2009}'
      result = { "title" => "Psychology of Crowds", "year" => 2009 }
      expect(CKAN.normalize_extras(source)).to eq(result)
    end

    it "parses a JSON object containing a string encoded object" do
      source = '{"values": "{\"a\": 1, \"b\": 2, \"c\": 3}"}'
      result = { "values" => { "a" => 1, "b" => 2, "c" => 3 } }
      expect(CKAN.normalize_extras(source)).to eq(result)
    end

    it "parses a JSON object containing a string encoded array" do
      source = '{"values": "[1, 2, 3]"}'
      result = { "values" => [1, 2, 3] }
      expect(CKAN.normalize_extras(source)).to eq(result)
    end

    it "parses a JSON object with escaped brackets" do
      source = "\{\"a\": 1, \"b\": 2, \"c\": 3\}"
      result = { "a" => 1, "b" => 2, "c" => 3 }
      expect(CKAN.normalize_extras(source)).to eq(result)
    end
    
    it "return an invalid JSON string as it is" do
      source = "text"
      expect(CKAN.normalize_extras(source)).to eq("text")
    end

    it "parses multiple nested string encoded JSON objects" do
      c = JSON.dump({ "c" => JSON.dump({}) })
      b = JSON.dump({ "b" => c })
      a = JSON.dump({ "a" => b })

      result = { "a" => { "b" => { "c" => {} } } }
      expect(CKAN.normalize_extras(a)).to eq(result)
    end

    it "parses multiple nested string encoded JSON arrays" do
      b = JSON.dump([1, 2, 3])
      a = JSON.dump([b, b, b])

      result = [[1, 2, 3], [1, 2, 3], [1, 2, 3]]
      expect(CKAN.normalize_extras(a)).to eq(result)
    end

    it "does not normalize Integers" do
      source = JSON.dump({ "a" => '1' })
      expectation = { "a" => '1' }
      expect(CKAN.normalize_extras(source)).to eq(expectation)
    end

    it "does not normalize floating point" do
      source = JSON.dump({ "a" => '3.5' })
      expectation = { "a" => '3.5' }
      expect(CKAN.normalize_extras(source)).to eq(expectation)
    end

    it "does not normalize Boolean" do
      source = JSON.dump({ "a" => 'true' })
      expectation = { "a" => 'true' }
      expect(CKAN.normalize_extras(source)).to eq(expectation)
    end

  end
end
