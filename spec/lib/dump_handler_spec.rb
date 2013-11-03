require 'spec_helper'
require 'dump_handler'

module MetadataHarvester

  describe DumpHandler do

    subject { DumpHandler.new }

    # Helper method to parse the input and return the result
    #
    def parsed(input)
      Oj.sc_parse(subject, Oj.dump(input))
      subject.records
    end

    it "handles an empty array" do
      input = []
      expect(parsed(input)).to eq(input)
    end

    it "handles one empty record" do
      input = [{}]
      expect(parsed(input)).to eq(input)
    end

    it "handles multiple empty records" do
      input = [{}, {}, {}]
      expect(parsed(input)).to eq(input)
    end

    it "handles one simple record" do
      input = [{ "id" => "statistics-2013", "reviews" => 3 }]
      expect(parsed(input)).to eq(input)
    end

    it "handles multiple simple records" do
      input = [{ "id" => "statistics-2013", "reviews" => 3 }] * 3
      expect(parsed(input)).to eq(input)
    end

    it "handles one record with one nested field" do
      input = [{ "id"      => "statistics-2013",
                 "reviews" => 3,
                 "author"  => { "name"     => "Marlene Hayes",
                                "birthday" => "1970-11-12" }}]

      expect(parsed(input)).to eq(input)
    end

    it "handles multiple records with one nested field" do
      input = [{ "id"      => "statistics-2013",
                 "reviews" => 3,
                 "author"  => { "name"     => "Marlene Hayes",
                                "birthday" => "1970-11-12" }}] * 3

      expect(parsed(input)).to eq(input)
    end

    it "handles one record with multiple nested fields" do
      input = [{ "id"          => "statistics-2013",
                 "reviews"     => 3,
                 "author"      => { "name"     => "Marlene Hayes",
                                    "birthday" => "1970-11-12" },
                 "description" => "Statistics of the year 2013",
                 "resources"   => { "id"       => "statistics-2013-pdf",
                                    "format"   => "PDF" }}]

      expect(parsed(input)).to eq(input)
    end

    it "handles multiple records with multiple nested fields" do
      input = [{ "id"          => "statistics-2013",
                 "reviews"     => 3,
                 "author"      => { "name"     => "Marlene Hayes",
                                    "birthday" => "1970-11-12" },
                 "description" => "Statistics of the year 2013",
                 "resources"   => { "id"       => "statistics-2013-pdf",
                                    "format"   => "PDF" }}] * 3

      expect(parsed(input)).to eq(input)
    end

    it "handles arbitrary depth" do
      input = [{ "a" => { "b" => { "c" => { "d" => { "e" => 1 }}}}}]
      expect(parsed(input)).to eq(input)
    end

#    it "handles one record with nested fields and arrays" do
#      resources = [{ "id" => 1, "formats" => ["PDF"] }] * 3
#
#      input = [{ "id"        => "statistics-2013",
#                 "tags"      => ["transparency", "statistics"],
#                 "resources" => resources }]
#
#      expect(parsed(input)).to eq(input)
#    end
#
#    it "handles arbitrary depth with mixed hashes and arrays" do
#      input = [{"a" => [{"b" => {"c" => [1, 2, 3]}}] * 3}]
#      expect(parsed(input)).to eq(input)
#    end
#
#    it "partitions the records by the defined threshold" do
#      threshold = DumpHandler.THRESHOLD
#      input = [{ "a" => 1 }] * threshold * 3
#
#      result = parsed(input)
#      expect(result.length).to be 3
#      expect(result).to eq(input.each_slice(3).to_a)
#    end

  end
  
end

