require 'spec_helper'
require 'dump_handler'

module MetadataHarvester

  describe DumpHandler do

    subject { DumpHandler.new }

    let(:threshold) { DumpHandler.const_get(:THRESHOLD) }

    # Helper method to parse the input and return the result
    #
    def parsed(input)
      Oj.sc_parse(subject, Oj.dump(input))
      subject.records
    end

    it "handles records.size == 0" do
      input = []
      expect(parsed(input)).to be_nil
    end

    it "handles records.size == 1 and record.size == 0" do
      input = [{}]
      expect(parsed(input)).to eq(input)
    end

    it "handles records.size > n and record.size == 0" do
      input = [{}, {}, {}]
      expect(parsed(input)).to eq(input)
    end

    it "handles records.size == 1 and record.size == 1" do
      input = [{ "a" => 3 }]
      expect(parsed(input)).to eq(input)
    end

    it "handles records.size == 1 and record.size > n" do
      input = [{ "a" => 3, "b" => "5", "c" => true }]
      expect(parsed(input)).to eq(input)
    end

    it "handles records.size > n and record.size == 1" do
      input = [{ "a" => 3 }] * 3
      expect(parsed(input)).to eq(input)
    end

    it "handles records.size > n and record.size > n" do
      input = [{ "a" => 3, "b" => "5", "c" => true }] * 3
      expect(parsed(input)).to eq(input)
    end

    it "handles records.size == 1 and array.size == 0" do
      input = [{ "a" => [] }] 
      expect(parsed(input)).to eq(input)
    end

    it "handles records.size > n and array.size == 0" do
      input = [{ "a" => [] }]  * 3
      expect(parsed(input)).to eq(input)
    end

    it "handles records.size == 1 and array.size == 1" do
      input = [{ "a" => ["b"] }] 
      expect(parsed(input)).to eq(input)
    end

    it "handles records.size > n and array.size == 1" do
      input = [{ "a" => ["b"] }]  * 3
      expect(parsed(input)).to eq(input)
    end

    it "handles records.size == 1 and array.size > n" do
      input = [{ "a" => ["b", 5, true] }]
      expect(parsed(input)).to eq(input)
    end

    it "handles records.size > n and array.size > n" do
      input = [{ "a" => ["b", 5, true] }] * 3
      expect(parsed(input)).to eq(input)
    end

    it "handles records.size == 1 and record.depth > n" do
      input = [{ "a" => { "b" => { "c" => 3 }}}]
      expect(parsed(input)).to eq(input)
    end

    it "handles records.size > n and record.depth > n" do
      input = [{ "a" => { "b" => { "c" => 3 }}}]
      expect(parsed(input)).to eq(input)
    end

    it "handles alternating hashes and arrays" do
      input = [{ "a" => [{ "b" => { "c" => [{ "d" => [3, 5, 7]}]}}]}]
      expect(parsed(input)).to eq(input)
    end
    
    it "handles a record with different field types" do
      input = [{ "a" => "1",
                 "b" => 2,
                 "c" => [3, 3, 4],
                 "d" => 5,
                 "e" => { "6" => { "f" => true }}}]

      expect(parsed(input)).to eq(input)
    end

    it "invokes the callback once if threshold is reached" do
      input = [{ "a" => 3 }] * 1000

      invocations = 0
      callback = lambda { |x| invocations += 1 }

      handler = DumpHandler.new(callback)
      Oj.sc_parse(handler, Oj.dump(input))
      expect(invocations).to be(1)
    end

  end
  
end

