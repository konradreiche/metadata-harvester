require 'spec_helper'
require 'archiver'
require 'oj'

module MetadataHarvester

  describe Archiver do

    after(:all) do
      FileUtils.remove_dir("spec/archives", true)
    end

    let(:date) { Date.new(2013, 10, 29) }
    let(:destination) { "spec/archives/example.com/#{date}-example.com.jl.gz" }

    describe "#initialize" do
      it "deletes an existing destination file" do
        FileUtils.mkdir_p("spec/archives/example.com")
        FileUtils.touch(destination)

        Archiver.new("spec/archives", "example.com", "CKAN", date, 1)
        expect(File.exists?(destination)).to be_false
      end

      it "creates the directory structure" do
        Archiver.new("spec/archives", "example.com", "CKAN", date, 1)
        expect(File.directory?("spec/archives/example.com")).to be_true
      end
    end

    describe "#store" do

      it "writes the header to the beginning of the file" do
        header = {"repository" => "example.com",
                  "type" => "CKAN",
                  "date" => "2013-10-29",
                  "count" => 1}

        archiver = Archiver.new("spec/archives", "example.com", "CKAN", date, 1)
        archiver.store { }

        content = Zlib::GzipReader.open(destination) { |gz| gz.readlines }
        json = Oj.load(content.first)
        expect(json).to eq(header)
      end

      it "yields a stream writer" do
        archiver = Archiver.new("spec/archives", "example.com", "CKAN", date, 1)
        hash = { id: "d8e8fca2dc0f896fd7cb4cb0031ba249" }

        archiver.store do |writer|
          Oj.to_stream(writer, hash)
        end

        content = Zlib::GzipReader.open(destination) { |gz| gz.readlines }
        expect(content.length).to be(2)

        json = Oj.load(content[1])
        expect(json).to eq(hash)
      end

    end
  end

end


