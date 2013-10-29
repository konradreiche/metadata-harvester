require 'spec_helper'
require 'archiver'

module MetadataHarvester

  describe Archiver do

    after(:all) do
      FileUtils.remove_dir("../archives", true)
    end

    describe "#initialize" do

      let(:date) { Date.new(2013, 10, 29) }
      let(:destination) { "../archives/example.com/#{date}-example.com.jl.gz" }

      it "deletes an existing destination file" do
        FileUtils.mkdir_p("../archives/example.com")
        FileUtils.touch(destination)

        Archiver.new("../archives", "example.com", "CKAN", date, 1)
        expect(File.exists?(destination)).to be_false
      end

      it "creates the directory structure" do
        Archiver.new("../archives", "example.com", "CKAN", date, 1)
        expect(File.directory?("../archives/example.com")).to be_true
      end
    end

  end

end


