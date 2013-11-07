require 'spec_helper'
require 'worker'
require 'webmock/rspec'

module MetadataHarvester

  describe Worker do

    subject { Worker.new }

    describe "#count" do
      it "counts the number of packages" do
        url = 'http://www.example.com/api/action/package_search'
        body = '{"result":{"count":1000}}'
        stub_request(:post, url).with(body: '{}')
        .to_return(status: 200, body: body, headers: {})
       
        response = subject.count('http://www.example.com/api')
        expect(response).to be(1000)
      end

      it "counts the number of packages of a legacy repository" do
        url = 'http://www.example.com/api/rest/dataset'
        body = '["a", "b", "c"]'
        stub_request(:get, url).to_return(status: 200, body: body, headers: {})

        response = subject.count('http://www.example.com/api', true)
        expect(response).to be(3)
      end
    end

  end

end
