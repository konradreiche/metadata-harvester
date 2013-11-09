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

    describe "#unify" do
      it "dismantles group objects to group strings" do
        records = [{ 'tags'   => [], 
                     'extras' => [],
                     'groups' => [{ 'name' => 'finance' }, 
                                  { 'name' => 'education' },
                                  { 'name' => 'health' }]}]

        result = subject.unify(records).first
        expect(result['groups']).to eq(['finance', 'education', 'health'])
      end

      it "dismantles tag objects to tag strings" do
        records = [{ 'groups' => [], 
                     'extras' => [],
                     'tags'   => [{ 'name' => 'economy' }, 
                                  { 'name' => 'statistic' },
                                  { 'name' => 'map' }]}]

        result = subject.unify(records).first
        expect(result['tags']).to eq(['economy', 'statistic', 'map'])
      end

      it "dismantles extras objects to an monolithic extras object" do
        extras1 = { 'key' => 'language',  'value' => 'english' }
        extras2 = { 'key' => 'harvested', 'value' => true }

        records = [{ 'groups' => [], 
                     'tags'   => [],
                     'extras' => [extras1, extras2] }]

        result = subject.unify(records).first
        expectation = { 'language' => 'english', 'harvested' => true }
        expect(result['extras']).to eq(expectation)
      end

      it "parses a nested extras object" do
        extras1 = { 'key' => 'languages',  'value' => '["german", "english"]' }
        extras2 = { 'key' => 'id',         'value' => '1' }

        records = [{ 'groups' => [], 
                     'tags'   => [],
                     'extras' => [extras1, extras2] }]

        result = subject.unify(records).first
        expectation = { 'languages' => ['german', 'english'], 'id' => 1 }
        expect(result['extras']).to eq(expectation)
      end

      it "parses string serialized booleans" do
        extras1 = { 'key' => 'free_of_charge', 'value' => 'true' }
        extras2 = { 'key' => 'free_to_use',    'value' => 'True' }

        records = [{ 'groups' => [], 
                     'tags'   => [],
                     'extras' => [extras1, extras2] }]

        result = subject.unify(records).first
        expectation = { 'free_of_charge' => true, 'free_to_use' => true }
        expect(result['extras']).to eq(expectation)

        extras1 = { 'key' => 'free_of_charge', 'value' => 'false' }
        extras2 = { 'key' => 'free_to_use',    'value' => 'False' }

        records = [{ 'groups' => [], 
                     'tags'   => [],
                     'extras' => [extras1, extras2] }]

        result = subject.unify(records).first
        expectation = { 'free_of_charge' => false, 'free_to_use' => false }
        expect(result['extras']).to eq(expectation)
      end
    end

  end

end
