require "spec_helper"

RSpec.describe "ActiveRecord Integration" do
  let(:config) do
    {
      adapter: "athena",
      database: "test_database",
      s3_output_location: "s3://test-bucket/query-results/",
      work_group: "primary",
      connection_options: {
        aws_config: {
          region: "us-east-1",
          access_key_id: "test_key",
          secret_access_key: "test_secret"
        }
      }
    }
  end

  let(:adapter) { ActiveRecord::ConnectionAdapters::AthenaAdapter.new(config) }

  describe "ActiveRecord compatibility" do
    it "supports statement cache" do
      expect(adapter.supports_statement_cache?).to be true
    end

    it "supports lazy transactions setting" do
      expect(adapter.supports_lazy_transactions?).to be false
    end

    it "responds to clear_cache!" do
      expect(adapter).to respond_to(:clear_cache!)
    end

    it "has clear_cache! method" do
      # ActiveRecord handles caching, we just need to support the interface
      expect { adapter.clear_cache! }.not_to raise_error
    end
  end

  describe "schema introspection" do
    it "can retrieve columns" do
      # Mock DESCRIBE result
      describe_result = {
        rows: [
          { data: [{ var_char_value: "id" }, { var_char_value: "bigint" }, { var_char_value: "" }] }
        ]
      }
      
      allow(adapter).to receive(:execute).with("DESCRIBE test_table").and_return(describe_result)
      
      columns = adapter.columns("test_table")
      expect(columns.first.name).to eq("id")
      expect(columns.first.sql_type).to eq("bigint")
    end
  end
end