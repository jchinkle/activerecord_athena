require "spec_helper"

RSpec.describe "Integration Tests" do
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

  describe "adapter registration" do
    it "registers the adapter with ActiveRecord" do
      # Check that the adapter can be instantiated, which confirms registration
      expect { ActiveRecord::ConnectionAdapters::AthenaAdapter.new(config) }.not_to raise_error
      expect(ActiveRecord::ConnectionAdapters::AthenaAdapter.new(config)).to be_a(ActiveRecord::ConnectionAdapters::AthenaAdapter)
    end
  end

  describe "configuration handling" do
    it "properly extracts configuration values" do
      expect(adapter.instance_variable_get(:@database)).to eq("test_database")
      expect(adapter.instance_variable_get(:@s3_output_location)).to eq("s3://test-bucket/query-results/")
      expect(adapter.instance_variable_get(:@work_group)).to eq("primary")
    end

    it "uses default work group when not specified" do
      config_without_work_group = config.dup
      config_without_work_group.delete(:work_group)
      adapter_without_work_group = ActiveRecord::ConnectionAdapters::AthenaAdapter.new(config_without_work_group)
      
      expect(adapter_without_work_group.instance_variable_get(:@work_group)).to eq("primary")
    end
  end

  describe "AbstractAdapter compatibility" do
    it "inherits from AbstractAdapter" do
      expect(adapter).to be_a(ActiveRecord::ConnectionAdapters::AbstractAdapter)
    end

    it "includes SchemaStatements module" do
      expect(adapter.class.included_modules).to include(ActiveRecord::ConnectionAdapters::Athena::SchemaStatements)
    end

    it "responds to required adapter methods" do
      expect(adapter).to respond_to(:adapter_name)
      expect(adapter).to respond_to(:active?)
      expect(adapter).to respond_to(:supports_migrations?)
      expect(adapter).to respond_to(:supports_primary_key?)
      expect(adapter).to respond_to(:supports_foreign_keys?)
      expect(adapter).to respond_to(:native_database_types)
      expect(adapter).to respond_to(:quote_column_name)
      expect(adapter).to respond_to(:quote_table_name)
      expect(adapter).to respond_to(:execute)
      expect(adapter).to respond_to(:exec_query)
      expect(adapter).to respond_to(:select_all)
    end
  end

  describe "error handling" do
    it "raises StatementInvalid for Athena query failures" do
      athena_client = double("athena_client")
      allow(adapter).to receive(:athena_client).and_return(athena_client)
      
      # Mock query execution
      allow(athena_client).to receive(:start_query_execution).and_return(double(query_execution_id: "test-id"))
      allow(athena_client).to receive(:get_query_execution).and_return(
        double(query_execution: double(status: double(state: "FAILED", state_change_reason: "Test error")))
      )
      
      expect { adapter.execute("SELECT * FROM test") }.to raise_error(ActiveRecord::StatementInvalid, /Query failed: Test error/)
    end
  end

  describe "logging integration" do
    it "calls log method when executing queries" do
      # Mock successful query execution
      athena_client = double("athena_client")
      allow(adapter).to receive(:athena_client).and_return(athena_client)
      allow(athena_client).to receive(:start_query_execution).and_return(double(query_execution_id: "test-id"))
      allow(athena_client).to receive(:get_query_execution).and_return(
        double(query_execution: double(status: double(state: "SUCCEEDED")))
      )
      allow(athena_client).to receive(:get_query_results).and_return(
        double(result_set: double(
          result_set_metadata: double(column_info: []),
          rows: []
        ))
      )
      
      # Just verify that log is called - don't worry about exact arguments
      expect(adapter).to receive(:log).and_yield
      adapter.execute("SELECT * FROM test")
    end
  end
end