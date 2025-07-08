require "spec_helper"

RSpec.describe "Write Operations" do
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

  describe "#exec_insert" do
    it "executes INSERT statements" do
      sql = "INSERT INTO users (name, email) VALUES ('John', 'john@example.com')"
      
      # Mock successful execution
      expect(adapter).to receive(:execute_query).with(sql).and_return({})
      
      result = adapter.exec_insert(sql)
      
      # Athena doesn't return generated IDs
      expect(result).to be_nil
    end

    it "handles parameter binding in INSERT statements" do
      sql = "INSERT INTO users (name, email) VALUES (?, ?)"
      binds = ["John", "john@example.com"]
      
      expected_sql = "INSERT INTO users (name, email) VALUES ('John', 'john@example.com')"
      expect(adapter).to receive(:execute_query).with(expected_sql).and_return({})
      
      result = adapter.exec_insert(sql, "SQL", binds)
      expect(result).to be_nil
    end
  end

  describe "#exec_update" do
    it "executes UPDATE statements with warning" do
      sql = "UPDATE users SET name = 'Jane' WHERE id = 1"
      
      # Mock logger
      logger = double("logger")
      allow(ActiveRecord::Base).to receive(:logger).and_return(logger)
      expect(logger).to receive(:warn).with(/UPDATE operations in Athena are limited/)
      allow(logger).to receive(:debug?).and_return(true)
      allow(logger).to receive(:debug)
      
      # Mock successful execution
      expect(adapter).to receive(:execute_query).with(sql).and_return({})
      
      result = adapter.exec_update(sql)
      
      # Athena doesn't return affected row count
      expect(result).to eq(0)
    end

    it "handles parameter binding in UPDATE statements" do
      sql = "UPDATE users SET name = ? WHERE id = ?"
      binds = ["Jane", 1]
      
      expected_sql = "UPDATE users SET name = 'Jane' WHERE id = 1"
      
      # Mock logger
      logger = double("logger")
      allow(ActiveRecord::Base).to receive(:logger).and_return(logger)
      expect(logger).to receive(:warn).with(/UPDATE operations in Athena are limited/)
      allow(logger).to receive(:debug?).and_return(true)
      allow(logger).to receive(:debug)
      
      expect(adapter).to receive(:execute_query).with(expected_sql).and_return({})
      
      result = adapter.exec_update(sql, "SQL", binds)
      expect(result).to eq(0)
    end
  end

  describe "#exec_delete" do
    it "executes DELETE statements with warning" do
      sql = "DELETE FROM users WHERE id = 1"
      
      # Mock logger
      logger = double("logger")
      allow(ActiveRecord::Base).to receive(:logger).and_return(logger)
      expect(logger).to receive(:warn).with(/DELETE operations in Athena are limited/)
      allow(logger).to receive(:debug?).and_return(true)
      allow(logger).to receive(:debug)
      
      # Mock successful execution
      expect(adapter).to receive(:execute_query).with(sql).and_return({})
      
      result = adapter.exec_delete(sql)
      
      # Athena doesn't return affected row count
      expect(result).to eq(0)
    end

    it "handles parameter binding in DELETE statements" do
      sql = "DELETE FROM users WHERE id = ?"
      binds = [1]
      
      expected_sql = "DELETE FROM users WHERE id = 1"
      
      # Mock logger
      logger = double("logger")
      allow(ActiveRecord::Base).to receive(:logger).and_return(logger)
      expect(logger).to receive(:warn).with(/DELETE operations in Athena are limited/)
      allow(logger).to receive(:debug?).and_return(true)
      allow(logger).to receive(:debug)
      
      expect(adapter).to receive(:execute_query).with(expected_sql).and_return({})
      
      result = adapter.exec_delete(sql, "SQL", binds)
      expect(result).to eq(0)
    end
  end

  describe "transaction support" do
    it "indicates no transaction support" do
      expect(adapter.supports_transactions?).to be false
      expect(adapter.supports_savepoints?).to be false
      expect(adapter.supports_lazy_transactions?).to be false
    end

    it "provides no-op transaction methods" do
      # These should not raise errors
      expect { adapter.begin_db_transaction }.not_to raise_error
      expect { adapter.commit_db_transaction }.not_to raise_error
      expect { adapter.rollback_db_transaction }.not_to raise_error
    end
  end
end