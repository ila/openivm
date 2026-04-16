#include <duckdb.hpp>
#include <iostream>
#include <fstream>
#include <sstream>
#include <iomanip>
#include <ctime>
#include <chrono>
#include <cmath>

namespace ivm_benchmark {

// Timestamp helper (PAC style)
static std::string Timestamp() {
	auto now = std::chrono::system_clock::now();
	std::time_t t = std::chrono::system_clock::to_time_t(now);
	char buf[64];
	std::strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", std::localtime(&t));
	return std::string(buf);
}

// Logging function (PAC style)
static void Log(const std::string &msg) {
	std::cout << "[" << Timestamp() << "] " << msg << std::endl;
}

class TPCCSetup {
private:
	duckdb::DuckDB db;
	duckdb::Connection conn;
	std::vector<std::string> deltas;
	int num_warehouses;
	int customers_per_district;

public:
	TPCCSetup(const std::string &db_path, int sf, int cpd)
		: db(db_path), conn(db), num_warehouses(sf), customers_per_district(cpd) {}

	void CreateSchema() {
		Log("Creating TPC-C schema...");

		conn.Query("DROP TABLE IF EXISTS ORDER_LINE");
		conn.Query("DROP TABLE IF EXISTS STOCK");
		conn.Query("DROP TABLE IF EXISTS ITEM");
		conn.Query("DROP TABLE IF EXISTS HISTORY");
		conn.Query("DROP TABLE IF EXISTS NEW_ORDER");
		conn.Query("DROP TABLE IF EXISTS OORDER");
		conn.Query("DROP TABLE IF EXISTS CUSTOMER");
		conn.Query("DROP TABLE IF EXISTS DISTRICT");
		conn.Query("DROP TABLE IF EXISTS WAREHOUSE");

		conn.Query(R"(CREATE TABLE WAREHOUSE (
			W_ID INT NOT NULL PRIMARY KEY,
			W_YTD DECIMAL(12, 2) NOT NULL,
			W_TAX DECIMAL(4, 4) NOT NULL,
			W_NAME VARCHAR(10) NOT NULL,
			W_STREET_1 VARCHAR(20), W_STREET_2 VARCHAR(20),
			W_CITY VARCHAR(20), W_STATE CHAR(2), W_ZIP CHAR(9)
		))");

		conn.Query(R"(CREATE TABLE DISTRICT (
			D_W_ID INT NOT NULL, D_ID INT NOT NULL,
			D_YTD DECIMAL(12, 2), D_TAX DECIMAL(4, 4),
			D_NEXT_O_ID INT NOT NULL,
			D_NAME VARCHAR(10), D_STREET_1 VARCHAR(20), D_STREET_2 VARCHAR(20),
			D_CITY VARCHAR(20), D_STATE CHAR(2), D_ZIP CHAR(9),
			PRIMARY KEY (D_W_ID, D_ID),
			FOREIGN KEY (D_W_ID) REFERENCES WAREHOUSE(W_ID)
		))");

		conn.Query(R"(CREATE TABLE CUSTOMER (
			C_W_ID INT NOT NULL, C_D_ID INT NOT NULL, C_ID INT NOT NULL,
			C_DISCOUNT DECIMAL(4, 4), C_CREDIT CHAR(2),
			C_LAST VARCHAR(16), C_FIRST VARCHAR(16),
			C_CREDIT_LIM DECIMAL(12, 2), C_BALANCE DECIMAL(12, 2),
			C_YTD_PAYMENT FLOAT, C_PAYMENT_CNT INT, C_DELIVERY_CNT INT,
			C_STREET_1 VARCHAR(20), C_STREET_2 VARCHAR(20),
			C_CITY VARCHAR(20), C_STATE CHAR(2), C_ZIP CHAR(9),
			C_PHONE CHAR(16), C_SINCE TIMESTAMP, C_MIDDLE CHAR(2),
			C_DATA VARCHAR(500),
			PRIMARY KEY (C_W_ID, C_D_ID, C_ID),
			FOREIGN KEY (C_W_ID, C_D_ID) REFERENCES DISTRICT(D_W_ID, D_ID)
		))");

		conn.Query(R"(CREATE TABLE ITEM (
			I_ID INT NOT NULL PRIMARY KEY,
			I_NAME VARCHAR(24), I_PRICE DECIMAL(5, 2),
			I_DATA VARCHAR(50), I_IM_ID INT
		))");

		conn.Query(R"(CREATE TABLE STOCK (
			S_W_ID INT NOT NULL, S_I_ID INT NOT NULL,
			S_QUANTITY INT, S_YTD DECIMAL(8, 2),
			S_ORDER_CNT INT, S_REMOTE_CNT INT, S_DATA VARCHAR(50),
			S_DIST_01 CHAR(24), S_DIST_02 CHAR(24), S_DIST_03 CHAR(24),
			S_DIST_04 CHAR(24), S_DIST_05 CHAR(24), S_DIST_06 CHAR(24),
			S_DIST_07 CHAR(24), S_DIST_08 CHAR(24), S_DIST_09 CHAR(24), S_DIST_10 CHAR(24),
			PRIMARY KEY (S_W_ID, S_I_ID),
			FOREIGN KEY (S_W_ID) REFERENCES WAREHOUSE(W_ID),
			FOREIGN KEY (S_I_ID) REFERENCES ITEM(I_ID)
		))");

		conn.Query(R"(CREATE TABLE OORDER (
			O_W_ID INT NOT NULL, O_D_ID INT NOT NULL, O_ID INT NOT NULL,
			O_C_ID INT NOT NULL, O_CARRIER_ID INT,
			O_OL_CNT INT, O_ALL_LOCAL INT, O_ENTRY_D TIMESTAMP,
			PRIMARY KEY (O_W_ID, O_D_ID, O_ID),
			FOREIGN KEY (O_W_ID, O_D_ID, O_C_ID) REFERENCES CUSTOMER(C_W_ID, C_D_ID, C_ID)
		))");

		conn.Query(R"(CREATE TABLE NEW_ORDER (
			NO_W_ID INT, NO_D_ID INT, NO_O_ID INT,
			PRIMARY KEY (NO_W_ID, NO_D_ID, NO_O_ID),
			FOREIGN KEY (NO_W_ID, NO_D_ID, NO_O_ID) REFERENCES OORDER(O_W_ID, O_D_ID, O_ID)
		))");

		conn.Query(R"(CREATE TABLE ORDER_LINE (
			OL_W_ID INT, OL_D_ID INT, OL_O_ID INT, OL_NUMBER INT,
			OL_I_ID INT, OL_DELIVERY_D TIMESTAMP,
			OL_AMOUNT DECIMAL(6, 2), OL_SUPPLY_W_ID INT, OL_QUANTITY DECIMAL(6, 2),
			OL_DIST_INFO CHAR(24),
			PRIMARY KEY (OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER),
			FOREIGN KEY (OL_W_ID, OL_D_ID, OL_O_ID) REFERENCES OORDER(O_W_ID, O_D_ID, O_ID),
			FOREIGN KEY (OL_SUPPLY_W_ID, OL_I_ID) REFERENCES STOCK(S_W_ID, S_I_ID)
		))");

		conn.Query(R"(CREATE TABLE HISTORY (
			H_C_ID INT, H_C_D_ID INT, H_C_W_ID INT,
			H_D_ID INT, H_W_ID INT, H_DATE TIMESTAMP,
			H_AMOUNT DECIMAL(6, 2), H_DATA VARCHAR(24),
			FOREIGN KEY (H_C_W_ID, H_C_D_ID, H_C_ID) REFERENCES CUSTOMER(C_W_ID, C_D_ID, C_ID),
			FOREIGN KEY (H_W_ID, H_D_ID) REFERENCES DISTRICT(D_W_ID, D_ID)
		))");

		Log("Schema created.");
	}

	void GenerateData() {
		Log("Generating TPC-C base data (SF=" + std::to_string(num_warehouses) + ", " +
			std::to_string(customers_per_district) + " customers/district)...");

		int total_warehouses = 0;
		int total_districts = 0;
		int total_customers = 0;
		int total_items = 100;  // Fixed for laptop
		int total_stock = 0;
		int total_orders = 0;
		int total_orderlines = 0;
		int total_neworders = 0;

		// Warehouses
		Log("  Inserting warehouses...");
		for (int w = 1; w <= num_warehouses; ++w) {
			std::string sql = "INSERT INTO WAREHOUSE VALUES (" + std::to_string(w) +
							  ", 300000.00, 0.1, 'Warehouse', 'S1', 'S2', 'City', 'CA', '12345')";
			conn.Query(sql);
			total_warehouses++;
		}
		Log("    Warehouses: " + std::to_string(total_warehouses));

		// Items
		Log("  Inserting items...");
		std::string item_sql = "INSERT INTO ITEM VALUES ";
		for (int i = 1; i <= total_items; ++i) {
			if (i > 1) item_sql += ", ";
			item_sql += "(" + std::to_string(i) + ", 'Item" + std::to_string(i) + "', 50.00, 'Data', " +
					   std::to_string(i % 1000) + ")";
		}
		conn.Query(item_sql);
		Log("    Items: " + std::to_string(total_items));

		// Districts, Customers, Stock, Orders
		for (int w = 1; w <= num_warehouses; ++w) {
			for (int d = 1; d <= 10; ++d) {
				// District
				std::string dist_sql = "INSERT INTO DISTRICT VALUES (" + std::to_string(w) + ", " +
									   std::to_string(d) + ", 30000.00, 0.1, 11, 'District', 'S1', 'S2', 'City', 'CA', '12345')";
				conn.Query(dist_sql);
				total_districts++;

				// Customers
				for (int c = 1; c <= customers_per_district; ++c) {
					std::string cust_sql = "INSERT INTO CUSTOMER VALUES (" +
										  std::to_string(w) + ", " + std::to_string(d) + ", " + std::to_string(c) +
										  ", 0.15, 'GC', 'LastName', 'FirstName', 50000.00, -5000.00, 0.0, 0, 0, " +
										  "'S1', 'S2', 'City', 'CA', '12345', '1234567890', NOW(), 'MI', 'Data')";
					conn.Query(cust_sql);
					total_customers++;
				}

				// Stock
				for (int i = 1; i <= total_items; ++i) {
					std::string stock_sql = "INSERT INTO STOCK VALUES (" +
										   std::to_string(w) + ", " + std::to_string(i) + ", 50, 0.0, 0, 0, 'StockData', " +
										   "'D01', 'D02', 'D03', 'D04', 'D05', 'D06', 'D07', 'D08', 'D09', 'D10')";
					conn.Query(stock_sql);
					total_stock++;
				}

				// Orders
				for (int o = 1; o <= 10; ++o) {
					int c = ((w + d + o) % customers_per_district) + 1;
					std::string order_sql = "INSERT INTO OORDER VALUES (" +
										   std::to_string(w) + ", " + std::to_string(d) + ", " + std::to_string(o) + ", " +
										   std::to_string(c) + ", NULL, 5, 1, NOW())";
					conn.Query(order_sql);
					total_orders++;

					// Order Lines
					for (int ol = 1; ol <= 3; ++ol) {
						int i = ((w + d + o + ol) % total_items) + 1;
						std::string ol_sql = "INSERT INTO ORDER_LINE VALUES (" +
											std::to_string(w) + ", " + std::to_string(d) + ", " + std::to_string(o) + ", " +
											std::to_string(ol) + ", " + std::to_string(i) + ", NULL, 100.00, " +
											std::to_string(w) + ", 5, 'DistInfo')";
						conn.Query(ol_sql);
						total_orderlines++;
					}
				}

				// New Orders
				for (int o = 1; o <= 10; o += 3) {
					std::string norder_sql = "INSERT INTO NEW_ORDER VALUES (" +
											std::to_string(w) + ", " + std::to_string(d) + ", " + std::to_string(o) + ")";
					conn.Query(norder_sql);
					total_neworders++;
				}
			}

			// Progress
			int pct = (w * 100) / num_warehouses;
			Log("    Warehouse " + std::to_string(w) + "/" + std::to_string(num_warehouses) +
				" (" + std::to_string(pct) + "%)");
		}

		Log("Data generation complete:");
		Log("  Warehouses: " + std::to_string(total_warehouses));
		Log("  Districts: " + std::to_string(total_districts));
		Log("  Customers: " + std::to_string(total_customers));
		Log("  Items: " + std::to_string(total_items));
		Log("  Stock: " + std::to_string(total_stock));
		Log("  Orders: " + std::to_string(total_orders));
		Log("  Order Lines: " + std::to_string(total_orderlines));
		Log("  New Orders: " + std::to_string(total_neworders));
	}

	void SimulateTransactions(int num_tx) {
		Log("Simulating " + std::to_string(num_tx) + " OLTP transactions...");

		// Get warehouse/district/customer info safely
		auto wres = conn.Query("SELECT COUNT(DISTINCT W_ID) FROM WAREHOUSE");
		auto dcount = 10;
		auto cres = conn.Query("SELECT COUNT(DISTINCT C_ID) FROM CUSTOMER");

		if (wres->RowCount() == 0 || cres->RowCount() == 0) {
			Log("Error: No warehouses or customers generated. Skipping transactions.");
			return;
		}

		int wcount = wres->GetValue(0, 0).GetValue<int>();
		int ccount = cres->GetValue(0, 0).GetValue<int>();

		if (wcount == 0 || ccount == 0) {
			Log("Error: Invalid data for transactions. Skipping.");
			return;
		}

		for (int i = 0; i < num_tx; ++i) {
			int w = (i % wcount) + 1;
			int d = ((i / wcount) % dcount) + 1;
			int c = (i % ccount) + 1;

			// Mix of transaction types
			int tx_type = i % 3;

			if (tx_type == 0) {
				// Payment transaction: UPDATE customer balance
				std::stringstream ss;
				ss << std::fixed << std::setprecision(2) << 500.0;
				std::string sql = "UPDATE CUSTOMER SET C_BALANCE = C_BALANCE - " + ss.str() +
								  ", C_PAYMENT_CNT = C_PAYMENT_CNT + 1 WHERE C_W_ID = " +
								  std::to_string(w) + " AND C_D_ID = " + std::to_string(d) +
								  " AND C_ID = " + std::to_string(c);
				conn.Query(sql);
				deltas.push_back(sql);

			} else if (tx_type == 1) {
				// Stock update: Update stock quantity
				int item_id = ((i % 100) + 1);
				std::string sql = std::string("UPDATE STOCK SET S_QUANTITY = S_QUANTITY - 5, S_ORDER_CNT = S_ORDER_CNT + 1 ") +
								  "WHERE S_W_ID = " + std::to_string(w) + " AND S_I_ID = " + std::to_string(item_id);
				conn.Query(sql);
				deltas.push_back(sql);

			} else {
				// Order delivery: Update order line delivery date
				int order_id = ((i % 10) + 1);
				int ol_number = ((i % 3) + 1);
				std::string sql = std::string("UPDATE ORDER_LINE SET OL_DELIVERY_D = NOW() ") +
								  "WHERE OL_W_ID = " + std::to_string(w) + " AND OL_D_ID = " +
								  std::to_string(d) + " AND OL_O_ID = " + std::to_string(order_id) +
								  " AND OL_NUMBER = " + std::to_string(ol_number);
				conn.Query(sql);
				deltas.push_back(sql);
			}

			// Progress logging
			if ((i + 1) % 10 == 0) {
				int pct = ((i + 1) * 100) / num_tx;
				Log("  Transactions: " + std::to_string(i + 1) + "/" + std::to_string(num_tx) +
					" (" + std::to_string(pct) + "%)");
			}
		}

		Log("Transaction simulation complete (" + std::to_string(deltas.size()) + " delta operations).");
	}

	void VerifyData() {
		Log("Verifying data integrity...");

		auto GetCount = [this](const std::string &table) {
			return conn.Query("SELECT COUNT(*) FROM " + table)->GetValue(0, 0).GetValue<int64_t>();
		};

		Log("Final row counts:");
		Log("  WAREHOUSE: " + std::to_string(GetCount("WAREHOUSE")));
		Log("  DISTRICT: " + std::to_string(GetCount("DISTRICT")));
		Log("  CUSTOMER: " + std::to_string(GetCount("CUSTOMER")));
		Log("  ITEM: " + std::to_string(GetCount("ITEM")));
		Log("  STOCK: " + std::to_string(GetCount("STOCK")));
		Log("  OORDER: " + std::to_string(GetCount("OORDER")));
		Log("  ORDER_LINE: " + std::to_string(GetCount("ORDER_LINE")));
		Log("  NEW_ORDER: " + std::to_string(GetCount("NEW_ORDER")));
	}

	void ExportDeltas(const std::string &output_file) {
		Log("Exporting deltas to " + output_file + "...");
		std::ofstream file(output_file);
		for (const auto &delta : deltas) {
			file << delta << ";\n";
		}
		Log("Deltas exported (" + std::to_string(deltas.size()) + " operations).");
	}
};

} // namespace ivm_benchmark

int main(int argc, char **argv) {
	int scale_factor = 3;  // Default SF3
	int customers_per_district = 100;
	std::string db_path = ":memory:";
	int num_transactions = 100;
	std::string output_file = "benchmark/deltas.sql";

	// Simple argument parsing
	for (int i = 1; i < argc; ++i) {
		std::string arg = argv[i];
		if (arg == "--scale" && i + 1 < argc) {
			scale_factor = std::stoi(argv[++i]);
		} else if (arg == "--customers" && i + 1 < argc) {
			customers_per_district = std::stoi(argv[++i]);
		} else if (arg == "--db" && i + 1 < argc) {
			db_path = argv[++i];
		} else if (arg == "--transactions" && i + 1 < argc) {
			num_transactions = std::stoi(argv[++i]);
		} else if (arg == "--output" && i + 1 < argc) {
			output_file = argv[++i];
		}
	}

	ivm_benchmark::Log("╔════════════════════════════════════════════════════════╗");
	ivm_benchmark::Log("║       TPC-C Data Generation for OpenIVM Testing        ║");
	ivm_benchmark::Log("╚════════════════════════════════════════════════════════╝");
	ivm_benchmark::Log("Configuration:");
	ivm_benchmark::Log("  Scale factor: " + std::to_string(scale_factor));
	ivm_benchmark::Log("  Customers per district: " + std::to_string(customers_per_district));
	ivm_benchmark::Log("  Transactions: " + std::to_string(num_transactions));
	ivm_benchmark::Log("  Database: " + db_path);
	ivm_benchmark::Log("  Output file: " + output_file);
	ivm_benchmark::Log("");

	try {
		ivm_benchmark::TPCCSetup setup(db_path, scale_factor, customers_per_district);
		setup.CreateSchema();
		setup.GenerateData();
		setup.SimulateTransactions(num_transactions);
		setup.VerifyData();
		setup.ExportDeltas(output_file);

		ivm_benchmark::Log("");
		ivm_benchmark::Log("✓ Setup complete! Database ready for IVM testing.");

	} catch (const std::exception &e) {
		ivm_benchmark::Log("✗ Error: " + std::string(e.what()));
		return 1;
	}

	return 0;
}
