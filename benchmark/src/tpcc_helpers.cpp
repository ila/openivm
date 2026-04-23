#include "tpcc_helpers.hpp"

#include "duckdb/common/printer.hpp"

#include <chrono>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <iomanip>
#include <random>
#include <sstream>
#include <unistd.h>

namespace openivm_bench {

std::string Timestamp() {
	auto now = std::chrono::system_clock::now();
	std::time_t t = std::chrono::system_clock::to_time_t(now);
	char buf[64];
	std::strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", std::localtime(&t));
	return std::string(buf);
}

void Log(const std::string &msg) {
	duckdb::Printer::Print("[" + Timestamp() + "] " + msg);
}

bool WriteAllBytes(int fd, const void *buf, size_t n) {
	const char *p = static_cast<const char *>(buf);
	while (n > 0) {
		ssize_t w = write(fd, p, n);
		if (w <= 0) {
			return false;
		}
		p += w;
		n -= static_cast<size_t>(w);
	}
	return true;
}

bool ReadAllBytes(int fd, void *buf, size_t n) {
	char *p = static_cast<char *>(buf);
	while (n > 0) {
		ssize_t r = read(fd, p, n);
		if (r <= 0) {
			return false;
		}
		p += r;
		n -= static_cast<size_t>(r);
	}
	return true;
}

bool FileExists(const std::string &path) {
	struct stat buffer;
	return (stat(path.c_str(), &buffer) == 0);
}

void CreateTPCCSchema(duckdb::Connection &con) {
	con.Query("CREATE TABLE WAREHOUSE (W_ID INT, W_YTD DECIMAL(12, 2), W_TAX DECIMAL(4, 4), W_NAME VARCHAR(10), "
	          "W_STREET_1 VARCHAR(20), W_STREET_2 VARCHAR(20), W_CITY VARCHAR(20), W_STATE CHAR(2), W_ZIP CHAR(9))");
	con.Query("CREATE TABLE DISTRICT (D_W_ID INT, D_ID INT, D_YTD DECIMAL(12, 2), D_TAX DECIMAL(4, 4), D_NEXT_O_ID "
	          "INT, D_NAME VARCHAR(10), D_STREET_1 VARCHAR(20), D_STREET_2 VARCHAR(20), D_CITY VARCHAR(20), D_STATE "
	          "CHAR(2), D_ZIP CHAR(9))");
	con.Query("CREATE TABLE CUSTOMER (C_W_ID INT, C_D_ID INT, C_ID INT, C_DISCOUNT DECIMAL(4, 4), C_CREDIT CHAR(2), "
	          "C_LAST VARCHAR(16), C_FIRST VARCHAR(16), C_CREDIT_LIM DECIMAL(12, 2), C_BALANCE DECIMAL(12, 2), "
	          "C_YTD_PAYMENT FLOAT, C_PAYMENT_CNT INT, C_DELIVERY_CNT INT, C_STREET_1 VARCHAR(20), C_STREET_2 "
	          "VARCHAR(20), C_CITY VARCHAR(20), C_STATE CHAR(2), C_ZIP CHAR(9), C_PHONE CHAR(16), C_SINCE TIMESTAMP, "
	          "C_MIDDLE CHAR(2), C_DATA VARCHAR(500))");
	con.Query("CREATE TABLE ITEM (I_ID INT, I_NAME VARCHAR(24), I_PRICE DECIMAL(5, 2), I_DATA VARCHAR(50), I_IM_ID "
	          "INT)");
	con.Query("CREATE TABLE STOCK (S_W_ID INT, S_I_ID INT, S_QUANTITY INT, S_YTD DECIMAL(8, 2), S_ORDER_CNT INT, "
	          "S_REMOTE_CNT INT, S_DATA VARCHAR(50), S_DIST_01 CHAR(24), S_DIST_02 CHAR(24), S_DIST_03 CHAR(24), "
	          "S_DIST_04 CHAR(24), S_DIST_05 CHAR(24), S_DIST_06 CHAR(24), S_DIST_07 CHAR(24), S_DIST_08 CHAR(24), "
	          "S_DIST_09 CHAR(24), S_DIST_10 CHAR(24))");
	con.Query("CREATE TABLE OORDER (O_W_ID INT, O_D_ID INT, O_ID INT, O_C_ID INT, O_CARRIER_ID INT, O_OL_CNT INT, "
	          "O_ALL_LOCAL INT, O_ENTRY_D TIMESTAMP)");
	con.Query("CREATE TABLE NEW_ORDER (NO_W_ID INT, NO_D_ID INT, NO_O_ID INT)");
	con.Query("CREATE TABLE ORDER_LINE (OL_W_ID INT, OL_D_ID INT, OL_O_ID INT, OL_NUMBER INT, OL_I_ID INT, "
	          "OL_DELIVERY_D TIMESTAMP, OL_AMOUNT DECIMAL(6, 2), OL_SUPPLY_W_ID INT, OL_QUANTITY DECIMAL(6, 2), "
	          "OL_DIST_INFO CHAR(24))");
	con.Query("CREATE TABLE HISTORY (H_C_ID INT, H_C_D_ID INT, H_C_W_ID INT, H_D_ID INT, H_W_ID INT, H_DATE "
	          "TIMESTAMP, H_AMOUNT DECIMAL(6, 2), H_DATA VARCHAR(24))");
}

void InsertTPCCData(duckdb::Connection &con, int scale_factor) {
	int num_warehouses = scale_factor;
	int num_items = 100;

	for (int w = 1; w <= num_warehouses; w++) {
		con.Query("INSERT INTO WAREHOUSE VALUES (" + std::to_string(w) +
		          ", 300000.00, 0.0500, 'Warehouse" + std::to_string(w) +
		          "', 'Street1', 'Street2', 'City', 'ST', '123456789')");
	}
	for (int w = 1; w <= num_warehouses; w++) {
		for (int d = 1; d <= 10; d++) {
			con.Query("INSERT INTO DISTRICT VALUES (" + std::to_string(w) + ", " + std::to_string(d) +
			          ", 30000.00, 0.0500, 1, 'District" + std::to_string(d) +
			          "', 'Street1', 'Street2', 'City', 'ST', '123456789')");
		}
	}
	for (int w = 1; w <= num_warehouses; w++) {
		for (int d = 1; d <= 10; d++) {
			for (int c = 1; c <= 30; c++) {
				con.Query("INSERT INTO CUSTOMER VALUES (" + std::to_string(w) + ", " + std::to_string(d) + ", " +
				          std::to_string(c) +
				          ", 0.05, 'GC', 'LastName', 'FirstName', 50000.00, 10000.00, 0.0, 0, 0, 'St1', 'St2', "
				          "'City', 'ST', '123456789', '1234567890123456', NOW(), 'M', 'data')");
			}
		}
	}
	for (int i = 1; i <= num_items; i++) {
		con.Query("INSERT INTO ITEM VALUES (" + std::to_string(i) + ", 'Item" + std::to_string(i) + "', " +
		          std::to_string(10 + (i % 90)) + ".99, 'ItemData', " + std::to_string((i % 10) + 1) + ")");
	}
	for (int w = 1; w <= num_warehouses; w++) {
		for (int i = 1; i <= num_items; i++) {
			con.Query("INSERT INTO STOCK VALUES (" + std::to_string(w) + ", " + std::to_string(i) + ", " +
			          std::to_string(50 + (i % 50)) +
			          ", 0.00, 0, 0, 'StockData', 'Dist1', 'Dist2', 'Dist3', 'Dist4', 'Dist5', 'Dist6', 'Dist7', "
			          "'Dist8', 'Dist9', 'Dist10')");
		}
	}
	for (int w = 1; w <= num_warehouses; w++) {
		for (int d = 1; d <= 10; d++) {
			for (int o = 1; o <= 5; o++) {
				con.Query("INSERT INTO OORDER VALUES (" + std::to_string(w) + ", " + std::to_string(d) + ", " +
				          std::to_string(o) + ", " + std::to_string((o % 30) + 1) + ", NULL, 5, 1, NOW())");
			}
		}
	}
	for (int w = 1; w <= num_warehouses; w++) {
		for (int d = 1; d <= 10; d++) {
			for (int o = 1; o <= 5; o++) {
				for (int ol = 1; ol <= 3; ol++) {
					con.Query("INSERT INTO ORDER_LINE VALUES (" + std::to_string(w) + ", " + std::to_string(d) + ", " +
					          std::to_string(o) + ", " + std::to_string(ol) + ", " + std::to_string((ol % 10) + 1) +
					          ", NULL, " + std::to_string(10 + (ol * 5)) + ".00, " + std::to_string(w) +
					          ", 5.00, 'DistInfo')");
				}
			}
		}
	}
}

std::vector<std::string> GenerateDeltaPool(int scale_factor) {
	std::vector<std::string> deltas;
	std::mt19937 rng(42);
	std::uniform_int_distribution<> type_dist(0, 99);
	std::uniform_int_distribution<> w_dist(1, scale_factor);
	std::uniform_int_distribution<> d_dist(1, 10);
	std::uniform_int_distribution<> c_dist(1, 30);
	std::uniform_int_distribution<> i_dist(1, 100);
	std::uniform_int_distribution<> amount_dist(50, 500);

	for (int i = 0; i < 500; i++) {
		int type = type_dist(rng);
		int w = w_dist(rng);
		int d = d_dist(rng);
		int c = c_dist(rng);
		int item = i_dist(rng);
		int amt = amount_dist(rng);

		double balance = -1.0 * amt;
		int qty = 50 + (i % 50);
		if (type < 40) {
			deltas.push_back("UPDATE CUSTOMER SET C_BALANCE = " + std::to_string(balance) +
			                 ", C_PAYMENT_CNT = " + std::to_string(i % 10) + " WHERE C_W_ID = " + std::to_string(w) +
			                 " AND C_D_ID = " + std::to_string(d) + " AND C_ID = " + std::to_string(c));
		} else if (type < 60) {
			deltas.push_back("UPDATE STOCK SET S_QUANTITY = " + std::to_string(qty) +
			                 ", S_ORDER_CNT = " + std::to_string(i % 20) + " WHERE S_W_ID = " + std::to_string(w) +
			                 " AND S_I_ID = " + std::to_string(item));
		} else if (type < 75) {
			deltas.push_back("UPDATE ORDER_LINE SET OL_DELIVERY_D = '2026-01-01 00:00:00' WHERE OL_W_ID = " +
			                 std::to_string(w) + " AND OL_D_ID = " + std::to_string(d) +
			                 " AND OL_O_ID = 1 AND OL_NUMBER = 1");
		} else if (type < 85) {
			deltas.push_back("INSERT INTO HISTORY VALUES (" + std::to_string(c) + ", " + std::to_string(d) + ", " +
			                 std::to_string(w) + ", " + std::to_string(d) + ", " + std::to_string(w) +
			                 ", '2026-01-01 00:00:00', " + std::to_string(amt) + ".00, 'Payment')");
		} else if (type < 90) {
			deltas.push_back("INSERT INTO NEW_ORDER VALUES (" + std::to_string(w) + ", " + std::to_string(d) + ", 1)");
		} else if (type < 95) {
			deltas.push_back("DELETE FROM NEW_ORDER WHERE NO_W_ID = " + std::to_string(w) +
			                 " AND NO_D_ID = " + std::to_string(d) + " LIMIT 1");
		} else {
			deltas.push_back("UPDATE WAREHOUSE SET W_YTD = " + std::to_string(amt * 100) +
			                 ".00 WHERE W_ID = " + std::to_string(w));
		}
	}
	return deltas;
}

} // namespace openivm_bench
