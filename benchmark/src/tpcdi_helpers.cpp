#include "tpcdi_helpers.hpp"

#include <string>
#include <vector>

namespace openivm_bench {

// Run SQL and silently ignore errors (used for pre-computed derived tables).
static void Exec(duckdb::Connection &con, const std::string &sql) {
	auto r = con.Query(sql);
	(void)r;
}

// ──────────────────────────────────────────────────────────────────────────────
// CreateTPCDISchema
// Creates all raw batch tables (leaf nodes that receive deltas) plus the flat
// pre-computed bronze tables for the crm/finwire paths that would otherwise
// require XML struct or fixed-width line parsing.
// ──────────────────────────────────────────────────────────────────────────────
void CreateTPCDISchema(duckdb::Connection &con) {
	// ── Raw batch tables ──────────────────────────────────────────────────────

	const char *trade_ddl =
	    "(t_id VARCHAR, t_dts TIMESTAMP, t_st_id VARCHAR, t_tt_id VARCHAR,"
	    " t_is_cash BOOLEAN, t_s_symb VARCHAR, t_qty INT, t_bid_price DOUBLE,"
	    " t_ca_id BIGINT, t_exec_name VARCHAR, t_trade_price DOUBLE,"
	    " t_chrg DOUBLE, t_comm DOUBLE, t_tax DOUBLE)";
	con.Query(std::string("CREATE TABLE batch1_trade ") + trade_ddl);
	con.Query(std::string("CREATE TABLE batch2_trade ") + trade_ddl);
	con.Query(std::string("CREATE TABLE batch3_trade ") + trade_ddl);

	con.Query("CREATE TABLE batch1_trade_history"
	          "(th_t_id VARCHAR, th_dts TIMESTAMP, th_st_id VARCHAR)");

	const char *dm_ddl = "(dm_date DATE, dm_s_symb VARCHAR,"
	                     " dm_close DOUBLE, dm_high DOUBLE, dm_low DOUBLE, dm_vol BIGINT)";
	con.Query(std::string("CREATE TABLE batch1_daily_market ") + dm_ddl);
	con.Query(std::string("CREATE TABLE batch2_daily_market ") + dm_ddl);
	con.Query(std::string("CREATE TABLE batch3_daily_market ") + dm_ddl);

	const char *hh_ddl = "(hh_h_t_id VARCHAR, hh_t_id VARCHAR,"
	                     " hh_before_qty INT, hh_after_qty INT)";
	con.Query(std::string("CREATE TABLE batch1_holding_history ") + hh_ddl);
	con.Query(std::string("CREATE TABLE batch2_holding_history ") + hh_ddl);
	con.Query(std::string("CREATE TABLE batch3_holding_history ") + hh_ddl);

	const char *wh_ddl = "(w_c_id BIGINT, w_s_symb VARCHAR,"
	                     " w_dts TIMESTAMP, w_action VARCHAR)";
	con.Query(std::string("CREATE TABLE batch1_watch_history ") + wh_ddl);
	con.Query(std::string("CREATE TABLE batch2_watch_history ") + wh_ddl);
	con.Query(std::string("CREATE TABLE batch3_watch_history ") + wh_ddl);

	const char *ct_ddl = "(ct_ca_id BIGINT, ct_dts TIMESTAMP,"
	                     " ct_amt DOUBLE, ct_name VARCHAR)";
	con.Query(std::string("CREATE TABLE batch1_cash_transaction ") + ct_ddl);
	con.Query(std::string("CREATE TABLE batch2_cash_transaction ") + ct_ddl);
	con.Query(std::string("CREATE TABLE batch3_cash_transaction ") + ct_ddl);

	con.Query("CREATE TABLE batch1_hr"
	          "(employeeid BIGINT, managerid BIGINT,"
	          " employeefirstname VARCHAR, employeelastname VARCHAR, employeemi VARCHAR,"
	          " employeejobcode VARCHAR, employeebranch VARCHAR,"
	          " employeeoffice VARCHAR, employeephone VARCHAR)");

	con.Query("CREATE TABLE batch1_status_type(st_id VARCHAR, st_name VARCHAR)");
	con.Query("CREATE TABLE batch1_tax_rate(tx_id VARCHAR, tx_name VARCHAR, tx_rate DOUBLE)");
	con.Query("CREATE TABLE batch1_trade_type(tt_id VARCHAR, tt_name VARCHAR)");
	con.Query("CREATE TABLE batch1_industry(in_id VARCHAR, in_name VARCHAR, in_sc_id VARCHAR)");

	con.Query("CREATE TABLE batch1_date("
	          "sk_dateid BIGINT, datevalue DATE, datedesc VARCHAR,"
	          " calendaryearid INT, calendaryeardesc VARCHAR,"
	          " calendarqtrid INT, calendarqtrdesc VARCHAR,"
	          " calendarmonthid INT, calendarmonthdesc VARCHAR,"
	          " calendarweekid INT, calendarweekdesc VARCHAR,"
	          " dayofweeknum INT, dayofweekdesc VARCHAR,"
	          " fiscalyearid INT, fiscalyeardesc VARCHAR,"
	          " fiscalqtrid INT, fiscalqtrdesc VARCHAR,"
	          " holidayflag BOOLEAN)");

	const char *prospect_ddl =
	    "(agencyid VARCHAR, lastname VARCHAR, firstname VARCHAR,"
	    " middleinitial VARCHAR, gender VARCHAR,"
	    " addressline1 VARCHAR, addressline2 VARCHAR, postalcode VARCHAR,"
	    " city VARCHAR, state VARCHAR, country VARCHAR, phone VARCHAR,"
	    " income DOUBLE, numbercars INT, numberchildren INT,"
	    " maritalstatus VARCHAR, age INT, creditrating INT,"
	    " ownorrentflag VARCHAR, employer VARCHAR,"
	    " numbercreditcards INT, networth DOUBLE)";
	con.Query(std::string("CREATE TABLE batch1_prospect ") + prospect_ddl);
	con.Query(std::string("CREATE TABLE batch2_prospect ") + prospect_ddl);
	con.Query(std::string("CREATE TABLE batch3_prospect ") + prospect_ddl);

	// ── Raw finwire + CRM staging tables (bronze-layer input) ───────────────
	// batch1_finwire: fixed-width lines parsed by query0008/0009/0010.
	// batch1_customer_mgmt: XML struct parsed by query0007.
	// batch2/3_customer + batch2/3_account: CDC flat files for query0007 batches 2/3.
	con.Query("CREATE TABLE batch1_finwire(pts TIMESTAMP, rec_type VARCHAR, line VARCHAR)");

	con.Query("CREATE TABLE batch1_customer_mgmt("
	          " \"_ActionTS\" TIMESTAMP,"
	          " \"_ActionType\" VARCHAR,"
	          " \"Customer\" STRUCT("
	          "  \"_C_ID\" BIGINT,"
	          "  \"_C_TAX_ID\" VARCHAR,"
	          "  \"_C_GNDR\" VARCHAR,"
	          "  \"_C_TIER\" VARCHAR,"
	          "  \"_C_DOB\" DATE,"
	          "  \"Name\" STRUCT(\"C_L_NAME\" VARCHAR, \"C_F_NAME\" VARCHAR, \"C_M_NAME\" VARCHAR),"
	          "  \"Address\" STRUCT(\"C_ADLINE1\" VARCHAR, \"C_ADLINE2\" VARCHAR,"
	          "   \"C_ZIPCODE\" VARCHAR, \"C_CITY\" VARCHAR, \"C_STATE_PROV\" VARCHAR, \"C_CTRY\" VARCHAR),"
	          "  \"ContactInfo\" STRUCT("
	          "   \"C_PRIM_EMAIL\" VARCHAR, \"C_ALT_EMAIL\" VARCHAR,"
	          "   \"C_PHONE_1\" STRUCT(\"C_CTRY_CODE\" VARCHAR, \"C_AREA_CODE\" VARCHAR,"
	          "    \"C_LOCAL\" VARCHAR, \"C_EXT\" VARCHAR),"
	          "   \"C_PHONE_2\" STRUCT(\"C_CTRY_CODE\" VARCHAR, \"C_AREA_CODE\" VARCHAR,"
	          "    \"C_LOCAL\" VARCHAR, \"C_EXT\" VARCHAR),"
	          "   \"C_PHONE_3\" STRUCT(\"C_CTRY_CODE\" VARCHAR, \"C_AREA_CODE\" VARCHAR,"
	          "    \"C_LOCAL\" VARCHAR, \"C_EXT\" VARCHAR)),"
	          "  \"TaxInfo\" STRUCT(\"C_LCL_TX_ID\" VARCHAR, \"C_NAT_TX_ID\" VARCHAR),"
	          "  \"Account\" STRUCT(\"_CA_ID\" BIGINT, \"_CA_TAX_ST\" VARCHAR,"
	          "   \"CA_B_ID\" BIGINT, \"CA_NAME\" VARCHAR)"
	          " ))");

	const char *cust_cdc_ddl =
	    "(cdc_dsn BIGINT, cdc_flag VARCHAR,"
	    " customerid BIGINT, taxid VARCHAR, gender VARCHAR, tier INT, dob DATE,"
	    " lastname VARCHAR, firstname VARCHAR, middleinitial VARCHAR,"
	    " addressline1 VARCHAR, addressline2 VARCHAR, postalcode VARCHAR,"
	    " city VARCHAR, stateprov VARCHAR, country VARCHAR,"
	    " email1 VARCHAR, email2 VARCHAR,"
	    " c_ctry_1 VARCHAR, c_area_1 VARCHAR, c_local_1 VARCHAR, c_ext_1 VARCHAR,"
	    " c_ctry_2 VARCHAR, c_area_2 VARCHAR, c_local_2 VARCHAR, c_ext_2 VARCHAR,"
	    " c_ctry_3 VARCHAR, c_area_3 VARCHAR, c_local_3 VARCHAR, c_ext_3 VARCHAR,"
	    " lcl_tx_id VARCHAR, nat_tx_id VARCHAR)";
	con.Query(std::string("CREATE TABLE batch2_customer ") + cust_cdc_ddl);
	con.Query(std::string("CREATE TABLE batch3_customer ") + cust_cdc_ddl);

	const char *acct_cdc_ddl =
	    "(cdc_dsn BIGINT, cdc_flag VARCHAR,"
	    " ca_c_id BIGINT, accountid BIGINT, taxstatus INT, ca_b_id BIGINT, accountdesc VARCHAR)";
	con.Query(std::string("CREATE TABLE batch2_account ") + acct_cdc_ddl);
	con.Query(std::string("CREATE TABLE batch3_account ") + acct_cdc_ddl);

	// ── Pre-computed flat bronze tables (bypass XML/finwire parsing) ──────────
	// These are the direct source tables for the silver-layer queries in the
	// benchmark.  We populate them directly in InsertTPCDIData() with synthetic
	// data rather than re-parsing batch1_customer_mgmt (XML struct) or
	// batch1_finwire (fixed-width lines).

	con.Query("CREATE TABLE crm_customer_mgmt("
	          " action_ts TIMESTAMP, action_type VARCHAR,"
	          " c_id BIGINT, c_tax_id VARCHAR, c_gndr VARCHAR, c_tier INT, c_dob DATE,"
	          " c_l_name VARCHAR, c_f_name VARCHAR, c_m_name VARCHAR,"
	          " c_adline1 VARCHAR, c_adline2 VARCHAR, c_zipcode VARCHAR,"
	          " c_city VARCHAR, c_state_prov VARCHAR, c_ctry VARCHAR,"
	          " c_prim_email VARCHAR, c_alt_email VARCHAR,"
	          " c_phone_1 VARCHAR, c_phone_2 VARCHAR, c_phone_3 VARCHAR,"
	          " c_lcl_tx_id VARCHAR, c_nat_tx_id VARCHAR,"
	          " ca_id BIGINT, ca_b_id BIGINT, ca_tax_st INT, ca_name VARCHAR)");

	con.Query("CREATE TABLE finwire_company("
	          " pts TIMESTAMP, company_name VARCHAR, cik VARCHAR,"
	          " status VARCHAR, industry_id VARCHAR, sp_rating VARCHAR,"
	          " founding_date DATE, address_line1 VARCHAR, address_line2 VARCHAR,"
	          " postal_code VARCHAR, city VARCHAR, state_province VARCHAR,"
	          " country VARCHAR, ceo_name VARCHAR, description VARCHAR)");

	con.Query("CREATE TABLE finwire_financial("
	          " pts TIMESTAMP, year INT, quarter INT,"
	          " quarter_start_date DATE, posting_date DATE,"
	          " revenue DOUBLE, earnings DOUBLE, eps DOUBLE, diluted_eps DOUBLE,"
	          " margin DOUBLE, inventory DOUBLE, assets DOUBLE, liabilities DOUBLE,"
	          " sh_out BIGINT, diluted_sh_out BIGINT,"
	          " cik BIGINT, company_name VARCHAR)");

	con.Query("CREATE TABLE finwire_security("
	          " pts TIMESTAMP, symbol VARCHAR, issue_type VARCHAR,"
	          " status VARCHAR, name VARCHAR, ex_id VARCHAR,"
	          " sh_out BIGINT, first_trade_date DATE, first_exchange_date DATE,"
	          " dividend DOUBLE, cik BIGINT, company_name VARCHAR)");
}

// ──────────────────────────────────────────────────────────────────────────────
// InsertTPCDIData
// Populates all tables (batch, reference, and pre-computed intermediates)
// and then materialises each layer of the DAG via CTAS so that every query in
// the benchmark has all its source tables already present.
// ──────────────────────────────────────────────────────────────────────────────
void InsertTPCDIData(duckdb::Connection &con, int scale_factor) {
	const int n = scale_factor;

	// ── Reference data ────────────────────────────────────────────────────────
	for (const char *r : {"('ACTV','Active')", "('CMPT','Completed')",
	                      "('SBMT','Submitted')", "('CNCL','Canceled')"})
		con.Query(std::string("INSERT INTO batch1_status_type VALUES ") + r);

	for (const char *r : {"('TX_LOCAL','Local Tax',0.05)", "('TX_NAT','National Tax',0.15)"})
		con.Query(std::string("INSERT INTO batch1_tax_rate VALUES ") + r);

	for (const char *r : {"('TMB','Market Buy')", "('TMS','Market Sell')", "('TLB','Limit Buy')"})
		con.Query(std::string("INSERT INTO batch1_trade_type VALUES ") + r);

	for (const char *r :
	     {"('IND1','Technology','TECH')", "('IND2','Finance','FIN')", "('IND3','Healthcare','HLT')"})
		con.Query(std::string("INSERT INTO batch1_industry VALUES ") + r);

	// 20 representative dates in Jan-Feb 2023
	{
		const char *months[] = {"January", "February"};
		const char *dow_name[] = {"Monday", "Tuesday", "Wednesday", "Thursday",
		                          "Friday", "Saturday", "Sunday"};
		for (int d = 0; d < 20; d++) {
			int month = 1 + (d / 14); // 0-13 → Jan, 14-19 → Feb
			int day = (d % 14) + 1;
			int dow = d % 7;
			int week = d / 7 + 1;
			std::string date = std::string("2023-0") + std::to_string(month) + "-" +
			                   (day < 10 ? "0" : "") + std::to_string(day);
			int sk = 20230000 + month * 100 + day;
			con.Query("INSERT INTO batch1_date VALUES (" + std::to_string(sk) +
			          ",DATE '" + date + "','Date " + date + "',"
			          "2023,'Year 2023'," +
			          std::to_string(month == 1 ? 1 : 1) + ",'Q1 2023'," +
			          std::to_string(month) + ",'" + months[month - 1] + "'," +
			          std::to_string(week) + ",'Week " + std::to_string(week) + "'," +
			          std::to_string(dow) + ",'" + dow_name[dow] + "'," +
			          "2023,'FY2023',1,'FQ1 2023'," + (dow >= 5 ? "true" : "false") + ")");
		}
	}

	// ── Employees ─────────────────────────────────────────────────────────────
	int n_emp = n * 3;
	for (int e = 1; e <= n_emp; e++) {
		long long mgr = (e == 1) ? 1 : 1; // all report to employee 1
		con.Query("INSERT INTO batch1_hr VALUES (" + std::to_string(e) + "," +
		          std::to_string(mgr) + ",'First" + std::to_string(e) + "','Last" +
		          std::to_string(e) + "','M','Trader','Branch1','Office1','555-" +
		          std::to_string(1000 + e) + "')");
	}

	// ── Prospects ─────────────────────────────────────────────────────────────
	int n_pros = n * 3;
	for (int p = 1; p <= n_pros; p++) {
		const std::string row =
		    "('AG" + std::to_string(p) + "','Last" + std::to_string(p) + "','First" +
		    std::to_string(p) + "','M','M','123 Main St','','10001','New York','NY','US',"
		    "'555-1001',80000.0,2,1,'M',35,700,'R','Corp',3,150000.0)";
		con.Query("INSERT INTO batch1_prospect VALUES " + row);
		con.Query("INSERT INTO batch2_prospect VALUES " + row);
		con.Query("INSERT INTO batch3_prospect VALUES " + row);
	}

	// ── Companies (finwire_company) ───────────────────────────────────────────
	int n_co = n * 3;
	const char *ind_ids[] = {"IND1", "IND2", "IND3"};
	for (int c = 1; c <= n_co; c++) {
		std::string cik = std::to_string(1000 + c);
		std::string ind = ind_ids[(c - 1) % 3];
		con.Query("INSERT INTO finwire_company VALUES("
		          "TIMESTAMP '2020-01-01 00:00:00',"
		          "'Company " +
		          std::to_string(c) + "','" + cik +
		          "',"
		          "'ACTV','" +
		          ind +
		          "','A+',"
		          "DATE '2000-01-01','1 Corp St','','10001','New York','NY','US',"
		          "'CEO " +
		          std::to_string(c) + "','Description " + std::to_string(c) + "')");

		// Two quarters of financials per company
		for (int q = 1; q <= 2; q++) {
			std::string qs = "2023-" + std::to_string((q - 1) * 3 + 1) + "-01";
			std::string ps = "2023-" + std::to_string((q - 1) * 3 + 2) + "-15";
			con.Query("INSERT INTO finwire_financial VALUES("
			          "TIMESTAMP '2020-01-01 00:00:00',"
			          "2023," +
			          std::to_string(q) + ",DATE '" + qs + "',DATE '" + ps + "',"
			          "1000000.0,150000.0,1.5,1.4,0.15,500000.0,2000000.0,800000.0,"
			          "1000000,1000000," +
			          std::to_string(1000 + c) + ",NULL)");
		}
	}

	// ── Securities (finwire_security) ─────────────────────────────────────────
	int n_sec = n * 5;
	for (int s = 1; s <= n_sec; s++) {
		std::string sym = std::string("SYM") + (s < 10 ? "00" : s < 100 ? "0" : "") + std::to_string(s);
		long long cik = 1000 + ((s - 1) % n_co) + 1;
		con.Query("INSERT INTO finwire_security VALUES("
		          "TIMESTAMP '2020-01-01 00:00:00','" +
		          sym +
		          "','CS','ACTV','"
		          "" +
		          sym + " Inc','NYSE',1000000,DATE '2010-01-01',DATE '2010-01-15',1.50," +
		          std::to_string(cik) + ",NULL)");
	}

	// ── Customers + Accounts (crm_customer_mgmt) ─────────────────────────────
	int n_cust = n * 3;
	int n_acct = n * 3;
	for (int i = 1; i <= n_cust; i++) {
		long long cid = 2000 + i;  // customer ID
		long long aid = 3000 + i;  // account ID
		long long bid = 1 + ((i - 1) % n_emp); // broker (employee) ID
		con.Query("INSERT INTO crm_customer_mgmt VALUES("
		          "TIMESTAMP '2020-01-01 00:00:00','NEW'," +
		          std::to_string(cid) + ",'TAX" + std::to_string(i) + "',"
		          "'M'," +
		          std::to_string(1 + i % 3) + ","
		          "DATE '1980-01-" +
		          std::to_string(1 + i % 28) + "',"
		          "'Last" +
		          std::to_string(i) + "','First" + std::to_string(i) + "','M',"
		          "'123 Main St','','10001','New York','NY','US',"
		          "'user" +
		          std::to_string(i) + "@test.com','',"
		          "'1-555-1001','','','TX_LOCAL','TX_NAT'," +
		          std::to_string(aid) + "," + std::to_string(bid) + ",1,'Account " +
		          std::to_string(aid) + "')");
	}

	// ── Trades + history + holdings + watches ─────────────────────────────────
	int n_trades = n * 10;
	const char *tt_ids[] = {"TMB", "TMS", "TLB"};
	for (int t = 1; t <= n_trades; t++) {
		std::string tid = std::string("T") + (t < 10 ? "000" : t < 100 ? "00" : "0") + std::to_string(t);
		int sym_idx = ((t - 1) % n_sec) + 1;
		long long aid = 3000 + ((t - 1) % n_acct) + 1;
		std::string sym = std::string("SYM") + (sym_idx < 10 ? "00" : sym_idx < 100 ? "0" : "") + std::to_string(sym_idx);
		std::string tt = tt_ids[(t - 1) % 3];
		double price = 10.0 + sym_idx * 2.5;
		int qty = 100 + t * 10;
		int day = 1 + (t - 1) % 28;
		int hour = 9 + (t % 8);
		std::string ts = std::string("TIMESTAMP '2023-01-") + (day < 10 ? "0" : "") + std::to_string(day) + " " +
		                 (hour < 10 ? "0" : "") + std::to_string(hour) + ":00:00'";
		std::string ts2 = std::string("TIMESTAMP '2023-01-") + (day < 10 ? "0" : "") + std::to_string(day) + " " +
		                  (hour + 1 < 10 ? "0" : "") + std::to_string(hour + 1) + ":00:00'";

		// batch1_trade
		con.Query("INSERT INTO batch1_trade VALUES('" + tid + "'," + ts + ",'ACTV','" + tt +
		          "',true,'" + sym + "'," + std::to_string(qty) + "," +
		          std::to_string(static_cast<int>(price)) + ".0," + std::to_string(aid) +
		          ",'Exec1'," + std::to_string(static_cast<int>(price * 1.01)) + ".0,5.0,2.0,1.5)");

		// batch1_trade_history: submitted → completed
		con.Query("INSERT INTO batch1_trade_history VALUES('" + tid + "'," + ts + ",'SBMT')");
		con.Query("INSERT INTO batch1_trade_history VALUES('" + tid + "'," + ts2 + ",'CMPT')");

		// holding history for even-indexed trades
		if (t % 2 == 0) {
			con.Query("INSERT INTO batch1_holding_history VALUES('" + tid + "','" + tid + "',0," +
			          std::to_string(qty) + ")");
		}

		// watch history: activate + cancel for every 3rd trade
		long long watcher = 2000 + ((t - 1) % n_cust) + 1;
		con.Query("INSERT INTO batch1_watch_history VALUES(" + std::to_string(watcher) + ",'" + sym +
		          "'," + ts + ",'ACTV')");
		if (t % 3 == 0) {
			con.Query("INSERT INTO batch1_watch_history VALUES(" + std::to_string(watcher) + ",'" + sym +
			          "',TIMESTAMP '2023-06-01 10:00:00','CNCL')");
		}
	}

	// ── Daily market: 3 days × n_sec symbols ─────────────────────────────────
	for (int d = 0; d < 3; d++) {
		std::string date = std::string("2023-01-") + (d + 2 < 10 ? "0" : "") + std::to_string(d + 2);
		for (int s = 1; s <= n_sec; s++) {
			std::string sym = std::string("SYM") + (s < 10 ? "00" : s < 100 ? "0" : "") + std::to_string(s);
			double close = 10.0 + s * 2.5 + d * 0.1;
			con.Query("INSERT INTO batch1_daily_market VALUES(DATE '" + date + "','" + sym + "'," +
			          std::to_string(close) + "," + std::to_string(close * 1.02) + "," +
			          std::to_string(close * 0.98) + ",100000)");
		}
	}
	// batch2/3 market: 2 days × first 2 symbols
	for (int d = 0; d < 2; d++) {
		std::string date = std::string("2023-02-") + (d + 1 < 10 ? "0" : "") + std::to_string(d + 1);
		for (int s = 1; s <= std::min(n_sec, 2); s++) {
			std::string sym = std::string("SYM") + (s < 10 ? "00" : s < 100 ? "0" : "") + std::to_string(s);
			double close = 10.0 + s * 2.5 + d * 0.2;
			for (const char *tbl : {"batch2_daily_market", "batch3_daily_market"})
				con.Query(std::string("INSERT INTO ") + tbl + " VALUES(DATE '" + date + "','" + sym +
				          "'," + std::to_string(close) + "," + std::to_string(close * 1.02) + "," +
				          std::to_string(close * 0.98) + ",200000)");
		}
	}

	// ── Cash transactions ─────────────────────────────────────────────────────
	int n_ct = n * 5;
	for (int c = 1; c <= n_ct; c++) {
		long long aid = 3000 + ((c - 1) % n_acct) + 1;
		double amt = 100.0 + c * 10.0;
		for (const char *tbl : {"batch1_cash_transaction", "batch2_cash_transaction",
		                        "batch3_cash_transaction"})
			con.Query(std::string("INSERT INTO ") + tbl + " VALUES(" + std::to_string(aid) +
			          ",TIMESTAMP '2023-01-15 10:00:00'," + std::to_string(amt) +
			          ",'Settlement " + std::to_string(c) + "')");
	}

	// ── Materialise bronze output tables (simple UNION ALL) ───────────────────

	Exec(con, "CREATE TABLE brokerage_cash_transaction AS "
	          "SELECT ct_ca_id,ct_dts,ct_amt,ct_name FROM batch1_cash_transaction "
	          "UNION ALL SELECT ct_ca_id,ct_dts,ct_amt,ct_name FROM batch2_cash_transaction "
	          "UNION ALL SELECT ct_ca_id,ct_dts,ct_amt,ct_name FROM batch3_cash_transaction");

	Exec(con, "CREATE TABLE brokerage_daily_market AS "
	          "SELECT dm_date,dm_s_symb,dm_close,dm_high,dm_low,dm_vol FROM batch1_daily_market "
	          "UNION ALL SELECT dm_date,dm_s_symb,dm_close,dm_high,dm_low,dm_vol FROM batch2_daily_market "
	          "UNION ALL SELECT dm_date,dm_s_symb,dm_close,dm_high,dm_low,dm_vol FROM batch3_daily_market");

	Exec(con, "CREATE TABLE brokerage_holding_history AS "
	          "SELECT hh_h_t_id,hh_t_id,hh_before_qty,hh_after_qty FROM batch1_holding_history "
	          "UNION ALL SELECT hh_h_t_id,hh_t_id,hh_before_qty,hh_after_qty FROM batch2_holding_history "
	          "UNION ALL SELECT hh_h_t_id,hh_t_id,hh_before_qty,hh_after_qty FROM batch3_holding_history");

	Exec(con, "CREATE TABLE brokerage_trade AS "
	          "SELECT t_id,t_dts,t_st_id,t_tt_id,t_is_cash,t_s_symb,t_qty,"
	          "t_bid_price,t_ca_id,t_exec_name,t_trade_price,t_chrg,t_comm,t_tax "
	          "FROM batch1_trade "
	          "UNION ALL SELECT t_id,t_dts,t_st_id,t_tt_id,t_is_cash,t_s_symb,t_qty,"
	          "t_bid_price,t_ca_id,t_exec_name,t_trade_price,t_chrg,t_comm,t_tax "
	          "FROM batch2_trade "
	          "UNION ALL SELECT t_id,t_dts,t_st_id,t_tt_id,t_is_cash,t_s_symb,t_qty,"
	          "t_bid_price,t_ca_id,t_exec_name,t_trade_price,t_chrg,t_comm,t_tax "
	          "FROM batch3_trade");

	Exec(con, "CREATE TABLE brokerage_trade_history AS SELECT * FROM batch1_trade_history");

	Exec(con, "CREATE TABLE brokerage_watch_history AS "
	          "SELECT w_c_id,w_s_symb,w_dts,w_action FROM batch1_watch_history "
	          "UNION ALL SELECT w_c_id,w_s_symb,w_dts,w_action FROM batch2_watch_history "
	          "UNION ALL SELECT w_c_id,w_s_symb,w_dts,w_action FROM batch3_watch_history");

	Exec(con, "CREATE TABLE hr_employee AS SELECT * FROM batch1_hr");

	Exec(con, "CREATE TABLE reference_date AS "
	          "SELECT sk_dateid AS sk_date_id, datevalue AS date_value, datedesc AS date_desc,"
	          " calendaryearid AS calendar_year_id, calendaryeardesc AS calendar_year_desc,"
	          " calendarqtrid AS calendar_qtr_id, calendarqtrdesc AS calendar_qtr_desc,"
	          " calendarmonthid AS calendar_month_id, calendarmonthdesc AS calendar_month_desc,"
	          " calendarweekid AS calendar_week_id, calendarweekdesc AS calendar_week_desc,"
	          " dayofweeknum AS day_of_week_num, dayofweekdesc AS day_of_week_desc,"
	          " fiscalyearid AS fiscal_year_id, fiscalyeardesc AS fiscal_year_desc,"
	          " fiscalqtrid AS fiscal_qtr_id, fiscalqtrdesc AS fiscal_qtr_desc,"
	          " holidayflag AS holiday_flag FROM batch1_date");

	Exec(con, "CREATE TABLE reference_industry AS SELECT * FROM batch1_industry");
	Exec(con, "CREATE TABLE reference_status_type AS SELECT * FROM batch1_status_type");
	Exec(con, "CREATE TABLE reference_tax_rate AS SELECT * FROM batch1_tax_rate");
	Exec(con, "CREATE TABLE reference_trade_type AS SELECT * FROM batch1_trade_type");

	Exec(con,
	     "CREATE TABLE syndicated_prospect AS "
	     "SELECT agencyid AS agency_id, lastname AS last_name, firstname AS first_name,"
	     " middleinitial AS middle_initial, gender,"
	     " addressline1 AS address_line1, addressline2 AS address_line2, postalcode AS postal_code,"
	     " city, state, country, phone, income,"
	     " numbercars AS number_cars, numberchildren AS number_children,"
	     " maritalstatus AS marital_status, age, creditrating AS credit_rating,"
	     " ownorrentflag AS own_or_rent_flag, employer,"
	     " numbercreditcards AS number_credit_cards, networth AS net_worth "
	     "FROM batch1_prospect "
	     "UNION ALL "
	     "SELECT agencyid,lastname,firstname,middleinitial,gender,"
	     "addressline1,addressline2,postalcode,city,state,country,phone,income,"
	     "numbercars,numberchildren,maritalstatus,age,creditrating,"
	     "ownorrentflag,employer,numbercreditcards,networth FROM batch2_prospect "
	     "UNION ALL "
	     "SELECT agencyid,lastname,firstname,middleinitial,gender,"
	     "addressline1,addressline2,postalcode,city,state,country,phone,income,"
	     "numbercars,numberchildren,maritalstatus,age,creditrating,"
	     "ownorrentflag,employer,numbercreditcards,networth FROM batch3_prospect");

	// ── Materialise silver tables (topological order) ─────────────────────────

	// employees
	Exec(con, "CREATE TABLE employees AS "
	          "SELECT CAST(employeeid AS VARCHAR) AS employee_id, managerid AS manager_id,"
	          " employeefirstname AS first_name, employeelastname AS last_name,"
	          " employeemi AS middle_initial, employeejobcode AS job_code,"
	          " employeebranch AS branch, employeeoffice AS office,"
	          " employeephone AS phone FROM hr_employee");

	// date
	Exec(con, "CREATE TABLE date AS SELECT * FROM reference_date");

	// companies (SCD2 with window — single version per CIK → end='9999-12-31')
	Exec(con,
	     "CREATE TABLE companies AS "
	     "SELECT fc.cik AS company_id, st.st_name AS status, fc.company_name AS name,"
	     " ind.in_name AS industry, fc.ceo_name AS ceo,"
	     " fc.address_line1, fc.address_line2, fc.postal_code, fc.city,"
	     " fc.state_province, fc.country, fc.description, fc.founding_date, fc.sp_rating,"
	     " fc.pts AS effective_timestamp,"
	     " COALESCE("
	     "  LAG(fc.pts) OVER (PARTITION BY fc.cik ORDER BY fc.pts DESC)"
	     "  - INTERVAL 1 MILLISECOND,"
	     "  TIMESTAMP '9999-12-31 23:59:59.999') AS end_timestamp,"
	     " CASE WHEN ROW_NUMBER() OVER (PARTITION BY fc.cik ORDER BY fc.pts DESC) = 1"
	     "  THEN true ELSE false END AS is_current "
	     "FROM finwire_company fc "
	     "JOIN reference_status_type st ON fc.status = st.st_id "
	     "JOIN reference_industry ind ON fc.industry_id = ind.in_id");

	// accounts (SCD2 from crm_customer_mgmt where ca_id IS NOT NULL)
	Exec(con,
	     "CREATE TABLE accounts AS "
	     "SELECT c.action_type,"
	     " CASE c.action_type"
	     "  WHEN 'NEW' THEN 'Active' WHEN 'ADDACCT' THEN 'Active'"
	     "  WHEN 'UPDACCT' THEN 'Active' WHEN 'CLOSEACCT' THEN 'Inactive'"
	     " END AS status,"
	     " c.ca_id AS account_id, c.ca_name AS account_desc,"
	     " c.c_id AS customer_id, c.c_tax_id AS tax_id, c.c_gndr AS gender,"
	     " c.c_tier AS tier, c.c_dob AS dob,"
	     " c.c_l_name AS last_name, c.c_f_name AS first_name, c.c_m_name AS middle_name,"
	     " c.c_adline1 AS address_line1, c.c_adline2 AS address_line2,"
	     " c.c_zipcode AS postal_code, c.c_city AS city,"
	     " c.c_state_prov AS state_province, c.c_ctry AS country,"
	     " c.c_prim_email AS primary_email, c.c_alt_email AS alternate_email,"
	     " c.c_phone_1 AS phone1, c.c_phone_2 AS phone2, c.c_phone_3 AS phone3,"
	     " c.c_lcl_tx_id AS local_tax_rate_name, ltx.tx_rate AS local_tax_rate,"
	     " c.c_nat_tx_id AS national_tax_rate_name, ntx.tx_rate AS national_tax_rate,"
	     " c.ca_tax_st AS tax_status, c.ca_b_id AS broker_id,"
	     " c.action_ts AS effective_timestamp,"
	     " COALESCE("
	     "  LAG(c.action_ts) OVER (PARTITION BY c.ca_id ORDER BY c.action_ts DESC)"
	     "  - INTERVAL 1 MILLISECOND,"
	     "  TIMESTAMP '9999-12-31 23:59:59.999') AS end_timestamp,"
	     " CASE WHEN ROW_NUMBER() OVER (PARTITION BY c.ca_id ORDER BY c.action_ts DESC) = 1"
	     "  THEN true ELSE false END AS is_current "
	     "FROM crm_customer_mgmt c "
	     "LEFT JOIN reference_tax_rate ntx ON c.c_nat_tx_id = ntx.tx_id "
	     "LEFT JOIN reference_tax_rate ltx ON c.c_lcl_tx_id = ltx.tx_id "
	     "WHERE c.ca_id IS NOT NULL");

	// customers (SCD2 from crm_customer_mgmt, action_type IN NEW/INACT/UPDCUST)
	Exec(con,
	     "CREATE TABLE customers AS "
	     "SELECT c.action_type,"
	     " CASE c.action_type"
	     "  WHEN 'NEW' THEN 'Active' WHEN 'ADDACCT' THEN 'Active'"
	     "  WHEN 'UPDACCT' THEN 'Active' WHEN 'UPDCUST' THEN 'Active'"
	     "  WHEN 'INACT' THEN 'Inactive'"
	     " END AS status,"
	     " c.c_id AS customer_id, c.ca_id AS account_id, c.c_tax_id AS tax_id,"
	     " c.c_gndr AS gender, c.c_tier AS tier, c.c_dob AS dob,"
	     " c.c_l_name AS last_name, c.c_f_name AS first_name, c.c_m_name AS middle_name,"
	     " c.c_adline1 AS address_line1, c.c_adline2 AS address_line2,"
	     " c.c_zipcode AS postal_code, c.c_city AS city,"
	     " c.c_state_prov AS state_province, c.c_ctry AS country,"
	     " c.c_prim_email AS primary_email, c.c_alt_email AS alternate_email,"
	     " c.c_phone_1 AS phone1, c.c_phone_2 AS phone2, c.c_phone_3 AS phone3,"
	     " c.c_lcl_tx_id AS local_tax_rate_name, ltx.tx_rate AS local_tax_rate,"
	     " c.c_nat_tx_id AS national_tax_rate_name, ntx.tx_rate AS national_tax_rate,"
	     " c.ca_tax_st AS account_tax_status, c.ca_b_id AS broker_id,"
	     " c.action_ts AS effective_timestamp,"
	     " COALESCE("
	     "  LAG(c.action_ts) OVER (PARTITION BY c.c_id ORDER BY c.action_ts DESC)"
	     "  - INTERVAL 1 MILLISECOND,"
	     "  TIMESTAMP '9999-12-31 23:59:59.999') AS end_timestamp,"
	     " CASE WHEN ROW_NUMBER() OVER (PARTITION BY c.c_id ORDER BY c.action_ts DESC) = 1"
	     "  THEN true ELSE false END AS is_current "
	     "FROM crm_customer_mgmt c "
	     "LEFT JOIN reference_tax_rate ntx ON c.c_nat_tx_id = ntx.tx_id "
	     "LEFT JOIN reference_tax_rate ltx ON c.c_lcl_tx_id = ltx.tx_id "
	     "WHERE c.action_type IN ('NEW','INACT','UPDCUST')");

	// securities (SCD2 from finwire_security + companies)
	Exec(con,
	     "CREATE TABLE securities AS "
	     "SELECT fs.symbol, fs.issue_type,"
	     " CASE fs.status WHEN 'ACTV' THEN 'Active' WHEN 'INAC' THEN 'Inactive' ELSE NULL END AS status,"
	     " fs.name, fs.ex_id AS exchange_id, fs.sh_out AS shares_outstanding,"
	     " fs.first_trade_date, fs.first_exchange_date, fs.dividend,"
	     " COALESCE(c1.name, c2.name) AS company_name,"
	     " COALESCE(c1.company_id, c2.company_id) AS company_id,"
	     " fs.pts AS effective_timestamp,"
	     " COALESCE("
	     "  LAG(fs.pts) OVER (PARTITION BY fs.symbol ORDER BY fs.pts DESC)"
	     "  - INTERVAL 1 MILLISECOND,"
	     "  TIMESTAMP '9999-12-31 23:59:59.999') AS end_timestamp,"
	     " CASE WHEN ROW_NUMBER() OVER (PARTITION BY fs.symbol ORDER BY fs.pts DESC) = 1"
	     "  THEN true ELSE false END AS is_current "
	     "FROM finwire_security fs "
	     "LEFT JOIN companies c1"
	     "  ON CAST(fs.cik AS VARCHAR) = CAST(c1.company_id AS VARCHAR)"
	     "  AND fs.pts BETWEEN c1.effective_timestamp AND c1.end_timestamp "
	     "LEFT JOIN companies c2"
	     "  ON fs.company_name = c2.name"
	     "  AND fs.pts BETWEEN c2.effective_timestamp AND c2.end_timestamp");

	// financials (SCD2 from finwire_financial + companies)
	Exec(con,
	     "CREATE TABLE financials AS "
	     "WITH s1 AS ("
	     "SELECT ff.year, ff.quarter, ff.quarter_start_date, ff.posting_date,"
	     " ff.revenue, ff.earnings, ff.eps, ff.diluted_eps, ff.margin,"
	     " ff.inventory, ff.assets, ff.liabilities, ff.sh_out, ff.diluted_sh_out,"
	     " COALESCE(c1.name, c2.name) AS company_name,"
	     " COALESCE(c1.company_id, c2.company_id) AS company_id,"
	     " ff.pts AS effective_timestamp "
	     "FROM finwire_financial ff "
	     "LEFT JOIN companies c1"
	     "  ON CAST(ff.cik AS VARCHAR) = CAST(c1.company_id AS VARCHAR)"
	     "  AND ff.pts BETWEEN c1.effective_timestamp AND c1.end_timestamp "
	     "LEFT JOIN companies c2"
	     "  ON ff.company_name = c2.name"
	     "  AND ff.pts BETWEEN c2.effective_timestamp AND c2.end_timestamp)"
	     "SELECT *,"
	     " COALESCE("
	     "  LAG(effective_timestamp) OVER (PARTITION BY company_id ORDER BY effective_timestamp DESC)"
	     "  - INTERVAL 1 MILLISECOND,"
	     "  TIMESTAMP '9999-12-31 23:59:59.999') AS end_timestamp,"
	     " CASE WHEN ROW_NUMBER() OVER (PARTITION BY company_id ORDER BY effective_timestamp DESC) = 1"
	     "  THEN true ELSE false END AS is_current "
	     "FROM s1");

	// trades_history (SCD2 — joins brokerage_trade + brokerage_trade_history + reference tables)
	Exec(con,
	     "CREATE TABLE trades_history AS "
	     "SELECT t.t_id AS trade_id, th.th_dts AS trade_timestamp,"
	     " t.t_ca_id AS account_id, ts.st_name AS trade_status,"
	     " tt.tt_name AS trade_type,"
	     " CASE t.t_is_cash WHEN true THEN 'Cash' WHEN false THEN 'Margin' END AS transaction_type,"
	     " t.t_s_symb AS symbol, t.t_exec_name AS executor_name,"
	     " t.t_qty AS quantity, t.t_bid_price AS bid_price,"
	     " t.t_trade_price AS trade_price, t.t_chrg AS fee,"
	     " t.t_comm AS commission, t.t_tax AS tax,"
	     " us.st_name AS update_status,"
	     " th.th_dts AS effective_timestamp,"
	     " COALESCE("
	     "  LAG(th.th_dts) OVER (PARTITION BY t.t_id ORDER BY th.th_dts DESC)"
	     "  - INTERVAL 1 MILLISECOND,"
	     "  TIMESTAMP '9999-12-31 23:59:59.999') AS end_timestamp,"
	     " CASE WHEN ROW_NUMBER() OVER (PARTITION BY t.t_id ORDER BY th.th_dts DESC) = 1"
	     "  THEN true ELSE false END AS is_current "
	     "FROM brokerage_trade t "
	     "JOIN brokerage_trade_history th ON t.t_id = th.th_t_id "
	     "JOIN reference_trade_type tt ON t.t_tt_id = tt.tt_id "
	     "JOIN reference_status_type ts ON t.t_st_id = ts.st_id "
	     "JOIN reference_status_type us ON th.th_st_id = us.st_id");

	// trades (MIN/MAX window over trades_history)
	Exec(con,
	     "CREATE TABLE trades AS "
	     "SELECT DISTINCT trade_id, account_id, trade_status, trade_type, transaction_type,"
	     " symbol, executor_name, quantity, bid_price, trade_price, fee, commission, tax,"
	     " MIN(effective_timestamp) OVER (PARTITION BY trade_id) AS create_timestamp,"
	     " MAX(effective_timestamp) OVER (PARTITION BY trade_id) AS close_timestamp "
	     "FROM trades_history");

	// daily_market — simplified (no 52-week self-join; reuse same date for high/low dates)
	Exec(con,
	     "CREATE TABLE daily_market AS "
	     "SELECT dm_date, dm_s_symb, dm_close, dm_high, dm_low, dm_vol,"
	     " dm_low AS fifty_two_week_low, dm_high AS fifty_two_week_high,"
	     " dm_date AS fifty_two_week_low_date, dm_date AS fifty_two_week_high_date "
	     "FROM brokerage_daily_market");

	// cash_transactions
	Exec(con,
	     "CREATE TABLE cash_transactions AS "
	     "WITH t AS ("
	     " SELECT ct_ca_id AS account_id, ct_dts AS transaction_timestamp,"
	     "  ct_amt AS amount, ct_name AS description"
	     " FROM brokerage_cash_transaction) "
	     "SELECT a.customer_id, t.* "
	     "FROM t "
	     "JOIN accounts a"
	     " ON t.account_id = a.account_id"
	     " AND t.transaction_timestamp BETWEEN a.effective_timestamp AND a.end_timestamp");

	// holdings_history
	Exec(con,
	     "CREATE TABLE holdings_history AS "
	     "WITH s1 AS ("
	     " SELECT hh_t_id AS trade_id, hh_h_t_id AS previous_trade_id,"
	     "  hh_before_qty AS previous_quantity, hh_after_qty AS quantity"
	     " FROM brokerage_holding_history) "
	     "SELECT s1.*, ct.account_id, ct.symbol, ct.create_timestamp, ct.close_timestamp,"
	     " ct.trade_price, ct.bid_price, ct.fee, ct.commission "
	     "FROM s1 "
	     "JOIN trades ct USING (trade_id)");

	// watches_history
	Exec(con,
	     "CREATE TABLE watches_history AS "
	     "WITH s1 AS ("
	     " SELECT w_c_id AS customer_id, w_s_symb AS symbol, w_dts AS watch_timestamp,"
	     "  CASE w_action WHEN 'ACTV' THEN 'Activate' WHEN 'CNCL' THEN 'Cancelled'"
	     "   ELSE NULL END AS action_type"
	     " FROM brokerage_watch_history) "
	     "SELECT s1.*, s.company_id, s.company_name, s.exchange_id, s.status AS security_status "
	     "FROM s1 "
	     "JOIN securities s USING (symbol)");

	// watches
	Exec(con,
	     "CREATE TABLE watches AS "
	     "WITH s1 AS ("
	     " SELECT customer_id, symbol, watch_timestamp, action_type,"
	     "  company_id, company_name, exchange_id, security_status,"
	     "  CASE action_type WHEN 'Activate' THEN watch_timestamp ELSE NULL END AS placed_timestamp,"
	     "  CASE action_type WHEN 'Cancelled' THEN watch_timestamp ELSE NULL END AS removed_timestamp"
	     " FROM watches_history),"
	     "s2 AS ("
	     " SELECT customer_id, symbol, company_id, company_name, exchange_id, security_status,"
	     "  MIN(placed_timestamp) AS placed_timestamp, MAX(removed_timestamp) AS removed_timestamp"
	     " FROM s1"
	     " GROUP BY customer_id, symbol, company_id, company_name, exchange_id, security_status) "
	     "SELECT *,"
	     " CASE WHEN removed_timestamp IS NULL THEN 'Active' ELSE 'Inactive' END AS watch_status "
	     "FROM s2");

	// ── Materialise gold / fact tables ────────────────────────────────────────

	// dim_broker
	Exec(con,
	     "CREATE TABLE dim_broker AS "
	     "SELECT md5(employee_id) AS sk_broker_id,"
	     " employee_id AS broker_id, manager_id, first_name, last_name,"
	     " middle_initial, job_code, branch, office, phone "
	     "FROM employees");

	// dim_date
	Exec(con, "CREATE TABLE dim_date AS SELECT * FROM date");

	// dim_company
	Exec(con,
	     "CREATE TABLE dim_company AS "
	     "SELECT md5(CAST(company_id AS VARCHAR) || '|' || CAST(effective_timestamp AS VARCHAR))"
	     "  AS sk_company_id,"
	     " company_id, status, name, industry, ceo,"
	     " address_line1, address_line2, postal_code, city, state_province, country,"
	     " description, founding_date, sp_rating,"
	     " CASE WHEN sp_rating IN ('BB','B','CCC','CC','C','D','BB+','B+','CCC+','BB-','B-','CCC-')"
	     "  THEN true ELSE false END AS is_lowgrade,"
	     " effective_timestamp, end_timestamp, is_current "
	     "FROM companies");

	// dim_customer
	Exec(con,
	     "CREATE TABLE dim_customer AS "
	     "WITH s1 AS ("
	     " SELECT c.*,"
	     "  p.agency_id, p.credit_rating, p.net_worth"
	     " FROM customers c"
	     " LEFT JOIN syndicated_prospect p"
	     "  ON c.first_name = p.first_name AND c.last_name = p.last_name"
	     "  AND c.postal_code = p.postal_code AND c.address_line1 = p.address_line1"
	     "  AND COALESCE(c.address_line2,'') = COALESCE(p.address_line2,''))"
	     "SELECT"
	     " md5(CAST(customer_id AS VARCHAR) || '|' || CAST(effective_timestamp AS VARCHAR))"
	     "  AS sk_customer_id,"
	     " customer_id, tax_id, status, last_name, first_name, middle_name AS middleinitial,"
	     " gender, tier, dob, address_line1, address_line2, postal_code, city,"
	     " state_province, country, phone1, phone2, phone3,"
	     " primary_email, alternate_email,"
	     " local_tax_rate_name, local_tax_rate, national_tax_rate_name, national_tax_rate,"
	     " agency_id, credit_rating, net_worth,"
	     " effective_timestamp, end_timestamp, is_current "
	     "FROM s1");

	// dim_security (BETWEEN temporal join on companies)
	Exec(con,
	     "CREATE TABLE dim_security AS "
	     "SELECT md5(s.symbol || '|' || CAST(s.effective_timestamp AS VARCHAR)) AS sk_security_id,"
	     " s.symbol, s.issue_type AS issue, s.status, s.name, s.exchange_id,"
	     " dc.sk_company_id, s.shares_outstanding, s.first_trade_date,"
	     " s.first_exchange_date, s.dividend,"
	     " s.effective_timestamp, s.end_timestamp, s.is_current "
	     "FROM securities s "
	     "JOIN dim_company dc"
	     "  ON s.company_id = dc.company_id"
	     "  AND s.effective_timestamp BETWEEN dc.effective_timestamp AND dc.end_timestamp");

	// dim_account (BETWEEN temporal join)
	Exec(con,
	     "CREATE TABLE dim_account AS "
	     "SELECT md5(CAST(a.account_id AS VARCHAR) || '|' || CAST(a.effective_timestamp AS VARCHAR))"
	     "  AS sk_account_id,"
	     " a.account_id, db.sk_broker_id, dc.sk_customer_id,"
	     " a.status, a.account_desc, a.tax_status,"
	     " a.effective_timestamp, a.end_timestamp, a.is_current "
	     "FROM accounts a "
	     "JOIN dim_customer dc"
	     "  ON a.customer_id = dc.customer_id"
	     "  AND a.effective_timestamp BETWEEN dc.effective_timestamp AND dc.end_timestamp "
	     "JOIN dim_broker db"
	     "  ON a.broker_id = db.broker_id");

	// dim_trade
	Exec(con,
	     "CREATE TABLE dim_trade AS "
	     "SELECT md5(trade_id || '|' || CAST(effective_timestamp AS VARCHAR)) AS sk_trade_id,"
	     " trade_id, trade_status AS status, transaction_type, trade_type AS type,"
	     " executor_name AS executed_by, effective_timestamp, end_timestamp, is_current "
	     "FROM trades_history");

	// wrk_company_financials
	Exec(con,
	     "CREATE TABLE wrk_company_financials AS "
	     "SELECT f.company_id, dc.sk_company_id, f.eps, f.revenue,"
	     " f.effective_timestamp, f.end_timestamp, f.is_current "
	     "FROM financials f "
	     "JOIN dim_company dc"
	     "  ON f.company_id = dc.company_id"
	     "  AND f.effective_timestamp BETWEEN dc.effective_timestamp AND dc.end_timestamp");

	// fact_cash_transactions (BETWEEN on dim_account)
	Exec(con,
	     "CREATE TABLE fact_cash_transactions AS "
	     "WITH s1 AS ("
	     " SELECT *, CAST(transaction_timestamp AS DATE) AS sk_transaction_date"
	     " FROM cash_transactions) "
	     "SELECT a.sk_customer_id, a.sk_account_id,"
	     " s1.sk_transaction_date, s1.transaction_timestamp, s1.amount, s1.description "
	     "FROM s1 "
	     "JOIN dim_account a"
	     "  ON s1.account_id = a.account_id"
	     "  AND s1.transaction_timestamp BETWEEN a.effective_timestamp AND a.end_timestamp");

	// fact_cash_balances
	Exec(con,
	     "CREATE TABLE fact_cash_balances AS "
	     "WITH s1 AS (SELECT * FROM fact_cash_transactions) "
	     "SELECT sk_customer_id, sk_account_id, sk_transaction_date,"
	     " SUM(amount) AS amount, description "
	     "FROM s1 "
	     "GROUP BY sk_customer_id, sk_account_id, sk_transaction_date, description");

	// fact_trade (BETWEEN on dim_trade, dim_account, dim_security)
	Exec(con,
	     "CREATE TABLE fact_trade AS "
	     "SELECT dt.sk_trade_id, db.sk_broker_id, da.sk_customer_id, da.sk_account_id,"
	     " ds.sk_security_id,"
	     " CAST(t.create_timestamp AS DATE) AS sk_create_date, t.create_timestamp,"
	     " CAST(t.close_timestamp AS DATE) AS sk_close_date, t.close_timestamp,"
	     " t.executor_name AS executed_by, t.quantity, t.bid_price, t.trade_price,"
	     " t.fee, t.commission, t.tax "
	     "FROM trades t "
	     "JOIN dim_trade dt"
	     "  ON t.trade_id = dt.trade_id"
	     "  AND t.create_timestamp BETWEEN dt.effective_timestamp AND dt.end_timestamp "
	     "JOIN dim_account da"
	     "  ON t.account_id = da.account_id"
	     "  AND t.create_timestamp BETWEEN da.effective_timestamp AND da.end_timestamp "
	     "JOIN dim_security ds"
	     "  ON t.symbol = ds.symbol"
	     "  AND t.create_timestamp BETWEEN ds.effective_timestamp AND ds.end_timestamp "
	     "JOIN dim_broker db ON da.sk_broker_id = db.sk_broker_id");

	// fact_holdings
	Exec(con,
	     "CREATE TABLE fact_holdings AS "
	     "WITH s1 AS (SELECT * FROM holdings_history) "
	     "SELECT ct.sk_trade_id AS sk_current_trade_id, pt.sk_trade_id,"
	     " da.sk_customer_id, da.sk_account_id, ds.sk_security_id,"
	     " CAST(s1.create_timestamp AS DATE) AS sk_trade_date, s1.create_timestamp AS trade_timestamp,"
	     " s1.trade_price AS current_price, s1.quantity AS current_holding,"
	     " s1.bid_price AS current_bid_price, s1.fee AS current_fee, s1.commission AS current_commission "
	     "FROM s1 "
	     "JOIN dim_trade ct USING (trade_id) "
	     "JOIN dim_trade pt ON s1.previous_trade_id = pt.trade_id "
	     "JOIN dim_account da"
	     "  ON s1.account_id = da.account_id"
	     "  AND s1.create_timestamp BETWEEN da.effective_timestamp AND da.end_timestamp "
	     "JOIN dim_security ds ON s1.symbol = ds.symbol");

	// fact_market_history
	Exec(con,
	     "CREATE TABLE fact_market_history AS "
	     "SELECT s.sk_security_id, s.sk_company_id, dmh.dm_date AS sk_date_id,"
	     " (s.dividend / dmh.dm_close) / 100 AS yield,"
	     " fifty_two_week_high, fifty_two_week_high_date AS sk_fifty_two_week_high_date,"
	     " fifty_two_week_low, fifty_two_week_low_date AS sk_fifty_two_week_low_date,"
	     " dm_close AS closeprice, dm_high AS dayhigh, dm_low AS daylow, dm_vol AS volume "
	     "FROM daily_market dmh "
	     "JOIN dim_security s"
	     "  ON s.symbol = dmh.dm_s_symb"
	     "  AND dmh.dm_date BETWEEN s.effective_timestamp AND s.end_timestamp "
	     "LEFT JOIN wrk_company_financials f USING (sk_company_id)");

	// fact_watches
	Exec(con,
	     "CREATE TABLE fact_watches AS "
	     "SELECT dc.sk_customer_id, ds.sk_security_id,"
	     " CAST(w.placed_timestamp AS DATE) AS sk_date_placed,"
	     " CAST(w.removed_timestamp AS DATE) AS sk_date_removed,"
	     " 1 AS watch_cnt "
	     "FROM watches w "
	     "JOIN dim_customer dc"
	     "  ON w.customer_id = dc.customer_id"
	     "  AND w.placed_timestamp BETWEEN dc.effective_timestamp AND dc.end_timestamp "
	     "JOIN dim_security ds"
	     "  ON w.symbol = ds.symbol"
	     "  AND w.placed_timestamp BETWEEN ds.effective_timestamp AND ds.end_timestamp");
}

// ──────────────────────────────────────────────────────────────────────────────
// GenerateTPCDIDeltaPool
// Returns INSERT/UPDATE SQL strings for source tables at every layer of the DAG
// so that any query in the benchmark suite sees at least one incoming delta.
// ──────────────────────────────────────────────────────────────────────────────
std::vector<std::string> GenerateTPCDIDeltaPool(int scale_factor) {
	std::vector<std::string> deltas;
	const int n = scale_factor;
	const int n_sec = n * 5;
	const int n_acct = n * 3;
	const int n_cust = n * 3;
	const int n_co = n * 3;
	const int n_emp = n * 3;
	const int base_trade = n * 10; // existing trade count

	// ── Bronze layer: insert into raw batch tables ─────────────────────────────
	// New trades in batch1_trade + batch1_trade_history
	for (int i = 0; i < 5; i++) {
		int sym_idx = (i % n_sec) + 1;
		long long aid = 3000 + (i % n_acct) + 1;
		std::string sym = std::string("SYM") + (sym_idx < 10 ? "00" : sym_idx < 100 ? "0" : "") + std::to_string(sym_idx);
		std::string tid = std::string("TD") + std::to_string(base_trade + 1000 + i);
		double price = 20.0 + i * 1.5;
		std::string ts = "TIMESTAMP '2023-07-" + std::to_string(1 + i) + " 10:00:00'";
		deltas.push_back("INSERT INTO batch1_trade VALUES('" + tid + "'," + ts + ",'ACTV','TMB',true,'" +
		                 sym + "',200," + std::to_string(static_cast<int>(price)) + ".0," +
		                 std::to_string(aid) + ",'DeltaExec'," +
		                 std::to_string(static_cast<int>(price * 1.01)) + ".0,5.0,2.0,1.5)");
		deltas.push_back("INSERT INTO batch1_trade_history VALUES('" + tid + "'," + ts + ",'SBMT')");
	}

	// New daily market rows in batch1_daily_market
	for (int i = 0; i < 5; i++) {
		int sym_idx = (i % n_sec) + 1;
		std::string sym = std::string("SYM") + (sym_idx < 10 ? "00" : sym_idx < 100 ? "0" : "") + std::to_string(sym_idx);
		double close = 30.0 + i * 0.5;
		deltas.push_back("INSERT INTO batch1_daily_market VALUES(DATE '2023-07-" +
		                 std::to_string(1 + i) + "','" + sym + "'," + std::to_string(close) + "," +
		                 std::to_string(close * 1.02) + "," + std::to_string(close * 0.98) + ",150000)");
	}

	// New cash transactions in batch1/2/3
	for (int i = 0; i < 3; i++) {
		long long aid = 3000 + (i % n_acct) + 1;
		deltas.push_back("INSERT INTO batch1_cash_transaction VALUES(" + std::to_string(aid) +
		                 ",TIMESTAMP '2023-07-01 11:00:00',500.0,'Delta settlement " +
		                 std::to_string(i) + "')");
	}

	// New watch history
	for (int i = 0; i < 3; i++) {
		long long watcher = 2000 + (i % n_cust) + 1;
		int sym_idx = (i % n_sec) + 1;
		std::string sym = std::string("SYM") + (sym_idx < 10 ? "00" : sym_idx < 100 ? "0" : "") + std::to_string(sym_idx);
		deltas.push_back("INSERT INTO batch1_watch_history VALUES(" + std::to_string(watcher) + ",'" +
		                 sym + "',TIMESTAMP '2023-07-01 09:00:00','ACTV')");
	}

	// ── Silver layer: insert into pre-computed intermediate tables ─────────────
	// brokerage_trade (source for trades_history, trades, holdings_history)
	for (int i = 0; i < 3; i++) {
		int sym_idx = (i % n_sec) + 1;
		long long aid = 3000 + (i % n_acct) + 1;
		std::string sym = std::string("SYM") + (sym_idx < 10 ? "00" : sym_idx < 100 ? "0" : "") + std::to_string(sym_idx);
		std::string tid = std::string("TB") + std::to_string(base_trade + 2000 + i);
		deltas.push_back("INSERT INTO brokerage_trade VALUES('" + tid +
		                 "',TIMESTAMP '2023-08-01 10:00:00','ACTV','TMB',true,'" + sym +
		                 "',300,25.0," + std::to_string(aid) + ",'BrokerExec',25.25,5.0,2.0,1.5)");
	}

	// brokerage_trade_history
	for (int i = 0; i < 3; i++) {
		std::string tid = std::string("TB") + std::to_string(base_trade + 2000 + i);
		deltas.push_back("INSERT INTO brokerage_trade_history VALUES('" + tid +
		                 "',TIMESTAMP '2023-08-01 10:00:00','SBMT')");
	}

	// brokerage_daily_market (source for daily_market)
	deltas.push_back("INSERT INTO brokerage_daily_market VALUES(DATE '2023-08-01','SYM001',28.0,28.5,27.5,120000)");

	// finwire_company (source for companies, dim_company)
	deltas.push_back("INSERT INTO finwire_company VALUES("
	                 "TIMESTAMP '2023-01-01 00:00:00','DeltaCorp','" +
	                 std::to_string(1000 + n_co + 1) + "',"
	                 "'ACTV','IND1','A+',DATE '2010-01-01','9 Delta Ave','','10001',"
	                 "'Boston','MA','US','Delta CEO','Delta Desc')");

	// finwire_security (source for securities, watches_history)
	deltas.push_back("INSERT INTO finwire_security VALUES("
	                 "TIMESTAMP '2023-01-01 00:00:00','DSYM','CS','ACTV','Delta Security Inc',"
	                 "'NYSE',500000,DATE '2015-01-01',DATE '2015-01-15',2.0," +
	                 std::to_string(1000 + n_co + 1) + ",NULL)");

	// crm_customer_mgmt (source for accounts, customers)
	long long new_cid = 2000 + n_cust + 1;
	long long new_aid = 3000 + n_acct + 1;
	deltas.push_back("INSERT INTO crm_customer_mgmt VALUES("
	                 "TIMESTAMP '2023-01-01 00:00:00','NEW'," +
	                 std::to_string(new_cid) + ",'TAXD','M',2,DATE '1990-01-01',"
	                 "'DeltaLast','DeltaFirst','M','99 Delta St','','10001','Chicago','IL','US',"
	                 "'delta@test.com','','1-555-9001','','','TX_LOCAL','TX_NAT'," +
	                 std::to_string(new_aid) + ",1,1,'Delta Account')");

	// ── Gold layer: insert into silver/computed tables ─────────────────────────
	// employees (source for dim_broker)
	deltas.push_back("INSERT INTO employees VALUES('" + std::to_string(n_emp + 1) + "'," +
	                 std::to_string(1) + ",'NewFirst','NewLast','M','Analyst','Branch2','Office2','555-9999')");

	// companies (source for dim_company, securities)
	deltas.push_back("INSERT INTO companies VALUES('DCMP','Active','NewCo','Technology','NewCEO',"
	                 "'99 New St','','10001','Boston','MA','US','New co desc',"
	                 "DATE '2010-01-01','A',"
	                 "TIMESTAMP '2023-01-01 00:00:00',TIMESTAMP '9999-12-31 23:59:59.999',true)");

	// securities (source for watches_history, dim_security)
	deltas.push_back("INSERT INTO securities VALUES('NSYM','CS','Active','NewSec Inc','NYSE',"
	                 "500000,DATE '2020-01-01',DATE '2020-01-15',1.0,'NewCo','DCMP',"
	                 "TIMESTAMP '2023-01-01 00:00:00',TIMESTAMP '9999-12-31 23:59:59.999',true)");

	// accounts (source for dim_account, cash_transactions)
	deltas.push_back("INSERT INTO accounts VALUES('NEW','Active'," + std::to_string(new_aid) + ","
	                 "'New Account'," + std::to_string(new_cid) + ",'TAXD','M',1,"
	                 "DATE '1990-01-01','DeltaLast','DeltaFirst','M','99 Delta St','','10001',"
	                 "'Chicago','IL','US','delta@test.com','','1-555-9001','','','TX_LOCAL',0.05,"
	                 "'TX_NAT',0.15,1,1,"
	                 "TIMESTAMP '2023-01-01 00:00:00',TIMESTAMP '9999-12-31 23:59:59.999',true)");

	// customers (source for dim_customer)
	deltas.push_back("INSERT INTO customers VALUES('NEW','Active'," + std::to_string(new_cid) + ","
	                 + std::to_string(new_aid) + ",'TAXD','M',1,DATE '1990-01-01',"
	                 "'DeltaLast','DeltaFirst','M','99 Delta St','','10001',"
	                 "'Chicago','IL','US','delta@test.com','','1-555-9001','','','TX_LOCAL',0.05,"
	                 "'TX_NAT',0.15,1,1,"
	                 "TIMESTAMP '2023-01-01 00:00:00',TIMESTAMP '9999-12-31 23:59:59.999',true)");

	// fact_cash_transactions (source for fact_cash_balances — schema: sk_customer_id, sk_account_id,
	// sk_transaction_date, transaction_timestamp, amount, description)
	deltas.push_back("INSERT INTO fact_cash_transactions VALUES("
	                 "'sk_cust_delta','sk_acct_delta',"
	                 "DATE '2023-07-01',TIMESTAMP '2023-07-01 12:00:00',250.0,'Delta settlement')");

	return deltas;
}

} // namespace openivm_bench
