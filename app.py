import pandas as pd
import streamlit as st
import io

def main():
    st.set_page_config(page_title="GLSource File to Subledger Journals validation", layout="wide")
    st.title("📊 DTV Subledger to EDMCS mapping validator")

    st.markdown("""
    Upload your **transaction CSV**, **mapping text files** (pipe-delimited), and **Product Mapping Excel** (sheet: `DTV_BDS_UB_PRODUCT_MAPPING`).

    Mapping logic:
    1. **DTV_Main_Account:** MAM_DB → GL + EXTC (EXTC='*' matches any CSV EXTC)
    2. **AccountType:** MA_DB → MainAccount
    3. **DTV_Sub_Account:** SAM_DB → GL + EXTC
    4. **DTV_Company:** COMPANY_DB → ATT Code
    5. **CostCenter:** CCM_DB → RCC + AccountType
    6. **Product:** VALIDATED from Excel → FIN_PRD_CD (sheet: DTV_BDS_UB_PRODUCT_MAPPING)
    """)

    ACCOUNT_TYPE_MAP = {"L": "Liability", "R": "Revenue", "O": "Other", "A": "Asset", "E": "Expense"}

# ---------- Helpers ----------
    def normalize_mapping_headers(df):  
        df.columns = df.columns.str.strip()
        col_rename = {}
        for col in df.columns:
            col_low = col.lower().replace(" ", "")
            if 'glaccount' in col_low: col_rename[col] = 'GL Account'
            elif 'etc' in col_low or 'extc' in col_low: col_rename[col] = 'EXTC'
            elif 'mainaccount' in col_low: col_rename[col] = 'MainAccount'
            elif 'accounttype' in col_low: col_rename[col] = 'AccountType'
            elif 'dtv_main_account' in col_low: col_rename[col] = 'DTV_Main_Account'
            elif 'dtv_sub_account' in col_low: col_rename[col] = 'DTV_Sub_Account'
            elif 'att_company' in col_low: col_rename[col] = 'ATT_Company'
            elif 'dtv_company' in col_low: col_rename[col] = 'DTV_Company'
            elif 'dtv_cost_center' in col_low: col_rename[col] = 'DTV_Cost_Center'
            elif 'rcc' in col_low: col_rename[col] = 'RCC'
            elif 'rco' in col_low: col_rename[col] = 'RCO'
        if col_rename:
            df.rename(columns=col_rename, inplace=True)
        return df

    def clean_str(val):
        if pd.isna(val) or val == "":
            return ""
        return str(val).strip().upper()

# ---------- Extraction ----------
    def extract_data(csv_file, mapping_files):
    # Read CSV
        df = pd.read_csv(csv_file, dtype=str, low_memory=False)
        df.columns = df.columns.str.strip()

    # positional 19 columns mapping (index -> column name)
        positional_indices = {
            0: "Record Type", 1: "Company Code", 2: "Transaction Source", 3: "Transaction Type",
            4: "Transaction Date", 5: "Accounting Period", 6: "Location Code", 7: "RCO",
            8: "RCC", 9: "GL Account (GLA)", 10: "Reference Number", 11: "Activity Code",
            12: "EXTC", 13: "Journal Categories", 14: "Amount", 15: "Product Code",
            16: "Currency", 17: "Stat", 18: "Expenditure comment"
        }

    # Pad missing columns by adding empty columns with numeric indices if needed
        for idx, col_name in positional_indices.items():
            if idx >= df.shape[1]:
                df[idx] = ""

    # Map using positional indices (safe even if columns are named differently)
        mapped_cols = {name: df.iloc[:, idx].fillna("").astype(str).str.strip() for idx, name in positional_indices.items()}
        core_df = pd.DataFrame(mapped_cols)

    # Only keep DTL rows (if Record Type exists)
        if "Record Type" in core_df.columns:
            core_df = core_df[core_df["Record Type"].str.strip() == "DTL"].copy()
        else:
            core_df["Record Type"] = "DTL"

    # Amount numeric
        core_df["Amount"] = pd.to_numeric(core_df.get("Amount", "0").replace("", 0), errors="coerce").fillna(0)

    # Load mapping text files (pipe-delimited) and normalize headers
        sam_df = mam_df = ma_df = company_df = ccm_df = None
        for f in mapping_files:
            try:
                temp_df = pd.read_csv(f, sep="|", dtype=str, low_memory=False)
            except Exception:
            # fallback attempt without sep param
                temp_df = pd.read_csv(f, dtype=str, low_memory=False)
            temp_df = normalize_mapping_headers(temp_df)
            for c in temp_df.columns:
                temp_df[c] = temp_df[c].astype(str).str.strip()
            fname = f.name.lower()
            if "sam" in fname: sam_df = temp_df
            elif "mam" in fname: mam_df = temp_df
            elif "ma" in fname: ma_df = temp_df
            elif "company" in fname: company_df = temp_df
            elif "ccm" in fname: ccm_df = temp_df
            else:
                st.info(f"Skipped mapping file (unrecognized name): {f.name}")

        if any(m is None for m in [sam_df, mam_df, ma_df, company_df, ccm_df]):
            raise ValueError("Missing one or more mapping files: sam, mam, ma, company, ccm")

        return core_df, sam_df, mam_df, ma_df, company_df, ccm_df

# ---------- Load product mapping Excel ----------
    def load_product_mapping(product_file, sheet_name="DTV_BDS_UB_PRODUCT_MAPPING"):
    # Read the specified sheet
        prod_df = pd.read_excel(product_file, sheet_name=sheet_name, dtype=str)
        prod_df.columns = prod_df.columns.str.strip()
    # enforce the expected columns exist
        if "ATT_SLS_PRD_ID" not in prod_df.columns or "FIN_PRD_CD" not in prod_df.columns:
            raise ValueError(f"Product mapping sheet must contain columns: ATT_SLS_PRD_ID, FIN_PRD_CD. Found: {list(prod_df.columns)}")
    # clean
        prod_df["ATT_SLS_PRD_ID"] = prod_df["ATT_SLS_PRD_ID"].astype(str).str.strip().str.upper()
        prod_df["FIN_PRD_CD"] = prod_df["FIN_PRD_CD"].astype(str).str.strip().str.upper()
    # Drop duplicates keeping first mapping
        prod_df = prod_df.drop_duplicates(subset=["ATT_SLS_PRD_ID"], keep="first")
        return prod_df

# ---------- Mapping helper ----------
    def map_with_wildcard_clean(df, mapping_df, df_gl_col, df_extc_col, map_col_name, default_val="000000"):
        df_gl = df[df_gl_col].apply(clean_str)
        df_extc = df[df_extc_col].apply(clean_str)
        mapping_df = mapping_df.copy()
        mapping_df["GL_CLEAN"] = mapping_df["GL Account"].apply(clean_str)
        mapping_df["EXTC_CLEAN"] = mapping_df["EXTC"].apply(lambda x: "*" if str(x).strip() == "*" else clean_str(x))
        mapped = []
        for gl_val, etc_val in zip(df_gl, df_extc):
            match = mapping_df[(mapping_df["GL_CLEAN"] == gl_val) & (mapping_df["EXTC_CLEAN"] == etc_val)]
            if match.empty:
                match = mapping_df[(mapping_df["GL_CLEAN"] == gl_val) & (mapping_df["EXTC_CLEAN"] == "*")]
            mapped.append(match.iloc[0][map_col_name] if not match.empty else default_val)
        return mapped

# ---------- Transformation ----------
    def transform_data(df, sam_df, mam_df, ma_df, company_df, ccm_df, product_df):
    # Clean & prepare keys
        df["GL_CLEAN"] = df.get("GL Account (GLA)", "").apply(clean_str)
        df["EXTC_CLEAN"] = df.get("EXTC", "").apply(clean_str)
        df["COMP_CLEAN"] = df.get("Company Code", "").apply(clean_str)
        df["RCC"] = df.get("RCC", "").astype(str).str.strip()
        df["RCO"] = df.get("RCO", "").astype(str).str.strip()

    # Map main/sub accounts
        df["DTV_Main_Account"] = map_with_wildcard_clean(df, mam_df, "GL_CLEAN", "EXTC_CLEAN", "DTV_Main_Account")
        df["DTV_Sub_Account"] = map_with_wildcard_clean(df, sam_df, "GL_CLEAN", "EXTC_CLEAN", "DTV_Sub_Account")

    # AccountType
        ma_map_full = dict(zip(ma_df["MainAccount"].apply(clean_str), ma_df["AccountType"]))
        df["AccountType_Code"] = df["DTV_Main_Account"].apply(clean_str).map(ma_map_full).fillna("UNKNOWN")
        df["AccountType"] = df["AccountType_Code"].map(ACCOUNT_TYPE_MAP).fillna(df["AccountType_Code"])

    # Company mapping
        company_map = dict(zip(company_df["ATT_Company"].apply(clean_str), company_df["DTV_Company"]))
        df["DTV_Company"] = df["COMP_CLEAN"].map(company_map).fillna("NULL")

    # Cost center mapping
        ccm_key = ccm_df["RCC"].astype(str).str.strip() + "|" + ccm_df["AccountType"].astype(str).str.strip()
        ccm_map = dict(zip(ccm_key, ccm_df["DTV_Cost_Center"]))
        cost_key = df["RCC"].astype(str).str.strip() + "|" + df["AccountType_Code"].astype(str).str.strip()
        df["DTV_Cost_Center"] = cost_key.map(ccm_map).fillna("000000")

    # ---------------- Product mapping using product_df (sheet: DTV_BDS_UB_PRODUCT_MAPPING) ----------------
    # Clean Product Code and mapping keys (case-insensitive & strip)
        df["Product Code"] = df.get("Product Code", "").astype(str).str.strip().str.upper()
        product_df = product_df = product_df = product_df  # no-op to indicate usage (keeps linter happy)

    # product_df already cleaned by loader, create mapping dict
        prod_map = dict(zip(product_df["ATT_SLS_PRD_ID"], product_df["FIN_PRD_CD"]))

    # Map to FIN_PRD_CD; do not fill with 0000 here so we can detect unmatched explicitly
        df["FIN_PRD_CD"] = df["Product Code"].map(prod_map)

    # Create final Product column (will be used in Account)
    # If FIN_PRD_CD missing/NaN -> set "0000"
        df["Product"] = df["FIN_PRD_CD"].fillna("0000").replace(["", "UNMATCHED", "MISSING"], "0000")
        df["Product"] = df["Product"].fillna("0000")

    # Debit/Credit
        df["Debit"] = df["Amount"].apply(lambda x: round(x, 2) if x > 0 else 0)
        df["Credit"] = df["Amount"].apply(lambda x: round(abs(x), 2) if x < 0 else 0)
        df["Amount"] = df["Amount"].round(2)

    # Defaults
        df["Intercompany"] = "0000"
        df["TaxJurisdiction"] = "000"
        df["Reserved1"] = "00000"
        df["Reserved2"] = "00000"

    # Build Account using final Product (mapped FIN_PRD_CD or "0000")
        df["Account"] = (
            df["DTV_Company"] + "*" +
            df["DTV_Main_Account"] + "*" +
            df["DTV_Sub_Account"] + "*" +
            df["DTV_Cost_Center"] + "*" +
            df["Intercompany"] + "*" +
            df["Product"] + "*" +
            df["TaxJurisdiction"] + "*" +
            df["Reserved1"] + "*" +
            df["Reserved2"]
        )

    # Unmatched: main/sub/company or product mapping missing
        unmatched = df[
            (df["DTV_Main_Account"] == "000000") |
            (df["DTV_Sub_Account"] == "000000") |
            (df["DTV_Company"] == "NULL") |
            (df["FIN_PRD_CD"].isna())  # product mapping didn't find
        ].copy()

    # Summary aggregation
        summary = df.groupby(
            ["DTV_Company", "DTV_Main_Account", "AccountType", "DTV_Sub_Account", "DTV_Cost_Center", "FIN_PRD_CD"],
            dropna=False, as_index=False
            ).agg({"Debit": "sum", "Credit": "sum"})

        summary["Debit"] = summary["Debit"].round(2)
        summary["Credit"] = summary["Credit"].round(2)

        summary["Account"] = (
            summary["DTV_Company"] + "*" +
            summary["DTV_Main_Account"] + "*" +
            summary["DTV_Sub_Account"] + "*" +
            summary["AccountType"] + "*" +
            summary["DTV_Cost_Center"] + "*0000*" +
            summary["FIN_PRD_CD"] + "*000*00000*00000"
        )


        return df, summary, unmatched

# ---------- Prepare rows for grouped summary download ----------
    def prepare_debit_credit_rows(summary):
        rows = []
        for _, row in summary.iterrows():
            base = row.to_dict()
            if row["Debit"] != 0:
                r = base.copy(); r["Debit"] = f"{row['Debit']:.2f}"; r["Credit"] = ""
                rows.append(r)
            if row["Credit"] != 0:
                r = base.copy(); r["Debit"] = ""; r["Credit"] = f"{row['Credit']:.2f}"
                rows.append(r)
        return pd.DataFrame(rows)

# ---------- UI: show results ----------
    def load_summary(df, summary, unmatched):
        total_debit = df["Debit"].sum()
        total_credit = df["Credit"].sum()

        st.subheader("💰 Total Debit & Credit")
        st.markdown(f"- **Total Debit:** {total_debit:,.2f}")
        st.markdown(f"- **Total Credit:** {total_credit:,.2f}")

        tab1, tab2, tab3 = st.tabs(["Grouped Summary", "Detailed DTL Records", "Unmatched Records"])

        with tab1:
            melted = prepare_debit_credit_rows(summary)
            totals_row = {**{c: "" for c in melted.columns}, "DTV_Company": "TOTAL",
                        "Debit": f"{total_debit:.2f}", "Credit": f"{total_credit:.2f}"}
            melted = pd.concat([melted, pd.DataFrame([totals_row])], ignore_index=True)
            st.dataframe(melted, use_container_width=True)
            buf = io.StringIO(); melted.to_csv(buf, index=False)
            st.download_button("💾 Download Grouped Summary", buf.getvalue(), "Grouped_Summary.csv", "text/csv")

        with tab2:
        # show relevant columns but ensure user sees mapped value
            display_df = df.copy()
        # ensure visible Product Code column shows the mapped value (rename Product -> Product Code)
            display_df["Product Code (Mapped)"] = display_df["Product"]
            st.dataframe(display_df, use_container_width=True)
            buf = io.StringIO(); display_df.to_csv(buf, index=False)
            st.download_button("💾 Download Detailed DTL", buf.getvalue(), "Detailed_DTL.csv", "text/csv")

        with tab3:
            if unmatched.empty:
                st.info("All records matched successfully! ✅ No unmatched records.")
            else:
                st.warning("⚠ Unmatched records found (main/sub/company/product).")
            # show mapping diagnosis columns
                show_cols = ["Product Code", "FIN_PRD_CD", "Product", "DTV_Company", "DTV_Main_Account", "DTV_Sub_Account", "Amount"]
                cols_to_show = [c for c in show_cols if c in unmatched.columns]
                st.dataframe(unmatched[cols_to_show], use_container_width=True)
                buf = io.StringIO(); unmatched[cols_to_show].to_csv(buf, index=False)
                st.download_button("💾 Download Unmatched Records", buf.getvalue(), "Unmatched_Records.csv", "text/csv")

# ---------- Streamlit file upload controls ----------
    csv_file = st.file_uploader("Upload Transaction CSV", type=["csv"])
    mapping_files = st.file_uploader("Upload All Mapping Text Files (pipe-delimited .txt)", type=["txt"], accept_multiple_files=True)
    product_file = st.file_uploader("Upload Product Mapping Excel", type=["xlsx"])

    if csv_file and mapping_files and product_file:
        try:
            with st.spinner("🚀 Extracting data..."):
                df, sam_df, mam_df, ma_df, company_df, ccm_df = extract_data(csv_file, mapping_files)
                product_df = load_product_mapping(product_file, sheet_name="DTV_BDS_UB_PRODUCT_MAPPING")

            with st.spinner("⚙️ Transforming data..."):
                df, summary, unmatched = transform_data(df, sam_df, mam_df, ma_df, company_df, ccm_df, product_df)

            with st.spinner("📦 Loading results..."):
                load_summary(df, summary, unmatched)

            st.success("✅ ETL process completed successfully!")
        except Exception as e:
            st.error(f"❌ Error: {e}")
    else:
        st.info("Please upload Transaction CSV, mapping text files, and Product Mapping Excel (sheet: DTV_BDS_UB_PRODUCT_MAPPING).")

if __name__ == "__main__":
    main()

