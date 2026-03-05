import sqlite3
import time
import requests
import json
import os
from OpenOrchestrator.orchestrator_connection.connection import OrchestratorConnection
from GetFilarkivAcessToken import GetFilarkivToken


orchestrator_connection = OrchestratorConnection(
    "MinEjenDomToFilarkiv_Dispatcher",
    os.getenv('OpenOrchestratorSQL'),
    os.getenv('OpenOrchestratorKey'),
    None,
    None
)

SQLITE_PATH = r"C:\Users\az72987\Desktop\minejendom2filarkiv.db"

FilarkivURL = orchestrator_connection.get_constant("FilarkivURL").value
Filarkiv_access_token = GetFilarkivToken(orchestrator_connection)


# -----------------------------
# FILARKIV API CALL
# -----------------------------

def add_basic_data_api(case_id, basic_data_type, basic_data_id, existing_basicdata):

    key = normalize_basicdata_key(basic_data_type, basic_data_id)

    if key in existing_basicdata:
        print(f"   Skipping existing basicdata {key}")
        return

    data = {
        "caseId": case_id,
        "basicDataType": basic_data_type,
        "basicDataId": str(basic_data_id)
    }

    response = requests.post(
        f"{FilarkivURL}/BasicData",
        headers={
            "Authorization": f"Bearer {Filarkiv_access_token}",
            "Content-Type": "application/json"
        },
        json=data
    )

    if response.status_code not in [200, 201]:
        raise Exception(f"FilArkiv API error: {response.status_code} - {response.text}")

    print(f"Added basicdata {key}")

    # Prevent duplicates in the same run
    existing_basicdata.add(key)


# -----------------------------
# MAIN PROCESS
# -----------------------------

def add_basic_data_to_cases(db_path: str, delay_ms: int = 1000):

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cases = get_cases_missing_basicdata(cur)

    if not cases:
        print("✓ All FilArkiv cases already have basic data.")
        conn.close()
        return

    print(f"Found {len(cases)} cases missing basic data.")

    for row in cases:

        case_id = row["FilArkivCaseId"]
        case_num = row["CaseNumber"]

        HusnummerId = row["HusnummerId"]
        bfe_number = row.get("Bfe")
        ejerlav = row["Ejerlavskode"]
        matrikelnr = row["Matrikelnummer"]

        try:

            print(f"\nProcessing case {case_num}")

            existing_basicdata = get_existing_basicdata(case_id)

            if HusnummerId:
                add_basic_data_api(case_id, 1, HusnummerId, existing_basicdata)

            if bfe_number:
                add_basic_data_api(case_id, 4, bfe_number, existing_basicdata)

            if ejerlav and matrikelnr:
                combo = f"{ejerlav} {matrikelnr}"
                add_basic_data_api(case_id, 3, combo, existing_basicdata)

            mark_basicdata_processed(cur, case_id)

            conn.commit()

            print(f"✓ Case {case_num} processed")

            time.sleep(delay_ms / 1000)

        except Exception as ex:

            print(f"✗ Error updating case {case_num}: {ex}")

            cur.execute(
                """
                UPDATE MinEjendom_Cases
                SET BasicDataErrors = ?
                WHERE FilArkivCaseId = ?
                """,
                (str(ex), case_id),
            )

            conn.commit()

    conn.close()

    print("\nAll cases processed.")


# -----------------------------
# DATABASE QUERY
# -----------------------------

def get_cases_missing_basicdata(cur):

    sql = """
        SELECT
            c.Id AS CaseId,
            c.CaseNumber,
            c.CaseTitle,
            c.CaseDate,
            c.FilArkivCaseId,
            c.CaseExists,
            c.IgnoreCase,
            c.FilArkivArchiveId,
            c.Note,
            c.BasicDataProcessed,
            c.BasicDataErrors,

            ca.AddressId,
            a.*

        FROM MinEjendom_Cases c

        INNER JOIN MinEjendom_CaseAddresses ca
            ON ca.CaseId = c.Id

        INNER JOIN MinEjendom_Addresses a
            ON ca.AddressId = a.Id

        WHERE c.FilArkivCaseId IS NOT NULL
        AND c.BasicDataProcessed IS NULL
        AND c.IgnoreCase = 0
        LIMIT 1;
    """

    cur.execute(sql)

    return [dict(r) for r in cur.fetchall()]


# -----------------------------
# BASIC DATA FUNCTIONS
# -----------------------------

def normalize_basicdata_key(basic_data_type, basic_data_id):

    value = str(basic_data_id).strip()

    # Only normalize UUIDs
    if basic_data_type == 1:
        value = value.lower()

    return (int(basic_data_type), value)


def get_existing_basicdata(case_id):

    response = requests.get(
        f"{FilarkivURL}/BasicData",
        headers={"Authorization": f"Bearer {Filarkiv_access_token}"},
        params={"caseId": case_id}
    )

    if response.status_code != 200:
        raise Exception(f"Failed to fetch existing basic data: {response.text}")

    data = response.json()

    existing = set()

    for item in data:
        key = normalize_basicdata_key(
            item["basicDataType"],
            item["basicDataId"]
        )
        existing.add(key)

    return existing


# -----------------------------
# MARK CASE AS PROCESSED
# -----------------------------

def mark_basicdata_processed(cur, case_id):

    cur.execute(
        """
        UPDATE MinEjendom_Cases
        SET BasicDataProcessed = 1
        WHERE FilArkivCaseId = ?
        """,
        (case_id,),
    )


# -----------------------------
# RUN SCRIPT
# -----------------------------

if __name__ == "__main__":

    add_basic_data_to_cases(SQLITE_PATH)