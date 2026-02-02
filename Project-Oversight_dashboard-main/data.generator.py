import sqlite3
import random
from datetime import datetime, timedelta
import os

# Configuration
DB_NAME = 'sap_project.db'
NUM_WEEKS = 24  # 6 months of data
START_DATE = datetime.now() - timedelta(weeks=NUM_WEEKS)

def create_schema(cursor):
    # 1. Table: Workstreams (Metadata)
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS workstreams (
        ws_id TEXT PRIMARY KEY,
        name TEXT,
        owner TEXT,
        budget REAL,
        complexity TEXT
    )
    ''')

    # 2. Table: Progress (Time Series)
    # Note the FOREIGN KEY linking to workstreams
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS progress (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        week_ending DATE,
        ws_id TEXT,
        planned_pct REAL,
        actual_pct REAL,
        budget_spent REAL,
        schedule_variance REAL,
        cpi REAL,
        FOREIGN KEY (ws_id) REFERENCES workstreams(ws_id)
    )
    ''')

    # 3. Table: ChangeRequests (Risks)
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS change_requests (
        cr_id TEXT PRIMARY KEY,
        ws_id TEXT,
        date_raised DATE,
        title TEXT,
        status TEXT,
        cost_impact REAL,
        time_impact_days INTEGER,
        FOREIGN KEY (ws_id) REFERENCES workstreams(ws_id)
    )
    ''')

def generate_data(cursor):
    print("--- Generating SQL Data ---")
    
    # --- 1. Workstreams Data ---
    workstreams_data = [
        ('WS_001', 'Finance (FICO)', 'Sarah J.', 1500000, 'High'),
        ('WS_002', 'Supply Chain (MM/SD)', 'Mike R.', 2200000, 'High'),
        ('WS_003', 'Human Capital (HXM)', 'Amit P.', 800000, 'Medium'),
        ('WS_004', 'Data Migration', 'Emily W.', 600000, 'Critical'),
        ('WS_005', 'Analytics (BW/SAC)', 'John D.', 450000, 'Medium')
    ]
    cursor.executemany('INSERT OR REPLACE INTO workstreams VALUES (?,?,?,?,?)', workstreams_data)
    print("✔ Populated Workstreams")

    # --- 2. Progress Data ---
    progress_records = []
    
    # Helper to access budget from the list above
    ws_budget_map = {row[0]: row[3] for row in workstreams_data}

    for ws in workstreams_data:
        ws_id = ws[0]
        budget = ws[3]
        
        current_planned = 0
        current_actual = 0
        current_spend = 0
        
        for i in range(NUM_WEEKS):
            week_date = START_DATE + timedelta(weeks=i)
            week_str = week_date.strftime('%Y-%m-%d')
            
            # Logic: Linear Plan
            increment_plan = 100 / NUM_WEEKS
            current_planned = min(100, current_planned + increment_plan)
            
            # Logic: Actuals (The Story)
            if ws_id == 'WS_002': # The Failing Project
                increment_actual = increment_plan * random.uniform(0.3, 0.8) 
                spend_rate = (budget / NUM_WEEKS) * random.uniform(1.1, 1.5)
            elif ws_id == 'WS_001': # The Good Project
                increment_actual = increment_plan * random.uniform(0.95, 1.05)
                spend_rate = (budget / NUM_WEEKS) * random.uniform(0.9, 1.1)
            else: # Average
                increment_actual = increment_plan * random.uniform(0.8, 1.0)
                spend_rate = (budget / NUM_WEEKS) * random.uniform(0.9, 1.1)

            current_actual = min(100, current_actual + increment_actual)
            current_spend += spend_rate
            
            # Metrics
            variance = round(current_actual - current_planned, 2)
            cpi = round((current_actual/100 * budget) / (current_spend + 1), 2)

            progress_records.append((
                week_str, ws_id, round(current_planned, 2), 
                round(current_actual, 2), round(current_spend, 2), 
                variance, cpi
            ))

    cursor.executemany('''
        INSERT INTO progress (week_ending, ws_id, planned_pct, actual_pct, budget_spent, schedule_variance, cpi) 
        VALUES (?,?,?,?,?,?,?)
    ''', progress_records)
    print("✔ Populated Progress Logs")

    # --- 3. Change Requests Data ---
    cr_records = []
    cr_counter = 1000

    for ws in workstreams_data:
        ws_id = ws[0]
        
        # Logic: Correlate CRs to Failure
        if ws_id == 'WS_002': num_crs = random.randint(15, 25) # High Risk
        elif ws_id == 'WS_001': num_crs = random.randint(0, 3) # Low Risk
        else: num_crs = random.randint(3, 8)
            
        for _ in range(num_crs):
            cr_date = START_DATE + timedelta(days=random.randint(0, NUM_WEEKS*7))
            cr_records.append((
                f"CR_{cr_counter}",
                ws_id,
                cr_date.strftime('%Y-%m-%d'),
                f"Change Req #{random.randint(1,99)}",
                random.choice(['Approved', 'Approved', 'Rejected', 'Pending']),
                random.randint(5000, 50000),
                random.randint(1, 10)
            ))
            cr_counter += 1

    cursor.executemany('INSERT OR REPLACE INTO change_requests VALUES (?,?,?,?,?,?,?)', cr_records)
    print("✔ Populated Change Requests")

# --- Execution ---
if __name__ == '__main__':
    # Remove old DB if exists to start fresh
    if os.path.exists(DB_NAME):
        os.remove(DB_NAME)
        
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    create_schema(cursor)
    generate_data(cursor)
    
    conn.commit()
    conn.close()
    print(f"\nSUCCESS: Database '{DB_NAME}' created.")