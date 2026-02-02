from flask import Flask, jsonify, render_template, Response
import sqlite3
import pathlib
import json

app = Flask(__name__)
DB_NAME = "sap_project.db"

# --- Database Helper ---
def get_db_connection():
    """Establishes a connection to the SQLite database."""
    conn = sqlite3.connect(DB_NAME)
    # This allows us to access columns by name (e.g., row['owner']) instead of index
    conn.row_factory = sqlite3.Row 
    return conn

# --- The Controller Route ---
@app.route('/')
def dashboard():
    conn = get_db_connection()
    
    # ==========================================
    # 1. KPI CARDS (Top of Dashboard)
    # ==========================================
    # We aggregate data to show high-level project health
    kpi_query = """
    SELECT 
        SUM(budget_spent) as total_spend,
        AVG(cpi) as avg_cpi,
        (SELECT COUNT(*) FROM change_requests WHERE status = 'Approved') as active_risks
    FROM progress
    WHERE week_ending = (SELECT MAX(week_ending) FROM progress)
    """
    kpi_data = conn.execute(kpi_query).fetchone()

    # ==========================================
    # 2. MAIN TABLE (Project Status Grid)
    # ==========================================
    # We JOIN Workstreams with Progress to get the "Latest Status"
    # This serves the detailed project cards/table in the UI
    status_query = """
    SELECT 
        w.ws_id, 
        w.name, 
        w.owner, 
        p.actual_pct, 
        p.planned_pct, 
        p.schedule_variance, 
        p.budget_spent,
        p.cpi
    FROM workstreams w
    JOIN progress p ON w.ws_id = p.ws_id
    WHERE p.week_ending = (SELECT MAX(week_ending) FROM progress)
    ORDER BY p.schedule_variance ASC
    """
    projects = conn.execute(status_query).fetchall()

    # ==========================================
    # 3. LINE CHART DATA (S-Curve)
    # ==========================================
    # Chart.js needs lists of data. We fetch the full history here.
    history_query = """
    SELECT week_ending, ws_id, actual_pct, planned_pct 
    FROM progress 
    ORDER BY week_ending ASC
    """
    history_rows = conn.execute(history_query).fetchall()
    
    # Process into JSON structure for Chart.js
    # We need a unique list of dates for the X-Axis
    labels = sorted(list(set([row['week_ending'] for row in history_rows])))
    
    # We need to organize data by Workstream for the lines
    chart_datasets = {}
    for row in history_rows:
        ws_id = row['ws_id']
        if ws_id not in chart_datasets:
            chart_datasets[ws_id] = {'actual': [], 'planned': []}
        chart_datasets[ws_id]['actual'].append(row['actual_pct'])
        chart_datasets[ws_id]['planned'].append(row['planned_pct'])

    # ==========================================
    # 4. BUBBLE CHART DATA (Risk vs. Delay)
    # ==========================================
    # We aggregate Change Requests (Count & Cost) and join with current Delay
    bubble_query = """
    SELECT 
        w.name, 
        COUNT(cr.cr_id) as cr_count, 
        COALESCE(SUM(cr.cost_impact), 0) as total_cr_cost, 
        p.schedule_variance
    FROM workstreams w
    LEFT JOIN change_requests cr ON w.ws_id = cr.ws_id AND cr.status = 'Approved'
    JOIN progress p ON w.ws_id = p.ws_id
    WHERE p.week_ending = (SELECT MAX(week_ending) FROM progress)
    GROUP BY w.ws_id
    """
    bubble_rows = conn.execute(bubble_query).fetchall()
    
    # Format for Chart.js Bubble format: {x: 10, y: 20, r: 5}
    bubble_data = []
    for row in bubble_rows:
        bubble_data.append({
            'label': row['name'],
            'x': row['cr_count'],              # X-Axis: Volume of Changes
            'y': row['schedule_variance'],     # Y-Axis: Schedule Delay (Negative is bad)
            'r': row['total_cr_cost'] / 10000  # Radius: Cost Impact (scaled down to fit screen)
        })

    conn.close()

    # Pass everything to the HTML template
    return render_template(
        'dashboard.html', 
        kpis=kpi_data,
        projects=projects,
        chart_labels=json.dumps(labels),
        chart_datasets=json.dumps(chart_datasets),
        bubble_data=json.dumps(bubble_data)
    )

if __name__ == '__main__':
    app.run(debug=True)