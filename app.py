from flask import Flask, render_template, request, jsonify, Response
from databricks import sql
import pandas as pd
import json
from datetime import datetime, timedelta
import csv
import io
from threading import Lock
import os
import requests

app = Flask(__name__)

# ============================================
# DATABRICKS APPS CONFIGURATION
# ============================================
DATABRICKS_HOST_RAW = os.getenv("DATABRICKS_HOST", "")
DATABRICKS_HOST_WITH_HTTPS = DATABRICKS_HOST_RAW if DATABRICKS_HOST_RAW.startswith('https://') else f'https://{DATABRICKS_HOST_RAW}'
DATABRICKS_HOST_WITHOUT_HTTPS = DATABRICKS_HOST_RAW.replace('https://', '').replace('http://', '')

DATABRICKS_HTTP_PATH = os.getenv("DATABRICKS_HTTP_PATH")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")  

DATABRICKS_CATALOG = os.getenv('DATABRICKS_CATALOG', '')
DATABRICKS_SCHEMA = os.getenv('DATABRICKS_SCHEMA', '')

WAREHOUSE_ID = DATABRICKS_HTTP_PATH.split('/')[-1] if DATABRICKS_HTTP_PATH else None

# Table references
PATIENT_TABLE = f"{DATABRICKS_CATALOG}.{DATABRICKS_SCHEMA}."
GENOMIC_TABLE = f"{DATABRICKS_CATALOG}.{DATABRICKS_SCHEMA}."
ASSAY_TABLE = f"{DATABRICKS_CATALOG}.{DATABRICKS_SCHEMA}."
ECOG_TABLE = f"{DATABRICKS_CATALOG}.{DATABRICKS_SCHEMA}."


def is_running_in_databricks_apps():
    """Check if running inside Databricks Apps environment."""
    return request.headers.get('X-Forwarded-Email') is not None


def get_current_user_email():
    """Get the current user's email from Databricks Apps headers."""
    return request.headers.get('X-Forwarded-Email')


def get_user_access_token():
    """
    Get the user's access token from Databricks Apps.
    
    Databricks Apps should provide the user's token via headers
    when the app is configured for user identity passthrough.
    """
    # Check various possible header locations
    auth_header = request.headers.get('Authorization', '')
    if auth_header.startswith('Bearer '):
        return auth_header[7:]
    
    # Databricks Apps specific headers
    user_token = request.headers.get('X-Databricks-Token')
    if user_token:
        return user_token
    
    user_token = request.headers.get('X-Forwarded-Access-Token')
    if user_token:
        return user_token
    
    return None


# ============================================
# DEBUG ENDPOINT
# ============================================

@app.route('/api/debug')
def debug():
    """Debug endpoint to test all connection methods."""
    results = {
        'environment': {
            'DATABRICKS_HOST': DATABRICKS_HOST_RAW,
            'DATABRICKS_HTTP_PATH': DATABRICKS_HTTP_PATH,
            'WAREHOUSE_ID': WAREHOUSE_ID,
            'HAS_PAT': bool(DATABRICKS_TOKEN),
            'AUTO_DETECTED_CLIENT_ID': os.getenv('DATABRICKS_CLIENT_ID', 'not set'),
            'AUTO_DETECTED_CLIENT_SECRET': 'set' if os.getenv('DATABRICKS_CLIENT_SECRET') else 'not set',
        },
        'request_headers': {
            'X-Forwarded-Email': request.headers.get('X-Forwarded-Email'),
            'X-Forwarded-User': request.headers.get('X-Forwarded-User'),
            'Authorization': 'Bearer ***' if request.headers.get('Authorization', '').startswith('Bearer ') else 'not present',
            'X-Databricks-Token': 'present' if request.headers.get('X-Databricks-Token') else 'not present',
            'X-Forwarded-Access-Token': 'present' if request.headers.get('X-Forwarded-Access-Token') else 'not present',
        },
        'all_headers': {k: ('***' if 'token' in k.lower() or 'secret' in k.lower() or 'auth' in k.lower() else v) 
                        for k, v in request.headers},
        'tests': {}
    }
    
    # Test 1: SQL Connector with User Token (if available)
    user_token = get_user_access_token()
    if user_token:
        results['tests']['sql_with_user_token'] = test_sql_with_token(user_token, 'user_token')
    else:
        results['tests']['sql_with_user_token'] = {'status': 'SKIPPED', 'reason': 'No user token found in headers'}
    
    # Test 2: SQL Connector without any auth (let environment handle it)
    results['tests']['sql_no_explicit_auth'] = test_sql_no_auth()
    
    # Test 3: SDK with explicitly cleared SP credentials
    results['tests']['sdk_without_sp'] = test_sdk_without_sp()
    
    # Test 4: PAT (local dev)
    if DATABRICKS_TOKEN:
        results['tests']['sql_with_pat'] = test_sql_with_token(DATABRICKS_TOKEN, 'PAT')
    
    return jsonify(results)


def test_sql_with_token(token, token_type):
    """Test SQL connector with a specific token."""
    try:
        from databricks import sql
        
        conn = sql.connect(
            server_hostname=DATABRICKS_HOST_WITHOUT_HTTPS,
            http_path=DATABRICKS_HTTP_PATH,
            access_token=token
        )
        
        with conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT CURRENT_USER() as user")
                result = cursor.fetchone()
                return {'status': 'SUCCESS', 'token_type': token_type, 'current_user': result[0]}
                
    except Exception as e:
        return {'status': 'ERROR', 'token_type': token_type, 'error': f'{type(e).__name__}: {str(e)}'}


def test_sql_no_auth():
    """Test SQL connector without explicit auth."""
    try:
        from databricks import sql
        
        conn = sql.connect(
            server_hostname=DATABRICKS_HOST_WITHOUT_HTTPS,
            http_path=DATABRICKS_HTTP_PATH
        )
        
        with conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT CURRENT_USER() as user")
                result = cursor.fetchone()
                return {'status': 'SUCCESS', 'current_user': result[0]}
                
    except Exception as e:
        return {'status': 'ERROR', 'error': f'{type(e).__name__}: {str(e)}'}


def test_sdk_without_sp():
    """Test SDK with SP credentials explicitly cleared."""
    try:
        from databricks.sdk import WorkspaceClient
        from databricks.sdk.service.sql import StatementState
        
        # Clear the auto-detected SP credentials temporarily
        original_client_id = os.environ.pop('DATABRICKS_CLIENT_ID', None)
        original_client_secret = os.environ.pop('DATABRICKS_CLIENT_SECRET', None)
        
        try:
            # Now SDK should NOT use oauth-m2m
            w = WorkspaceClient(host=DATABRICKS_HOST_WITH_HTTPS)
            
            # Check what auth type is being used
            auth_type = w.config.auth_type if hasattr(w.config, 'auth_type') else 'unknown'
            
            response = w.statement_execution.execute_statement(
                warehouse_id=WAREHOUSE_ID,
                statement="SELECT CURRENT_USER() as user",
                catalog=DATABRICKS_CATALOG,
                schema=DATABRICKS_SCHEMA,
                wait_timeout="30s"
            )
            
            if response.status.state == StatementState.SUCCEEDED:
                query_user = response.result.data_array[0][0] if response.result and response.result.data_array else 'no result'
                return {'status': 'SUCCESS', 'auth_type': auth_type, 'current_user': query_user}
            else:
                return {'status': 'QUERY_FAILED', 'auth_type': auth_type, 
                        'error': response.status.error.message if response.status.error else 'unknown'}
                
        finally:
            # Restore the environment variables
            if original_client_id:
                os.environ['DATABRICKS_CLIENT_ID'] = original_client_id
            if original_client_secret:
                os.environ['DATABRICKS_CLIENT_SECRET'] = original_client_secret
                
    except Exception as e:
        return {'status': 'ERROR', 'error': f'{type(e).__name__}: {str(e)}'}


# ============================================
# QUERY EXECUTION 
# ============================================

def execute_query(query, params=None):
    """Execute a SQL query."""
    try:
        app.logger.info(f"Executing query: {query[:200]}...")
        
        if is_running_in_databricks_apps():
            # Try user token first
            user_token = get_user_access_token()
            if user_token:
                app.logger.info("Using user token from headers")
                return execute_query_with_token(query, user_token)
            
            # Try SDK with SP credentials cleared
            app.logger.info("Trying SDK without SP credentials")
            return execute_query_with_sdk_no_sp(query)
        else:
            # Local development with PAT
            if DATABRICKS_TOKEN:
                return execute_query_with_token(query, DATABRICKS_TOKEN)
            raise ValueError("No DATABRICKS_TOKEN set for local development")
            
    except Exception as e:
        app.logger.error(f"Database error: {e}")
        return []


def execute_query_with_token(query, token):
    """Execute query using a specific token."""
    from databricks import sql
    
    conn = sql.connect(
        server_hostname=DATABRICKS_HOST_WITHOUT_HTTPS,
        http_path=DATABRICKS_HTTP_PATH,
        access_token=token
    )
    
    with conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            columns = [desc[0] for desc in cursor.description]
            results = cursor.fetchall()
            app.logger.info(f"Query returned {len(results)} rows")
            return [dict(zip(columns, row)) for row in results]


def execute_query_with_sdk_no_sp(query):
    """Execute query using SDK with SP credentials cleared."""
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.sql import StatementState
    
    # Temporarily clear SP credentials so SDK uses different auth
    original_client_id = os.environ.pop('DATABRICKS_CLIENT_ID', None)
    original_client_secret = os.environ.pop('DATABRICKS_CLIENT_SECRET', None)
    
    try:
        w = WorkspaceClient(host=DATABRICKS_HOST_WITH_HTTPS)
        
        response = w.statement_execution.execute_statement(
            warehouse_id=WAREHOUSE_ID,
            statement=query,
            catalog=DATABRICKS_CATALOG,
            schema=DATABRICKS_SCHEMA,
            wait_timeout="30s"
        )
        
        if response.status.state == StatementState.SUCCEEDED:
            if response.manifest and response.manifest.schema:
                columns = [col.name for col in response.manifest.schema.columns]
                rows = response.result.data_array if response.result and response.result.data_array else []
                return [dict(zip(columns, row)) for row in rows]
            return []
        else:
            error_msg = response.status.error.message if response.status.error else "Unknown error"
            raise Exception(f"Query failed: {error_msg}")
            
    finally:
        # Restore environment variables
        if original_client_id:
            os.environ['DATABRICKS_CLIENT_ID'] = original_client_id
        if original_client_secret:
            os.environ['DATABRICKS_CLIENT_SECRET'] = original_client_secret


# ============================================
# API ENDPOINTS
# ============================================

@app.route('/api/whoami')
def whoami():
    """Check current user identity and row filter status."""
    results = {
        'environment': 'Databricks Apps' if is_running_in_databricks_apps() else 'Local',
        'forwarded_email': get_current_user_email(),
        'has_user_token': get_user_access_token() is not None,
    }
    
    try:
        result = execute_query("SELECT CURRENT_USER() as user")
        results['current_user'] = result[0]['user'] if result else 'no result'
    except Exception as e:
        results['current_user'] = f'error: {str(e)}'
    
    # Check group memberships
    try:
        query = f"""
            SELECT DISTINCT p.group_name, is_account_group_member(p.group_name) as is_member
            FROM {DATABRICKS_CATALOG}.prod_config.cohort_permissions p
        """
        results['group_memberships'] = execute_query(query)
    except Exception as e:
        results['group_memberships'] = f'error: {str(e)}'
    
    # Check accessible cohorts
    try:
        query = f"""
            SELECT COUNT(DISTINCT cohort_id) as cnt
            FROM {DATABRICKS_CATALOG}.prod_config.cohort_permissions p
            WHERE is_account_group_member(p.group_name)
        """
        result = execute_query(query)
        results['accessible_cohorts'] = result[0]['cnt'] if result else 0
    except Exception as e:
        results['accessible_cohorts'] = f'error: {str(e)}'
    
    # Status check
    current_user = results.get('current_user', '')
    if isinstance(current_user, str) and '@' in current_user and 'error' not in current_user.lower():
        results['row_filtering_status'] = 'WORKING - User identity confirmed'
    else:
        results['row_filtering_status'] = 'NOT WORKING - See /api/debug for details'
    
    return jsonify(results)


@app.route('/')
def index():
    """Dashboard homepage."""
    stats = {
        'total_patients': 0,
        'total_mutations': 0,
        'total_assays': 0,        
        'current_user': get_current_user_email() or 'Unknown'
    }
    
    try:
        result = execute_query(f"SELECT COUNT(DISTINCT cohort_id) as count FROM {PATIENT_TABLE}")
        if result:
            stats['total_patients'] = result[0]['count']
        
        result = execute_query(f"SELECT COUNT(DISTINCT gene_name_hgnc) as count FROM {GENOMIC_TABLE}")
        if result:
            stats['total_mutations'] = result[0]['count']
        
        result = execute_query(f"SELECT COUNT(*) as count FROM {ASSAY_TABLE}")
        if result:
            stats['total_assays'] = result[0]['count']
                
            
    except Exception as e:
        app.logger.error(f"Error loading stats: {e}")
    
    return render_template('index.html', stats=stats)


@app.route('/patients')
def patients():
    """Patient search and listing page."""
    return render_template('patients.html')

@app.route('/api/patients/search', methods=['GET'])
def search_patients():
    """API endpoint to search patients."""
    mrn = request.args.get('mrn', '')
    name = request.args.get('name', '')
    gene = request.args.get('gene', '')
    
    query = f"""
        SELECT DISTINCT 
            p.cohort_id,
            p.mrn,
            p.first_name,
            p.last_name,
            p.dob,
            p.gender,
            p.race,
            p.ethnicity
        FROM {PATIENT_TABLE} p
    """
    
    conditions = []
    if mrn:
        conditions.append(f"p.mrn LIKE '%{mrn}%'")
    if name:
        conditions.append(f"(LOWER(p.first_name) LIKE LOWER('%{name}%') OR LOWER(p.last_name) LIKE LOWER('%{name}%'))")
    if gene:
        query += f" JOIN {ASSAY_TABLE} a ON p.cohort_id = a.cohort_id"
        query += f" JOIN {GENOMIC_TABLE} g ON a.result_id = g.result_id"
        conditions.append(f"UPPER(g.gene_name_hgnc) = UPPER('{gene}')")
    
    if conditions:
        query += " WHERE " + " AND ".join(conditions)
    
    query += " LIMIT 100"
    
    results = execute_query(query)
    return jsonify(results)

@app.route('/mutations')
def mutations():
    """Mutation analysis page."""
    # Get list of all genes for dropdown
    query = f"SELECT DISTINCT gene_name_hgnc FROM {GENOMIC_TABLE} WHERE gene_name_hgnc IS NOT NULL ORDER BY gene_name_hgnc"
    genes = execute_query(query)
    gene_list = [g['gene_name_hgnc'] for g in genes]
    return render_template('mutations.html', genes=gene_list)

@app.route('/api/mutations/count', methods=['GET'])
def mutation_count():
    """API endpoint to count patients with specific mutations."""
    gene = request.args.get('gene', '')
    date_from = request.args.get('date_from', '')
    date_to = request.args.get('date_to', '')
    second_gene = request.args.get('second_gene', '')
    
    if not gene:
        return jsonify({'error': 'Gene parameter required'}), 400
    
    # Base query - join genomic to assay to patient
    query = f"""
        SELECT 
            COUNT(DISTINCT a.cohort_id) as patient_count
        FROM {GENOMIC_TABLE} g
        JOIN {ASSAY_TABLE} a ON g.result_id = a.result_id
        WHERE UPPER(g.gene_name_hgnc) = UPPER('{gene}')
    """
    
    if date_from:
        query += f" AND a.specimen_collected_dttm >= '{date_from}'"
    if date_to:
        query += f" AND a.specimen_collected_dttm <= '{date_to}'"
    
    if second_gene:
        query = f"""
            SELECT COUNT(DISTINCT a1.cohort_id) as patient_count
            FROM {ASSAY_TABLE} a1
            WHERE a1.cohort_id IN (
                SELECT DISTINCT a.cohort_id
                FROM {GENOMIC_TABLE} g
                JOIN {ASSAY_TABLE} a ON g.result_id = a.result_id
                WHERE UPPER(g.gene_name_hgnc) = UPPER('{gene}')
            )
            AND a1.cohort_id IN (
                SELECT DISTINCT a.cohort_id
                FROM {GENOMIC_TABLE} g
                JOIN {ASSAY_TABLE} a ON g.result_id = a.result_id
                WHERE UPPER(g.gene_name_hgnc) = UPPER('{second_gene}')
            )
        """
    
    result = execute_query(query)
    return jsonify(result[0] if result else {'patient_count': 0})


@app.route('/api/mutations/patients', methods=['GET'])
def mutation_patients():
    """API endpoint to get patient list with specific mutations."""
    gene = request.args.get('gene', '')
    
    if not gene:
        return jsonify({'error': 'Gene parameter required'}), 400
    
    query = f"""
        SELECT DISTINCT 
            p.cohort_id,
            p.mrn,
            p.first_name,
            p.last_name,
            g.gene_name_hgnc,
            g.amino_acid_change_hgvs,
            g.mutation_vaf,
            a.specimen_collected_dttm,
            a.proc_name
        FROM {GENOMIC_TABLE} g
        JOIN {ASSAY_TABLE} a ON g.result_id = a.result_id
        JOIN {PATIENT_TABLE} p ON a.cohort_id = p.cohort_id
        WHERE UPPER(g.gene_name_hgnc) = UPPER('{gene}')
        ORDER BY a.specimen_collected_dttm DESC
        LIMIT 500
    """
    
    results = execute_query(query)
    return jsonify(results)


@app.route('/api/mutations/yearly', methods=['GET'])
def mutations_yearly():
    """API endpoint to get yearly mutation counts."""
    gene = request.args.get('gene', '')
    
    if not gene:
        return jsonify({'error': 'Gene parameter required'}), 400
    
    query = f"""
        SELECT 
            YEAR(a.specimen_collected_dttm) as year,
            COUNT(DISTINCT a.cohort_id) as patient_count
        FROM {GENOMIC_TABLE} g
        JOIN {ASSAY_TABLE} a ON g.result_id = a.result_id
        WHERE UPPER(g.gene_name_hgnc) = UPPER('{gene}')
            AND a.specimen_collected_dttm IS NOT NULL
        GROUP BY YEAR(a.specimen_collected_dttm)
        ORDER BY year
    """
    
    results = execute_query(query)
    return jsonify(results)

@app.route('/assays')
def assays():
    """Assay/NGS Panel search page."""
    return render_template('assays.html')

@app.route('/api/assays/search', methods=['GET'])
def search_assays():
    """API endpoint to search assays."""
    panel_type = request.args.get('panel_type', '')
    date_from = request.args.get('date_from', '')
    date_to = request.args.get('date_to', '')
    cohort_id = request.args.get('cohort_id', '')
    
    query = f"""
        SELECT 
            a.result_id,
            a.cohort_id,
            a.specimen_number,
            a.proc_name,
            a.ordering_dttm,
            a.specimen_collected_dttm,
            a.result_time,
            p.mrn,
            p.first_name,
            p.last_name
        FROM {ASSAY_TABLE} a
        JOIN {PATIENT_TABLE} p ON a.cohort_id = p.cohort_id
        WHERE 1=1
    """
    
    if panel_type:
        query += f" AND LOWER(a.proc_name) LIKE LOWER('%{panel_type}%')"
    if date_from:
        query += f" AND a.specimen_collected_dttm >= '{date_from}'"
    if date_to:
        query += f" AND a.specimen_collected_dttm <= '{date_to}'"
    if cohort_id:
        query += f" AND a.cohort_id = '{cohort_id}'"
    
    query += " ORDER BY a.specimen_collected_dttm DESC LIMIT 200"
    
    results = execute_query(query)
    return jsonify(results)

@app.route('/api/assays/count_by_panel', methods=['GET'])
def assays_count_by_panel():
    """API endpoint to count patients tested with specific panel."""
    panel_name = request.args.get('panel_name', 'LEUKEMIA')
    
    query = f"""
        SELECT 
            COUNT(DISTINCT a.cohort_id) as patient_count,
            COUNT(*) as total_tests
        FROM {ASSAY_TABLE} a
        WHERE LOWER(a.proc_name) LIKE LOWER('%{panel_name}%')
    """
    
    result = execute_query(query)
    return jsonify(result[0] if result else {'patient_count': 0, 'total_tests': 0})


@app.route('/ecog')
def ecog():
    """ECOG Performance Status page."""
    return render_template('ecog.html')

@app.route('/api/ecog/search', methods=['GET'])
def search_ecog():
    """API endpoint to search ECOG scores."""
    cohort_id = request.args.get('cohort_id', '')
    ecog_score = request.args.get('ecog_score', '')
    date_from = request.args.get('date_from', '')
    date_to = request.args.get('date_to', '')
    
    query = f"""
        SELECT 
            e.cohort_id,
            e.ecog,
            e.ecogps_interpretation,
            e.ecog_date_full,
            e.note_type,
            p.mrn,
            p.first_name,
            p.last_name
        FROM {ECOG_TABLE} e
        JOIN {PATIENT_TABLE} p ON e.cohort_id = p.cohort_id
        WHERE 1=1
    """
    
    if cohort_id:
        query += f" AND e.cohort_id = '{cohort_id}'"
    if ecog_score:
        query += f" AND e.ecog = {ecog_score}"
    if date_from:
        query += f" AND e.ecog_date_full >= '{date_from}'"
    if date_to:
        query += f" AND e.ecog_date_full <= '{date_to}'"
    
    query += " ORDER BY e.ecog_date_full DESC LIMIT 200"
    
    results = execute_query(query)
    return jsonify(results)


# ==================== OVERVIEW CHARTS API ENDPOINTS ====================

@app.route('/api/overview/patient_sex', methods=['GET'])
def overview_patient_sex():
    """API endpoint to get patient sex distribution."""
    query = f"""
        SELECT 
            gender as label,
            COUNT(*) as count
        FROM {PATIENT_TABLE}
        WHERE gender IS NOT NULL
        GROUP BY gender
        ORDER BY count DESC
    """
    results = execute_query(query)
    return jsonify(results)


@app.route('/api/overview/patient_race', methods=['GET'])
def overview_patient_race():
    """API endpoint to get patient race distribution."""
    query = f"""
        SELECT 
            race as label,
            COUNT(*) as count
        FROM {PATIENT_TABLE}
        WHERE race IS NOT NULL
        GROUP BY race
        ORDER BY count DESC
    """
    results = execute_query(query)
    return jsonify(results)


@app.route('/api/overview/patient_ethnicity', methods=['GET'])
def overview_patient_ethnicity():
    """API endpoint to get patient ethnicity distribution."""
    query = f"""
        SELECT 
            ethnicity as label,
            COUNT(*) as count
        FROM {PATIENT_TABLE}
        WHERE ethnicity IS NOT NULL
        GROUP BY ethnicity
        ORDER BY count DESC
    """
    results = execute_query(query)
    return jsonify(results)


@app.route('/api/overview/test_type', methods=['GET'])
def overview_test_type():
    """API endpoint to get test type distribution."""
    query = f"""
        SELECT 
            proc_name as label,
            COUNT(*) as count
        FROM {ASSAY_TABLE}
        WHERE proc_name IS NOT NULL
        GROUP BY proc_name
        ORDER BY count DESC
    """
    results = execute_query(query)
    return jsonify(results)


@app.route('/api/overview/samples_per_patient', methods=['GET'])
def overview_samples_per_patient():
    """API endpoint to get distribution of samples per patient."""
    query = f"""
        SELECT 
            sample_count as label,
            COUNT(*) as count
        FROM (
            SELECT 
                cohort_id,
                COUNT(DISTINCT specimen_number) as sample_count
            FROM {ASSAY_TABLE}
            GROUP BY cohort_id
        ) sub
        GROUP BY sample_count
        ORDER BY sample_count
    """
    results = execute_query(query)
    return jsonify(results)


@app.route('/api/overview/mutation_count', methods=['GET'])
def overview_mutation_count():
    """API endpoint to get mutation count distribution per specimen."""
    query = f"""
        SELECT 
            mutation_count as label,
            COUNT(*) as count
        FROM (
            SELECT 
                a.specimen_number,
                COUNT(DISTINCT CONCAT(
                    COALESCE(g.gene_name_hgnc, ''),
                    COALESCE(g.cdna_change_hgvs, ''),
                    COALESCE(g.amino_acid_change_hgvs, '')
                )) as mutation_count
            FROM {ASSAY_TABLE} a
            LEFT JOIN {GENOMIC_TABLE} g ON a.result_id = g.result_id
            GROUP BY a.specimen_number
        ) sub
        WHERE mutation_count > 0
        GROUP BY mutation_count
        ORDER BY mutation_count
    """
    results = execute_query(query)
    return jsonify(results)

@app.route('/api/overview/mutated_genes', methods=['GET'])
def overview_mutated_genes():
    """API endpoint to get mutated genes statistics."""
        
    total_query = f"""
        SELECT COUNT(DISTINCT specimen_number) as total_count
        FROM {ASSAY_TABLE}
        WHERE specimen_number IS NOT NULL
    """
    total_result = execute_query(total_query)
    total_specimens = total_result[0]['total_count'] if total_result else 1  # Avoid division by zero
        
    gene_query = f"""
        SELECT 
            g.gene_name_hgnc as gene_name,
            COUNT(*) as row_count,
            COUNT(DISTINCT a.specimen_number) as specimen_count
        FROM {GENOMIC_TABLE} g
        INNER JOIN {ASSAY_TABLE} a ON g.result_id = a.result_id
        WHERE g.gene_name_hgnc IS NOT NULL 
            AND g.gene_name_hgnc != ''
            AND a.specimen_number IS NOT NULL
        GROUP BY g.gene_name_hgnc
        ORDER BY COUNT(DISTINCT a.specimen_number) DESC
    """
    
    gene_results = execute_query(gene_query)
    
    # Calculate frequency percentage 
    for gene in gene_results:
        gene['frequency_pct'] = round((gene['specimen_count'] * 100.0 / total_specimens), 2) if total_specimens > 0 else 0
    
    return jsonify(gene_results if gene_results else [])


if __name__ == '__main__':
    app.run(debug=True, port=5000)